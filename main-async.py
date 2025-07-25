#!/usr/bin/env python

import datetime
import os
import inspect
import requests
import schedule
import time
import threading
import multiprocessing
import json

from dotenv import load_dotenv
from multiprocessing import Process
from twisted.internet import reactor, task

from ctrader_open_api.endpoints import EndPoints
from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import ProtoOAExecutionType
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoHeartbeatEvent
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOADealListReq, ProtoOADealListRes, ProtoOAApplicationAuthReq,
    ProtoOASubscribeSpotsRes, ProtoOAApplicationAuthRes,
    ProtoOAGetPositionUnrealizedPnLReq, ProtoOAGetPositionUnrealizedPnLRes,
    ProtoOAExecutionEvent, ProtoOAAccountLogoutRes, ProtoOAAccountAuthRes,
    ProtoOAGetAccountListByAccessTokenReq, ProtoOAAccountLogoutReq, ProtoOAAccountAuthReq
)

# For logging use
load_dotenv()
filename = os.path.basename(inspect.getfile(inspect.currentframe()))

class CTraderAsyncClient:
    def __init__(self, ctid_trader_account_id=None, process_name=None):
        self.client = None
        self.current_account_id = ctid_trader_account_id
        self.process_name = process_name or f"Process-{ctid_trader_account_id}"
        self.connection_completed = False
        self.command_processed = True
        self.shutdown_event = threading.Event()
        self.connection_attempts = 0
        self.max_connection_attempts = 10  # Increased to handle longer disconnections

        # Load environment variables
        self.app_client_id = os.getenv("APP_CLIENT_ID")
        self.app_client_secret = os.getenv("APP_CLIENT_SECRET")
        self.access_token = os.getenv("ACCESS_TOKEN")
        self.host_type = os.getenv("ACCOUNT_TYPE").lower()

        # Telegram bot configuration
        self.telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")

        self.position_pnl_data = None
        self.money_digits = 2
        self.deal_list_data = None

        # Scheduler task
        self.scheduler_task = None

        # Tạo session để tái sử dung connection
        self.session = requests.Session()

    def initialize(self):
        """Initialize the client and validate host type"""
        print(f"[{self.process_name}] Initializing client for account {self.current_account_id}")

        # Validate host type
        if self.host_type not in ["live", "demo"]:
            print(f"{self.host_type} is not a valid host type. Using demo.")
            self.host_type = "demo"

        host = EndPoints.PROTOBUF_LIVE_HOST if self.host_type == "live" else EndPoints.PROTOBUF_DEMO_HOST

        print(f"[{self.process_name}] Connecting to {host}:{EndPoints.PROTOBUF_PORT}")

        self.client = Client(host, EndPoints.PROTOBUF_PORT, TcpProtocol)

        # Set up callbacks
        self.client.setConnectedCallback(self.connected)
        self.client.setDisconnectedCallback(self.disconnected)
        self.client.setMessageReceivedCallback(self.on_message_received)

        # Schedule reports
        schedule_pnl_report_interval = int(os.getenv("SCHEDULE_PNL_REPORT_INTERVAL"))
        schedule_pnl_report_time = int(os.getenv("SCHEDULE_PNL_REPORT_TIME"))
        # Calculate minutes for PnL report based on interval
        for hour in range(0, 24, schedule_pnl_report_interval):
            schedule.every().day.at(f"{hour:02d}:{schedule_pnl_report_time:02d}").do(self.schedule_pnl_report)

        schedule_deals_report_interval = int(os.getenv("SCHEDULE_DEALS_REPORT_INTERVAL"))
        schedule_deals_report_time = os.getenv("SCHEDULE_DEALS_REPORT_TIME")
        schedule.every(schedule_deals_report_interval).days.at(schedule_deals_report_time).do(self.schedule_daily_deal_report)

        # Default 04:00 in Saturday UTC +7 # 21:00 in Friday UTC 0
        schedule_weekly_report_time = os.getenv("SCHEDULE_WEEKLY_REPORT_TIME", "21:00")
        schedule.every().friday.at(schedule_weekly_report_time).do(self.schedule_weekly_deal_report)

        # Start scheduler
        self.scheduler_task = task.LoopingCall(self.run_scheduler)
        self.scheduler_task.start(60)  # Run every 60 seconds

    def connected(self, client):
        """Callback for client connection"""
        print(f"[{self.process_name}] Connected successfully {self.host_type.capitalize()} A/c {self.current_account_id}")
        self.connection_attempts = 0

        # Send application auth request
        request = ProtoOAApplicationAuthReq()
        request.clientId = self.app_client_id
        request.clientSecret = self.app_client_secret

        print(f"[{self.process_name}] Sending application auth request...")
        deferred = client.send(request)
        deferred.addErrback(self.on_error)

    def disconnected(self, client, reason):
        """Callback for client disconnection"""
        print(f"[{self.process_name}] Disconnected - Account {self.current_account_id}: {reason}")
        self.connection_completed = False

        # Clean up the reason string to remove HTML-like tags
        reason_str = str(reason)
        # Remove any angle brackets that might cause issues
        reason_str = reason_str.replace('<', '').replace('>', '')

        # Send disconnection notification to Telegram
        disconnect_msg = f"⚠️ <b>CONNECTION LOST</b>\n"
        disconnect_msg += f"🏦 {self.host_type.capitalize()} A/c {self.current_account_id}\n"
        disconnect_msg += f"📅 Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        disconnect_msg += f"📝 Reason: {reason_str}\n"
        self.send_telegram_message(disconnect_msg)

    def on_message_received(self, client, message):
        """Callback for receiving all messages"""
        # Handle heartbeat
        if message.payloadType == ProtoHeartbeatEvent().payloadType:
            print(f"[{self.process_name}] Heartbeat received at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            return

        # List of ignored messages
        if message.payloadType in [ProtoOASubscribeSpotsRes().payloadType, ProtoOAAccountLogoutRes().payloadType]:
            return

        elif message.payloadType == ProtoOAApplicationAuthRes().payloadType:
            print(f"[{self.process_name}] API authorized {self.host_type.capitalize()} A/c {self.current_account_id}")
            if self.current_account_id is not None:
                self.send_proto_oa_account_auth_req()
                return
            self.connection_completed = True

        elif message.payloadType == ProtoOAAccountAuthRes().payloadType:
            protoOAAccountAuthRes = Protobuf.extract(message)
            print(f"[{self.process_name}] Account {protoOAAccountAuthRes.ctidTraderAccountId} has been authorized")
            self.connection_completed = True

            # Send reconnection success notification if this was a reconnection
            if self.connection_attempts > 0:
                success_msg = f"✅ <b>RECONNECTION SUCCESSFUL</b>\n"
                success_msg += f"🏦 {self.host_type.capitalize()} A/c {self.current_account_id}\n"
                success_msg += f"📅 Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                success_msg += f"🔄 Connection restored after {self.connection_attempts} attempts."
                self.send_telegram_message(success_msg)
                self.connection_attempts = 0

        elif message.payloadType == ProtoOAGetPositionUnrealizedPnLRes().payloadType:
            pnl_response = Protobuf.extract(message)
            self.position_pnl_data = pnl_response.positionUnrealizedPnL
            self.money_digits = pnl_response.moneyDigits
            print(f"[{self.process_name}] Position PnL data received - Account {self.current_account_id}: {len(self.position_pnl_data)} positions")

            if self.position_pnl_data:
                self.send_pnl_telegram_report()
            else:
                self.send_empty_pnl_telegram_report()

        elif message.payloadType == ProtoOADealListRes().payloadType:
            deal_response = Protobuf.extract(message)
            self.deal_list_data = deal_response.deal
            print(f"[{self.process_name}] Deal list data received - Account {self.current_account_id}: {len(self.deal_list_data)} deals")

            # Check if this is weekly report
            if hasattr(self, 'is_weekly_report') and self.is_weekly_report:
                self.is_weekly_report = False  # Reset flag
                if self.deal_list_data:
                    self.send_weekly_deal_telegram_report()
                else:
                    self.send_empty_weekly_deal_telegram_report()
            else:
                # Original daily report logic
                yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
                start_of_day = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

                if self.deal_list_data:
                    self.send_deal_telegram_report(start_of_day)
                else:
                    self.send_empty_deal_telegram_report(start_of_day)

        elif message.payloadType == ProtoOAExecutionEvent().payloadType:
            execution_event = Protobuf.extract(message)
            if hasattr(execution_event, 'executionType') and execution_event.executionType == ProtoOAExecutionType.ORDER_FILLED:
                self.handle_execution_event(execution_event)

        else:
            print(f"[{self.process_name}] Message received - Account {self.current_account_id}: \n", Protobuf.extract(message))

        self.command_processed = True

    def handle_execution_event(self, execution_event):
        """Handle execution events"""
        positionStatus = execution_event.position.positionStatus
        deal = execution_event.deal

        symbol = self.get_symbol_name(deal.symbolId)
        volume = self.calculate_volume(deal.symbolId, deal.volume, deal.moneyDigits)

        # Add account ID to telegram message to distinguish between accounts
        print("positionStatus...", positionStatus)
        telegram_msg = f"🎯 <b>{"NEW POSITION OPEN" if positionStatus == 1 else "POSITION CLOSED AUTO" if positionStatus == 2 else "POSITION CLOSED MANUAL" if positionStatus == 3 else 'n/a' }</b>\n"
        telegram_msg += f"🏦 {self.host_type.capitalize()} A/c {self.current_account_id}\n"
        telegram_msg += f"📊 PID: ${deal.positionId}\n"
        telegram_msg += f"💰 Symbol: {symbol}\n"
        if positionStatus == 1:
            telegram_msg += f"📈 Trade Side: {'BUY' if deal.tradeSide == 1 else 'SELL' if deal.tradeSide == 2 else 'n/a'}\n"
        telegram_msg += f"💵 Volume: {volume} lot\n"

        if hasattr(deal, 'executionPrice'):
            telegram_msg += f"💰 Execution Price: {deal.executionPrice}\n"
        if positionStatus == 2 or positionStatus == 3:
            telegram_msg += f"💰 Entry Price: {deal.closePositionDetail.entryPrice}\n"
            telegram_msg += f"💰 GrossProfit: {deal.closePositionDetail.grossProfit/ (10 ** deal.moneyDigits)}\n"
            telegram_msg += f"💰 Swap: {deal.closePositionDetail.swap/ (10 ** deal.moneyDigits)}\n"
            telegram_msg += f"💰 Commission: {deal.closePositionDetail.commission/ (10 ** deal.moneyDigits)}\n"
            telegram_msg += f"💰 Balance: {deal.closePositionDetail.balance/ (10 ** deal.moneyDigits)}\n"

        telegram_msg += f"⏰ Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        self.send_telegram_message(telegram_msg)

    def on_error(self, failure):
        """Callback for errors"""
        print(f"[{self.process_name}] Message Error - Account {self.current_account_id}: {failure}")

    def send_proto_oa_get_account_list_by_access_token_req(self, client_msg_id=None):
        """Send get account list request"""
        request = ProtoOAGetAccountListByAccessTokenReq()
        request.accessToken = self.access_token
        deferred = self.client.send(request, clientMsgId=client_msg_id)
        deferred.addErrback(self.on_error)

    def send_proto_oa_account_logout_req(self, client_msg_id=None):
        """Send account logout request"""
        request = ProtoOAAccountLogoutReq()
        request.ctidTraderAccountId = self.current_account_id
        deferred = self.client.send(request, clientMsgId=client_msg_id)
        deferred.addErrback(self.on_error)

    def send_proto_oa_account_auth_req(self, client_msg_id=None):
        """Send account auth request"""
        request = ProtoOAAccountAuthReq()
        request.ctidTraderAccountId = self.current_account_id
        request.accessToken = self.access_token
        deferred = self.client.send(request, clientMsgId=client_msg_id)
        deferred.addErrback(self.on_error)

    def send_proto_oa_get_position_unrealized_pnl_req(self, client_msg_id=None):
        """Send get position unrealized PnL request"""
        request = ProtoOAGetPositionUnrealizedPnLReq()
        request.ctidTraderAccountId = self.current_account_id
        deferred = self.client.send(request, clientMsgId=client_msg_id)
        deferred.addErrback(self.on_error)

    def send_proto_oa_deal_list_req(self, from_timestamp=None, to_timestamp=None, client_msg_id=None):
        """Send get deal list request"""
        request = ProtoOADealListReq()
        request.ctidTraderAccountId = self.current_account_id
        if from_timestamp:
            request.fromTimestamp = from_timestamp
        if to_timestamp:
            request.toTimestamp = to_timestamp
        request.maxRows = 1000
        deferred = self.client.send(request, clientMsgId=client_msg_id)
        deferred.addErrback(self.on_error)

    def run_scheduler(self):
        """Run the scheduler"""
        schedule.run_pending()

    def schedule_pnl_report(self):
        """Schedule PnL report"""
        print(f"[{self.process_name}] Schedule PnL report - Account {self.current_account_id}...")
        if self.connection_completed and self.current_account_id:
            reactor.callLater(0, self.send_hourly_pnl_list_req)

    def schedule_daily_deal_report(self):
        """Schedule daily deal report"""
        print(f"[{self.process_name}] Schedule daily deal report - Account {self.current_account_id}...")
        if self.connection_completed and self.current_account_id:
            reactor.callLater(0, self.send_daily_deal_list_req)

    def schedule_weekly_deal_report(self):
        """Schedule weekly deal report"""
        print(f"[{self.process_name}] Schedule weekly deal report - Account {self.current_account_id}...")
        if self.connection_completed and self.current_account_id:
            reactor.callLater(0, self.send_weekly_deal_list_req)

    def send_hourly_pnl_list_req(self):
        """Send hourly PnL report"""
        print(f"[{self.process_name}] Sending hourly PnL report - Account {self.current_account_id}...")
        # Request current PnL data
        self.send_proto_oa_get_position_unrealized_pnl_req()

    def send_daily_deal_list_req(self):
        """Send daily deal report for closed positions"""
        print(f"[{self.process_name}] Sending daily deal report - Account {self.current_account_id}...")

        # Calculate timestamps for previous day (00:00 to 23:59)
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        start_of_day = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)

        from_timestamp = int(start_of_day.timestamp() * 1000)
        to_timestamp = int(end_of_day.timestamp() * 1000)

        # Request deal list data
        self.deal_list_data = None
        self.send_proto_oa_deal_list_req(from_timestamp, to_timestamp)

    def send_weekly_deal_list_req(self):
        """Send weekly deal report for closed positions"""
        print(f"[{self.process_name}] Sending weekly deal report - Account {self.current_account_id}...")
        # Current time: Friday 21:15 PM
        now = today = datetime.datetime.now()

        # Last Sunday 21:00 PM (5 days ago)
        last_sunday = now - datetime.timedelta(days=5)
        last_sunday = last_sunday.replace(hour=21, minute=0, second=0, microsecond=0)

        # Last Friday 21:00 PM (today, 15 minutes ago)
        last_friday = now.replace(hour=21, minute=0, second=0, microsecond=0)

        # Convert to microseconds
        from_timestamp = int(last_sunday.timestamp() * 1_000)
        to_timestamp = int(last_friday.timestamp() * 1_000)

        print(f"Last Sunday 21:00 PM UTC 0: {from_timestamp}")
        print(f"Last Friday 21:00 PM UTC 0: {to_timestamp}")
        # Store week range for the report
        self.weekly_report_start = last_sunday
        self.weekly_report_end = last_friday

        # Request deal list data
        self.deal_list_data = None
        self.is_weekly_report = True  # Flag to identify this is weekly report
        self.send_proto_oa_deal_list_req(from_timestamp, to_timestamp)

    def run(self):
        """Main run method to start the client"""
        try:
            print(f"[{self.process_name}] Starting client for account {self.current_account_id}")
            self.initialize()

            # Start the client service
            self.client.startService()

            # Start the reactor (this will block until stopped)
            reactor.run(installSignalHandlers=False)

        except Exception as e:
            print(f"[{self.process_name}] Error in run method: {e}")
            raise
        finally:
            # Cleanup
            if self.scheduler_task and self.scheduler_task.running:
                self.scheduler_task.stop()
            if self.client:
                self.client.stopService()

    def stop(self):
        """Stop the client"""
        print(f"[{self.process_name}] Stopping client...")
        self.shutdown_event.set()
        if self.scheduler_task and self.scheduler_task.running:
            self.scheduler_task.stop()
        if self.client:
            self.client.stopService()
        reactor.callLater(0, reactor.stop)

    #TELEGRAM MESSAGE HANDLER
    def send_telegram_message(self, message):
        """Send message to Telegram"""
        print(f"[{self.process_name}] Telegram message: {message}")

        start_telegram = datetime.datetime.now()
        try:
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            payload = {
                'chat_id': self.telegram_chat_id,
                'text': message,
                'parse_mode': 'HTML'
            }
            # response = requests.post(url, json=payload, timeout=1)
            response = self.session.post(url, json=payload, timeout=1)
            if response.status_code != 200:
                print(f"[{self.process_name}] Failed to send telegram message: {response.text}")
            print(f"[{self.process_name}] Telegram message sent in {(datetime.datetime.now() - start_telegram).total_seconds():.3f} seconds")
        except Exception as e:
            print(f"[{self.process_name}] Error sending telegram message: {e}")

    def send_pnl_telegram_report(self):
        """Send PnL data as formatted table to Telegram"""
        if not self.is_trading_hours():
            print(f"[{self.process_name}] Outside trading hours - PnL report not sent for A/c {self.current_account_id}")
            return

        if not self.position_pnl_data:
            return

        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Create table header
        telegram_msg = f"📊 <b>Open Position Report {self.host_type.capitalize()} A/c {self.current_account_id}</b>\n"
        telegram_msg += f"⏰ Time: {current_time}\n\n"
        telegram_msg += f"<pre>"
        telegram_msg += f"{'PID':<10} {'Gross':<9} {'Net':<9}\n"
        telegram_msg += f"{'-'*10} {'-'*9} {'-'*9}\n"

        total_gross_pnl = 0
        total_net_pnl = 0

        # Add each position
        for position in self.position_pnl_data:
            position_id = position.positionId
            gross_pnl = position.grossUnrealizedPnL / (10 ** self.money_digits)
            net_pnl = position.netUnrealizedPnL / (10 ** self.money_digits)

            total_gross_pnl += gross_pnl
            total_net_pnl += net_pnl

            telegram_msg += f"{position_id:<10} {gross_pnl:<9.2f} {net_pnl:<9.2f}\n"

        # Add totals
        telegram_msg += f"{'-'*10} {'-'*9} {'-'*9}\n"
        telegram_msg += f"{'TOTAL':<10} {total_gross_pnl:<9.2f} {total_net_pnl:<9.2f}\n"
        telegram_msg += f"</pre>"

        # Add summary
        telegram_msg += f"\n💰 <b>SUMMARY</b>\n"
        telegram_msg += f"📈 Total Gross: {total_gross_pnl:.2f}\n"
        telegram_msg += f"📉 Total Net: {total_net_pnl:.2f}\n"
        telegram_msg += f"🔢 Open Positions: {len(self.position_pnl_data)}"

        self.send_telegram_message(telegram_msg)

    def send_deal_telegram_report(self, start_date):
        """Send deal data as formatted table to Telegram"""
        if not self.is_trading_hours():
            print(f"[{self.process_name}] Outside trading hours - Deals report not sent for A/c {self.current_account_id}")
            return

        if not self.deal_list_data:
            return

        # Filter only closed deals (deals with closePositionDetail)
        closed_deals_1 = [deal for deal in self.deal_list_data if hasattr(deal, 'closePositionDetail') and hasattr(deal.closePositionDetail, 'balance')]

        closed_deals = [deal for deal in closed_deals_1 if deal.closePositionDetail.balance > 0]

        if len(closed_deals) == 0:
            self.send_empty_deal_telegram_report(start_date)
            return

        # Create table header with Deal ID column
        telegram_msg = f"📊 <b>Daily Deal Report {self.host_type.capitalize()} A/c {self.current_account_id}</b>\n"
        telegram_msg += f"📅 Date: {start_date.strftime('%Y-%m-%d')}\n"
        telegram_msg += f"⏰ Report Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        telegram_msg += f"<pre>"
        telegram_msg += f"{'DID':<10} {'Symbol':<7} {'Side':<5} {'Time':<9} {'Vol':<5} {'Swap':<6} {'Comm':<6} {'Net':<7} {'Bal':<9}\n"
        telegram_msg += f"{'-'*10} {'-'*7} {'-'*5} {'-'*9} {'-'*5} {'-'*6} {'-'*6} {'-'*7} {'-'*9}\n"

        total_swap = 0
        total_commission = 0
        total_net_profit = 0
        current_balance = 0  # You might need to get this from your account data

        # Process all deals first to minimize loop overhead
        deal_rows = []

        for deal in closed_deals:
            # Deal ID
            deal_id = getattr(deal, 'dealId', 'N/A')

            # Extract symbol (you may need to adjust this based on your data structure)
            symbol = self.get_symbol_name(deal.symbolId)

            # Opening direction
            direction = 'Sell' if deal.tradeSide == 1 else 'Buy'  # Adjust logic as needed

            # Closing time
            close_detail = deal.closePositionDetail
            close_time = deal.executionTimestamp
            if close_time != 'N/A':
                # Convert timestamp to readable format (adjust based on your timestamp format)
                try:
                    if isinstance(close_time, int):
                        close_time = datetime.datetime.fromtimestamp(close_time/1000).strftime('%H:%M:%S')
                    else:
                        close_time = str(close_time)[:16]  # Truncate if too long
                except:
                    close_time = 'N/A'

            # Usage in the code:
            volume = self.calculate_volume(deal.symbolId, deal.volume, close_detail.moneyDigits)

            # Swap
            swap = close_detail.swap / (10 ** close_detail.moneyDigits)

            # Commission
            commission = close_detail.commission / (10 ** close_detail.moneyDigits)

            # Net profit
            gross_profit = close_detail.grossProfit / (10 ** close_detail.moneyDigits)
            net_profit = gross_profit + swap + commission

            # Balance (you might need to calculate running balance)
            current_balance = close_detail.balance / (10 ** close_detail.moneyDigits)

            total_swap += swap
            total_commission += commission
            total_net_profit += net_profit

            deal_rows.append(f"{deal_id:<10} {symbol:<7} {direction:<5} {close_time:<9} {volume:<5.2f} {swap:<6.2f} {commission:<6.2f} {net_profit:<7.2f} {current_balance:<9.2f}")

        # Join all deal rows at once
        telegram_msg += "\n".join(deal_rows) + "\n"

        # Add totals
        telegram_msg += f"{'-'*10} {'-'*7} {'-'*5} {'-'*9} {'-'*5} {'-'*6} {'-'*6} {'-'*7} {'-'*9}\n"
        telegram_msg += f"{'TOTAL':<10} {'':<7} {'':<5} {'':<9} {'':<5} {total_swap:<6.2f} {total_commission:<6.2f} {total_net_profit:<7.2f} {current_balance:<9.2f}\n"
        telegram_msg += f"</pre>"

        # Add summary
        telegram_msg += f"\n💰 <b>SUMMARY</b>\n"
        telegram_msg += f"🔄 Total Swap: {total_swap:.2f}\n"
        telegram_msg += f"💳 Total Commission: {total_commission:.2f}\n"
        telegram_msg += f"💵 Total Net Profit: {total_net_profit:.2f}\n"
        telegram_msg += f"💰 Final Balance: {current_balance:.2f}\n"
        telegram_msg += f"🔢 Closed Deals: {len(closed_deals)}"

        self.send_telegram_message(telegram_msg)

    def send_weekly_deal_telegram_report(self):
        """Send weekly deal data as formatted table to Telegram"""
        if not self.deal_list_data:
            return

        # Filter only closed deals
        closed_deals_1 = [deal for deal in self.deal_list_data if hasattr(deal, 'closePositionDetail') and hasattr(deal.closePositionDetail, 'balance')]
        closed_deals = [deal for deal in closed_deals_1 if deal.closePositionDetail.balance > 0]

        if len(closed_deals) == 0:
            self.send_empty_weekly_deal_telegram_report()
            return

        # Create table header
        telegram_msg = f"📊 <b>Weekly Deal Report {self.host_type.capitalize()} A/c {self.current_account_id}</b>\n"
        telegram_msg += f"📅 Week: {self.weekly_report_start.strftime('%Y-%m-%d')} to {self.weekly_report_end.strftime('%Y-%m-%d')}\n"
        telegram_msg += f"⏰ Report Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        telegram_msg += f"<pre>"
        telegram_msg += f"{'Date':<10} {'Symbol':<7} {'Side':<5} {'Vol':<5} {'Swap':<6} {'Comm':<6} {'Net':<7}\n"
        telegram_msg += f"{'-'*10} {'-'*7} {'-'*5} {'-'*5} {'-'*6} {'-'*6} {'-'*7}\n"

        total_swap = 0
        total_commission = 0
        total_net_profit = 0
        daily_summaries = {}

        # Group deals by day and calculate totals
        for deal in closed_deals:
            close_detail = deal.closePositionDetail

            # Extract deal date
            deal_date = datetime.datetime.fromtimestamp(deal.executionTimestamp/1000).strftime('%m-%d')

            # Extract symbol
            symbol = self.get_symbol_name(deal.symbolId)

            # Opening direction
            direction = 'Sell' if deal.tradeSide == 1 else 'Buy'

            # Volume
            volume = self.calculate_volume(deal.symbolId, deal.volume, close_detail.moneyDigits)

            # Swap, Commission, Net profit
            swap = close_detail.swap / (10 ** close_detail.moneyDigits)
            commission = close_detail.commission / (10 ** close_detail.moneyDigits)
            gross_profit = close_detail.grossProfit / (10 ** close_detail.moneyDigits)
            net_profit = gross_profit + swap + commission

            total_swap += swap
            total_commission += commission
            total_net_profit += net_profit

            # Add to daily summary
            if deal_date not in daily_summaries:
                daily_summaries[deal_date] = {'deals': 0, 'net_profit': 0}
            daily_summaries[deal_date]['deals'] += 1
            daily_summaries[deal_date]['net_profit'] += net_profit

            telegram_msg += f"{deal_date:<10} {symbol:<7} {direction:<5} {volume:<5.2f} {swap:<6.2f} {commission:<6.2f} {net_profit:<7.2f}\n"

        # Add totals
        telegram_msg += f"{'-'*10} {'-'*7} {'-'*5} {'-'*5} {'-'*6} {'-'*6} {'-'*7}\n"
        telegram_msg += f"{'TOTAL':<10} {'':<7} {'':<5} {'':<5} {total_swap:<6.2f} {total_commission:<6.2f} {total_net_profit:<7.2f}\n"
        telegram_msg += f"</pre>"

        # Add daily breakdown
        telegram_msg += f"\n📈 <b>DAILY BREAKDOWN</b>\n"
        for date, summary in sorted(daily_summaries.items()):
            telegram_msg += f"📅 {date}: {summary['deals']} deals, Net P&L: {summary['net_profit']:.2f}\n"

        # Add summary
        telegram_msg += f"\n💰 <b>WEEKLY SUMMARY</b>\n"
        telegram_msg += f"🔄 Total Swap: {total_swap:.2f}\n"
        telegram_msg += f"💳 Total Commission: {total_commission:.2f}\n"
        telegram_msg += f"💵 Total Net Profit: {total_net_profit:.2f}\n"
        telegram_msg += f"🔢 Total Closed Deals: {len(closed_deals)}\n"
        telegram_msg += f"📊 Trading Days: {len(daily_summaries)}"

        self.send_telegram_message(telegram_msg)

    def send_empty_deal_telegram_report(self, start_of_day):
        """Send empty deal report when no closed deals are found"""
        if not self.is_trading_hours():
            print(f"[{self.process_name}] Outside trading hours - Empty Deals report not sent for A/c {self.current_account_id}")
            return

        telegram_msg = f"📊 <b>Daily Deal Report {self.host_type.capitalize()} A/c {self.current_account_id}</b>\n"
        telegram_msg += f"📅 Date: {start_of_day.strftime('%Y-%m-%d')}\n"
        telegram_msg += f"⏰ Report Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        telegram_msg += f"<pre>"
        telegram_msg += f"✅ No closed deals found for this period."
        telegram_msg += f"</pre>"
        self.send_telegram_message(telegram_msg)

    def send_empty_deal_telegram_report(self, start_of_day):
        """Send empty deal report when no closed deals are found"""
        if not self.is_trading_hours():
            print(f"[{self.process_name}] Outside trading hours - Empty Deals report not sent for A/c {self.current_account_id}")
            return

        telegram_msg = f"📊 <b>Daily Deal Report {self.host_type.capitalize()} A/c {self.current_account_id}</b>\n"
        telegram_msg += f"📅 Date: {start_of_day.strftime('%Y-%m-%d')}\n"
        telegram_msg += f"⏰ Report Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        telegram_msg += f"<pre>"
        telegram_msg += f"✅ No closed deals found for this period."
        telegram_msg += f"</pre>"
        self.send_telegram_message(telegram_msg)

    def send_empty_weekly_deal_telegram_report(self):
        """Send empty weekly deal report when no closed deals are found"""
        telegram_msg = f"📊 <b>Weekly Deal Report {self.host_type.capitalize()} A/c {self.current_account_id}</b>\n"
        telegram_msg += f"📅 Week: {self.weekly_report_start.strftime('%Y-%m-%d')} to {self.weekly_report_end.strftime('%Y-%m-%d')}\n"
        telegram_msg += f"⏰ Report Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        telegram_msg += f"<pre>"
        telegram_msg += f"✅ No closed deals found for this week."
        telegram_msg += f"</pre>"
        self.send_telegram_message(telegram_msg)

    def send_empty_pnl_telegram_report(self):
        """Send empty PnL report when no open positions are found"""
        if not self.is_trading_hours():
            print(f"[{self.process_name}] Outside trading hours - Empty PnL report not sent for A/c {self.current_account_id}")
            return

        telegram_msg = f"📊 <b>Open Position Report {self.host_type.capitalize()} A/c {self.current_account_id}</b>\n"
        telegram_msg += f"⏰ Time: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        telegram_msg += f"<pre>"
        telegram_msg += f"🎯 No Open Position"
        telegram_msg += f"</pre>"
        self.send_telegram_message(telegram_msg)

    def is_trading_hours(self):
        now = datetime.datetime.now()

        current_weekday = now.weekday()  # 0=Monday, 6=Sunday
        current_time = now.time()

        # Trading hours
        market_open = datetime.time(22, 2, 0)    # 22:02:00
        market_close_friday = datetime.time(20, 57, 0)  # 20:57:00

        if current_weekday == 6:  # Sunday
            # Sunday trading starts at 22:02:00
            return current_time >= market_open

        elif current_weekday in [0, 1, 2, 3]:  # Monday to Thursday
            # Trading ends at 20:59:00, then starts again at 22:02:00
            return True

        elif current_weekday == 4:  # Friday
            # Friday trading ends at 20:57:00
            return current_time <= market_close_friday

        else:  # Saturday (5)
            return False

    def get_symbol_name(self, symbol_id):
        symbol_map = {
            5: "AUDUSD",
            12: "NZDUSD",
            41: "XAUUSD",
            10026: "BTCUSD"
        }
        return symbol_map.get(symbol_id, 'N/A')

    def calculate_volume(self, symbol_id, volume, money_digits):
        digits = {
            41: 2,        # XAUUSD
            10026: 0,     # BTCUSD
        }
        # other symbols default to 5 digits
        return volume / (10 ** (money_digits +  digits.get(symbol_id, 5)))

def run_client_process(account_id):
    """Run a single client in its own process"""
    client = None
    try:
        print(f"Starting process for account {account_id}")
        client = CTraderAsyncClient(account_id, f"Process-{account_id}")

        # Set up signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            print(f"Received signal {signum} for account {account_id}")
            if client:
                client.stop()

        import signal
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # Start the client
        client.run()

    except KeyboardInterrupt:
        print(f"Process for account {account_id} interrupted")
    except Exception as e:
        print(f"Error in process for account {account_id}: {e}")
        raise
    finally:
        if client:
            client.stop()

def main():
    """Main entry point - start multiple processes"""
    # Load environment variables
    print("#" * 86)
    print("# Loading environment variables...")
    print("# APP_CLIENT_ID                    :",os.getenv("APP_CLIENT_ID"))
    print("# APP_CLIENT_SECRET                :",os.getenv("APP_CLIENT_SECRET"))
    print("# ACCESS_TOKEN                     :",os.getenv("ACCESS_TOKEN"))
    print("# ACCOUNT_TYPE                     :",os.getenv("ACCOUNT_TYPE"))
    print("# TELEGRAM_BOT_TOKEN               :",os.getenv("TELEGRAM_BOT_TOKEN"))
    print("# TELEGRAM_CHAT_ID                 :",os.getenv("TELEGRAM_CHAT_ID"))
    print("# SCHEDULE_PNL_REPORT_INTERVAL     :",os.getenv("SCHEDULE_PNL_REPORT_INTERVAL"))
    print("# SCHEDULE_PNL_REPORT_TIME         :",os.getenv("SCHEDULE_PNL_REPORT_TIME"))
    print("# SCHEDULE_DEALS_REPORT_INTERVAL   :",os.getenv("SCHEDULE_DEALS_REPORT_INTERVAL"))
    print("# SCHEDULE_DEALS_REPORT_TIME       :",os.getenv("SCHEDULE_DEALS_REPORT_TIME"))
    print("# ACCOUNT_ID_LIST                  :",os.getenv("ACCOUNT_ID_LIST"))
    print("#" * 86)

    account_ids = json.loads(os.getenv("ACCOUNT_ID_LIST"))
    processes = []

    try:
        print("Starting multi-process CTrader clients...")

        # Create and start processes
        for account_id in account_ids:
            process = Process(
                target=run_client_process,
                args=(account_id,),
                name=f"CTrader-{account_id}"
            )
            process.start()
            processes.append(process)
            print(f"Started process for account {account_id} (PID: {process.pid})")

            # Small delay between starting processes
            time.sleep(1)

        # Wait for all processes
        for process in processes:
            process.join()

    except KeyboardInterrupt:
        print("\nShutting down all processes...")
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
                if process.is_alive():
                    print(f"Force killing process {process.pid}")
                    process.kill()
                    process.join()
        print("All processes terminated")

    except Exception as e:
        print(f"Error in main: {e}")
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join()


if __name__ == "__main__":
    # Set multiprocessing start method
    multiprocessing.set_start_method('spawn', force=True)
    main()