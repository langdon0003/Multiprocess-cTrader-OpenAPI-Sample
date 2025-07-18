# Multiprocessing cTrader Open API

A better sample scripts than the original Sample given by cTrader
https://github.com/spotware/OpenApiPy/blob/main/samples/ConsoleSample/main.py

The original script uses 1 thread, all user command, message received, are handled
in one single thread, causing major disconnection and inconsistences message receiving.

This sample uses multiprocessing, solves multi account alerts at once issue.

### What you need
- VPS Ubuntu (prefer)
- Python - 3.13
- Ctrader Open API
- Telegram bot & channel

### How to start

##### Note down all these values
1. Go to https://openapi.ctrader.com/apps
2. Click **Credentials** and copy `Client ID` and `Secret`
3. Click **Sandbox** and choose Scope: **Account info *ONLY*** and copy `Access token`
4. Click **Trading Account** and copy list of `accountId`
    - `"live": false"` for demo account
    - `"live": true"` for live account


##### Create `.env` file in this folder

Full `.env` example

```
APP_CLIENT_ID="xxx"
APP_CLIENT_SECRET="xxx"

#Account info ONLY
ACCESS_TOKEN="xxx"

# ACCOUNT TYPE AND ID LIST
# ACCOUNT_TYPE="demo"
# ACCOUNT_ID_LIST=[43911111,43961112]

# ACCOUNT TYPE AND ID LIST
ACCOUNT_TYPE="live"
ACCOUNT_ID_LIST=[44011111,44011112]

TELEGRAM_BOT_TOKEN="xxx"
TELEGRAM_CHAT_ID="-1002823041111"


SCHEDULE_PNL_REPORT_INTERVAL=2  # Every 2 hours
SCHEDULE_PNL_REPORT_TIME="5"  # at xx:05

SCHEDULE_DEALS_REPORT_INTERVAL=1 # Every 1 day
SCHEDULE_DEALS_REPORT_TIME="07:30"  # at 07:30 (UTC 0 if run on VPS)
```
##### Install python in your OS (prefer MacOS/Ubuntu) or your VPS (prefer Ubuntu),
Command line for setting up on your VPS (Ubuntu)

```
sudo apt update
sudo apt install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt install python3.13 python3.13-venv python3.13-dev python3-pip nodejs npm -y
npm install -g pm2

git clone https://github.com/langdon0003/Multiprocess-cTrader-OpenAPI-Sample.git
cd Multiprocess-cTrader-OpenAPI-Sample/

python3 -m venv myenv
source myenv/bin/activate
pip install -r requirements.txt

cp sampleenv .env
vi .env

pm2 start main-async.py --name ctrader-live && pm2 log all
```