# cTrader

A better sample scripts than the original Sample given by cTrader
https://github.com/spotware/OpenApiPy/blob/main/samples/ConsoleSample/main.py

The original script uses 1 thread, all user command, message received, are handled
in one single thread, causing major disconnection and inconsistance message receiving.

This sample uses multiprocessing, solves multi account alert at once issue.

# Version
1. Python - 3.12

# How to start
1. Install python in your OS, google how to
2. `pip install -r requirements.txt`
3. Create `.env` file in this folder (eg: The folder this README.md is in)
4. Go to https://openapi.ctrader.com/apps
5. Click "Credentails"
6. Click "Sandbox"
7. Note down all these values
8. Put the following
```
APP_CLIENT_ID="xxx"
APP_CLIENT_SECRET="xxx"
ACCESS_TOKEN="xxx"
REFRESH_TOKEN="xxx"
ACCOUNT_TYPE="<demo or live>"
```
