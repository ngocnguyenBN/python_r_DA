import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime

mt5.initialize()

# Log in to the demo account
account = 5033013189  # Replace with your demo account number
password = "8-WwAaIk"  # Replace with your demo account password
server = "MetaQuotes-Demo"  # Replace with your broker's server name

mt5.login(account, password, server)

print("Connected to account:", mt5.account_info()._asdict())

account_info = mt5.account_info()
if account_info is None:
    print("Failed to retrieve account info:", mt5.last_error())
else:
    account_info_dict = account_info._asdict()

# Fetch open positions
positions = mt5.positions_get()
positions_list = []
if positions:
    for pos in positions:
        positions_list.append(pos._asdict())

# Fetch pending orders
orders = mt5.orders_get()
orders_list = []
if orders:
    for order in orders:
        orders_list.append(order._asdict())

# Structure the output
output = {
    "AccountType": account_info_dict.get("account_type", "Demo"),
    "Balance": account_info_dict.get("balance", 0),
    "Equity": account_info_dict.get("equity", 0),
    "Margin": account_info_dict.get("margin", 0),
    "FreeMargin": account_info_dict.get("margin_free", 0),
    "MarginLevel": account_info_dict.get("margin_level", 0),
    "IsLive": account_info_dict.get("trade_mode") == 0,
    "Number": account_info_dict.get("login"),
    "BrokerName": server,
    "Currency": account_info_dict.get("currency"),
    "Leverage": account_info_dict.get("leverage"),
    "Positions": positions_list,
    "PendingOrders": orders_list,
}

# Print the structured data
print(output)

# Shutdown the MT5 terminal
mt5.shutdown()
