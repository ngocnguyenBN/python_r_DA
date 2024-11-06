import requests
from bs4 import BeautifulSoup
import yfinance as yf
import pandas as pd
import re
from IPython.display import display
from tabulate import tabulate
import pytz
from datetime import datetime, timedelta


def DOWNLOAD_YAH_PRICES_BY_CODE(symbol, period="max"):
    """
    Downloads and merges price, dividend, and shares data for a given symbol.

    Parameters:
    symbol (str): The stock ticker symbol.
    period (str): The time period for which data should be downloaded.
                  Options include "1mo", "3mo", "1y", or "max" (default is "max").

    Returns:
    DataFrame: A DataFrame containing price, dividend, and shares data merged by date.
    """
    # symbol = "IBM"
    ticker = yf.Ticker(symbol)

    # Download historical price data
    price_data = ticker.history(period=period)[
        ["Open", "High", "Low", "Close", "Volume"]
    ]
    price_data = price_data.reset_index()
    price_data = CLEAN_COLNAMES(price_data)
    price_data["date"] = pd.to_datetime(price_data["date"]).dt.date

    # Download dividends and shares data
    try:
        dividend_data = ticker.dividends
        dividend_data = dividend_data.reset_index()
        dividend_data = CLEAN_COLNAMES(dividend_data)
        dividend_data["date"] = pd.to_datetime(dividend_data["date"]).dt.date
    except pytz.exceptions.UnknownTimeZoneError as e:
        shares_data = pd.DataFrame(columns=["date", "dividends"])

    # shares_data = ticker.get_shares_full(start=start_date, end=end_date)
    # shares_data = pd.DataFrame(list(shares_data.items()), columns=["date", "sharesout"])
    try:
        # Lấy dữ liệu shares từ ticker
        shares_data = ticker.get_shares_full()
        # Convert shares data to DataFrame
        shares_data = pd.DataFrame(
            list(shares_data.items()), columns=["date", "sharesout"]
        )
        shares_data["date"] = pd.to_datetime(
            shares_data["date"], errors="coerce"
        ).dt.date

    except pytz.exceptions.UnknownTimeZoneError as e:
        shares_data = pd.DataFrame(columns=["date", "sharesout"])

    # shares_data["date"] = pd.to_datetime(shares_data["date"]).dt.date

    df_info = ticker.info
    final_data = pd.merge(price_data, dividend_data, on="date", how="left")
    final_data = pd.merge(final_data, shares_data, on="date", how="left")
    final_data["sharesout"] = final_data["sharesout"].ffill()

    # Add the symbol column
    final_data["codesource"] = symbol
    final_data["market_cap"] = df_info.get("marketCap")
    final_data["float"] = df_info.get("floatShares")
    final_data["cur"] = df_info.get("currency")
    final_data["rt"] = final_data["close"] / final_data["close"].shift(1) - 1
    final_data["change"] = final_data["close"] - final_data["close"].shift(1)

    final_data = final_data.drop_duplicates(subset=["codesource", "date"])

    final_data["rt"] = final_data.groupby("codesource")["rt"].transform(
        lambda x: x.fillna(0)
    )
    final_data["rt_1"] = 1 + final_data["rt"]
    final_data["cumul"] = final_data.groupby("codesource")["rt_1"].cumprod()
    final_data["cumul_rev"] = (
        final_data.groupby("codesource")["cumul"].transform("last")
        / final_data["cumul"]
    )
    final_data["capi"] = final_data["market_cap"] * final_data["cumul_rev"]
    final_data = final_data.drop(columns=["rt_1", "cumul", "cumul_rev"])
    SHOW_DATA(final_data)

    return final_data


# Usage example
# symbol_data = DOWNLOAD_YAH_PRICES_BY_CODE("IBM", period="max")
# symbol_data = DOWNLOAD_YAH_PRICES_BY_CODE("META", period="max")
def CACULATE_CAPI(df):
    df = df.drop_duplicates(subset=["codesource", "date"])
    df["rt"] = df.groupby("codesource")["rt"].transform(lambda x: x.fillna(0))
    df["rt_1"] = 1 + df["rt"]
    df["cumul"] = df.groupby("codesource")["rt_1"].cumprod()
    df["cumul_rev"] = df.groupby("codesource")["cumul"].transform("last") / df["cumul"]
    df["capi"] = df["market_cap"] * df["cumul_rev"]
    df = df.drop(columns=["rt_1", "cumul", "cumul_rev"])
    SHOW_DATA(df)
    return df


def CACULATE_RT_CHANGE(df):
    if "close_adj" in df.columns:
        df["rt"] = df["close_adj"] / df["close_adj"].shift(1) - 1
        df["change"] = df["close_adj"] - df["close_adj"].shift(1)
    else:
        df["rt"] = df["close"] / df["close"].shift(1) - 1
        df["change"] = df["close"] - df["close"].shift(1)


def SHOW_DATA(df):
    print(tabulate(df, headers="keys", tablefmt="pretty", showindex=False))


def CONVERT_NUMBER(value):
    """Helper function to convert strings like '1.2B' into a numeric value."""
    try:
        if "B" in value:
            return float(value.replace("B", "").replace(",", "")) * 1e9
        elif "M" in value:
            return float(value.replace("M", "").replace(",", "")) * 1e6
        elif "K" in value:
            return float(value.replace("K", "").replace(",", "")) * 1e3
        else:
            return float(value.replace(",", ""))
    except ValueError:
        return None


def CLEAN_COLNAMES(df):
    """
    Cleans column names of a DataFrame by:
    - Converting to lowercase
    - Replacing spaces and hyphens with underscores
    - Replacing '%' with 'percent_'
    - Removing '*' characters
    - Stripping any leading/trailing whitespace

    Parameters:
    df (pd.DataFrame): The DataFrame with columns to clean.

    Returns:
    pd.DataFrame: A new DataFrame with cleaned column names.
    """
    df = df.copy()  # Create a copy to avoid modifying the original DataFrame
    df.columns = [
        re.sub(
            r"\s+",
            "",  # Remove all whitespace
            re.sub(
                r"\-",
                "_",  # Replace hyphens with underscores
                re.sub(
                    r"\%",
                    "percent_",  # Replace '%' with 'percent_'
                    re.sub(r"\*", "", col.lower()),
                ),
            ),
        )  # Remove '*' and make lowercase
        for col in df.columns
    ]
    return df
