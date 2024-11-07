import requests
from bs4 import BeautifulSoup
import yfinance as yf
import pandas as pd


def trainee_download_yah_shares(pCode="IBM"):
    # Initialize final data
    final_data = pd.DataFrame()

    # Step 1: Scrape Yahoo Finance for key statistics
    pURL = f"https://au.finance.yahoo.com/quote/{pCode}/key-statistics"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    }

    response = requests.get(pURL, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    # Step 2: Extract float and shares outstanding values using XPath or HTML structure
    try:
        float_value = soup.select_one(
            "#Col1-0-KeyStatistics-Proxy section div:nth-of-type(2) div:nth-of-type(2) div div:nth-of-type(2) div div table tbody tr:nth-of-type(5) td:nth-of-type(2)"
        ).text.strip()
        float_value = convert_number(float_value)
    except:
        float_value = None

    try:
        sharesout_value = soup.select_one(
            "#Col1-0-KeyStatistics-Proxy section div:nth-of-type(2) div:nth-of-type(2) div div:nth-of-type(2) div div table tbody tr:nth-of-type(3) td:nth-of-type(2)"
        ).text.strip()
        sharesout_value = convert_number(sharesout_value)
    except:
        sharesout_value = None

    # Step 3: Get market capitalization using yfinance
    stock = yf.Ticker(pCode)
    market_cap = stock.info.get("marketCap", None)

    # If yfinance didn't retrieve marketCap, fall back to scraping the page for it
    if market_cap is None:
        pURL = f"https://au.finance.yahoo.com/quote/{pCode}/"
        response = requests.get(pURL, headers=headers)
        soup = BeautifulSoup(response.content, "html.parser")

        try:
            market_cap_value = soup.select_one(
                "#quote-summary div:nth-of-type(2) table tbody tr:nth-of-type(1) td:nth-of-type(2)"
            ).text.strip()
            market_cap = convert_number(market_cap_value)
        except:
            market_cap = None

    # Step 4: Create the final dataframe
    final_data = pd.DataFrame(
        {
            "codesource": [pCode],
            "share_outstanding": [sharesout_value],
            "float": [float_value],
            "market_cap": [market_cap],
        }
    )

    # Display the final data (You could use print or logging)
    print(final_data)

    return final_data


def convert_number(value):
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


# Example usage:
x = trainee_download_yah_shares("AAPL")


import requests
import pandas as pd
from datetime import datetime, timedelta
import os


def epoch_to_date(x_epoch, convert_to="CHARACTER"):
    if convert_to == "CHARACTER":
        return datetime.fromtimestamp(x_epoch).strftime("%Y-%m-%d %H:%M:%S")
    elif convert_to == "DATETIME":
        return datetime.fromtimestamp(x_epoch)
    elif convert_to == "DATE":
        return datetime.fromtimestamp(x_epoch).date()
    else:
        raise ValueError(
            "Invalid value for convert_to. Choose from 'CHARACTER', 'DATETIME', or 'DATE'."
        )


def download_entrade_index_intraday(
    type="index",
    codesource="VN30",
    code="INDVN30",
    NbMinutes=11 * 365 * 24 * 60,
    OffsetDays=0,
    pInterval="1",
):

    # Calculate End.time and Start.time in epoch time
    end_time = round(datetime.now().timestamp()) - (OffsetDays * 24 * 60 * 60 - 1)
    start_time = round(end_time - NbMinutes * 60)

    # Construct the URL
    p_url = f"https://services.entrade.com.vn/chart-api/v2/ohlcs/{type}?from={start_time}&to={end_time}&symbol={codesource}&resolution={pInterval}"
    print("Download URL:", p_url)

    # Request data from URL
    response = requests.get(p_url)

    # Print raw JSON data for inspection
    try:
        data = response.json()
        print(
            "Raw JSON data:", data
        )  # Inspect the structure here to ensure it's what we expect
    except ValueError as e:
        print(f"Failed to parse JSON data. Error: {e}")
        return pd.DataFrame()

    # Convert data to DataFrame if available
    if len(data) > 1:
        df = pd.DataFrame(data)
        df["timestamp_vn"] = pd.to_datetime(df["t"], unit="s") + timedelta(hours=7)
        df["open"] = df["o"]
        df["high"] = df["h"]
        df["low"] = df["l"]
        df["close"] = df["c"]
        df["volume"] = df["v"]

        # Additional calculations
        df["hhmmss"] = df["timestamp_vn"].dt.strftime("%H:%M")
        df["date"] = df["timestamp_vn"].dt.date
        df["source"] = "ENTRADE"
        df["code"] = code
        df["codesource"] = codesource
        df["ticker"] = codesource
        df["type"] = "IND" if type == "index" else "STK"
        df["timestamp"] = df["timestamp_vn"].astype(str)

        # Placeholder values for ref1 mappings
        ref1 = {"iso2": "VN", "iso3": "VNM", "country": "VIETNAM", "continent": "ASIA"}

        for col, val in ref1.items():
            df[col] = val
        df = df.drop(columns=["o", "h", "l", "c", "v"])

        return df
    else:
        print("No data rows available")
        return pd.DataFrame()


# Example usage
final_data = download_entrade_index_intraday()
print(final_data.head())
