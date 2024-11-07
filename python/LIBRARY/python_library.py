import requests
from bs4 import BeautifulSoup
import yfinance as yf
import pandas as pd
import re
from IPython.display import display
from tabulate import tabulate
import pytz
from datetime import datetime, timedelta


def EPOCH_TO_DATE(x_epoch=1651671000, convert_to="CHARACTER"):
    if convert_to == "CHARACTER":
        x_res = datetime.fromtimestamp(x_epoch).strftime("%Y-%m-%d %H:%M:%S")
    elif convert_to == "DATETIME":
        x_res = datetime.fromtimestamp(x_epoch)
    elif convert_to == "DATE":
        x_res = datetime.fromtimestamp(x_epoch).date()
    else:
        raise ValueError(
            "Invalid value for convert_to. Choose from 'CHARACTER', 'DATETIME', or 'DATE'."
        )

    return x_res


def DOWNLOAD_ENT_PRICES_BY_CODE(
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
        df["source"] = "ENT"
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


def DOWNLOAD_CAF_PRICES_BY_CODE(p_codesource="vnindex", code_int="INDVNINDEX"):
    data_list = []
    to_continue = True
    k = 1

    while to_continue:
        # Construct the URL
        url = f"https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol={p_codesource}&StartDate=&EndDate=&PageIndex={k}&PageSize=100"

        try:
            # Make the request and parse JSON
            response = requests.get(url)
            x = response.json()

            # Check if data is available
            if "Data" in x and "Data" in x["Data"] and len(x["Data"]["Data"]) > 0:
                # Convert data to DataFrame
                df = pd.DataFrame(x["Data"]["Data"])

                # Clean and rename columns
                df = df.rename(columns=lambda col: col.strip().lower())
                df["thaydoi"] = df["thaydoi"].astype(str)
                df["khoiluongkhoplenh"] = df["khoiluongkhoplenh"].astype(str)
                df["giatrikhoplenh"] = df["giatrikhoplenh"].astype(str)
                # Select and transform columns as per requirements
                df = pd.DataFrame(
                    {
                        "source": "CAF",
                        "codesource": p_codesource.upper(),
                        "ticker": p_codesource.upper(),
                        "date": pd.to_datetime(
                            df["ngay"], format="%d/%m/%Y", errors="coerce"
                        ),
                        "open": pd.to_numeric(df["giamocua"], errors="coerce"),
                        "high": pd.to_numeric(df["giacaonhat"], errors="coerce"),
                        "low": pd.to_numeric(df["giathapnhat"], errors="coerce"),
                        "close_adj": pd.to_numeric(df["giadieuchinh"], errors="coerce"),
                        "close": pd.to_numeric(df["giadongcua"], errors="coerce"),
                        "change": pd.to_numeric(
                            df["thaydoi"].str.split("(", expand=True)[0],
                            errors="coerce",
                        ),
                        "varpc": pd.to_numeric(
                            df["thaydoi"].str.extract(r"\((.*?)%\)")[0], errors="coerce"
                        ),
                        "volume": pd.to_numeric(
                            df["khoiluongkhoplenh"].str.replace(",", ""),
                            errors="coerce",
                        ),
                        "turnover": pd.to_numeric(
                            df["giatrikhoplenh"].str.replace(",", ""), errors="coerce"
                        ),
                    }
                )

                data_list.append(df)
                k += 1  # Move to the next page
            else:
                to_continue = False  # Stop if there's no more data

        except Exception as e:
            print(f"Error retrieving or processing data: {e}")
            to_continue = False  # Stop in case of error

    # Combine all pages into a single DataFrame
    if data_list:
        data_all = pd.concat(data_list, ignore_index=True)
        data_all = data_all.sort_values(by="date").reset_index(drop=True)

        # Calculate additional columns
        data_all["reference"] = data_all["close"] - data_all["change"]
        data_all["rt"] = data_all["change"] / data_all["reference"]
        data_all["code"] = code_int.upper()

        SHOW_DATA(data_all)
        return data_all
    else:
        return pd.DataFrame()  # Return empty DataFrame if no data


# x = DOWNLOAD_CAF_PRICES_BY_CODE(p_codesource="VNM", code_int="")


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
