import requests
from bs4 import BeautifulSoup
import yfinance as yf
import pandas as pd
import re
from IPython.display import display
from tabulate import tabulate
import pytz
from datetime import datetime, timedelta, date
import os


def DOWNLOAD_CAF_PRICES_BY_CODE(p_codesource="vnindex", code_int="INDVNINDEX"):
    data_list = []
    to_continue = True
    k = 1

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    }

    while to_continue:
        # Construct the URL
        url = f"https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol={p_codesource}&StartDate=&EndDate=&PageIndex={k}&PageSize=100"

        try:
            # Make the request and parse JSON
            response = requests.get(url, headers=headers)
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


def DOWNLOAD_CAF_FOREIGN_BY_CODE(p_codesource="vnindex", code_int="INDVNINDEX"):
    data_list = []
    to_continue = True
    k = 1

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    }

    while to_continue:
        # Construct the URL
        url = f"https://s.cafef.vn/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx?Symbol={p_codesource}&StartDate=&EndDate=&PageIndex={k}&PageSize=100"

        try:
            # Make the request and parse JSON
            response = requests.get(url, headers=headers)
            x = response.json()

            # Check if data is available
            if "Data" in x and "Data" in x["Data"] and len(x["Data"]["Data"]) > 0:
                # Convert data to DataFrame
                x_data = pd.DataFrame(x["Data"]["Data"])

                # Clean and rename columns
                x_data = x_data.rename(columns=lambda col: col.strip().lower())
                x_data = pd.DataFrame(
                    {
                        "source": "CAF",
                        "codesource": p_codesource.upper(),
                        "ticker": p_codesource.upper(),
                        "date": pd.to_datetime(
                            x_data["ngay"], format="%d/%m/%Y", errors="coerce"
                        ),
                        "buy_volume_foreign": pd.to_numeric(
                            x_data["klmua"], errors="coerce"
                        ),
                        "sell_volume_foreign": pd.to_numeric(
                            x_data["klban"], errors="coerce"
                        ),
                        "buy_turnover_foreign": pd.to_numeric(
                            x_data["gtmua"], errors="coerce"
                        ),
                        "sell_turnover_foreign": pd.to_numeric(
                            x_data["gtban"], errors="coerce"
                        ),
                    }
                )

                if code_int:
                    x_data["code"] = code_int

                data_list.append(x_data)
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
        SHOW_DATA(data_all)
        return data_all
    else:
        return pd.DataFrame()  # Return empty DataFrame if no data


def DOWNLOAD_CAF_ORDER_BY_CODE(p_codesource="vnindex", code_int="INDVNINDEX"):
    data_list = []
    to_continue = True
    k = 1

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    }

    while to_continue:
        # Construct the URL
        url = f"https://s.cafef.vn/Ajax/PageNew/DataHistory/ThongKeDL.ashx?Symbol={p_codesource}&StartDate=&EndDate=&PageIndex={k}&PageSize=100"

        try:
            # Make the request and parse JSON
            response = requests.get(url, headers=headers)
            x = response.json()

            # Check if data is available
            if "Data" in x and "Data" in x["Data"] and len(x["Data"]["Data"]) > 0:
                # Convert data to DataFrame
                x_data = pd.DataFrame(x["Data"]["Data"])

                # Clean and rename columns
                x_data = x_data.rename(columns=lambda col: col.strip().lower())
                x_data = pd.DataFrame(
                    {
                        "source": "CAF",
                        "codesource": p_codesource.upper(),
                        "ticker": p_codesource.upper(),
                        "date": pd.to_datetime(
                            x_data["date"], format="%d/%m/%Y", errors="coerce"
                        ),
                        "buy_volume_order": pd.to_numeric(
                            x_data["kldatmua"], errors="coerce"
                        ),
                        "sell_volume_order": pd.to_numeric(
                            x_data["kldatban"], errors="coerce"
                        ),
                        "buy_order": pd.to_numeric(
                            x_data["solenhmua"], errors="coerce"
                        ),
                        "sell_order": pd.to_numeric(
                            x_data["solenhdatban"], errors="coerce"
                        ),
                    }
                )

                if code_int:
                    x_data["code"] = code_int

                data_list.append(x_data)
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
        # SHOW_DATA(data_all)
        return data_all
    else:
        return pd.DataFrame()  # Return empty DataFrame if no data
