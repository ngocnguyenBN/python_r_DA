import requests
import yfinance as yf
import pandas as pd
import re
import time
import pytz
import os
import schedule
import numpy as np

from bs4 import BeautifulSoup
from IPython.display import display
from tabulate import tabulate
from datetime import datetime, timedelta, date
from pathlib import Path
from functools import reduce

# from python_library import *


def check_time_between(start, end):
    """Check if the current time is between `start` and `end`."""
    now = datetime.now().time()
    start_time = datetime.strptime(start, "%H:%M").time()
    end_time = datetime.strptime(end, "%H:%M").time()
    return start_time <= now <= end_time


def get_last_weekdays(days):
    """Get the last `days` weekdays."""
    today = datetime.today()
    last_dates = [today - timedelta(days=i) for i in range(days * 2)]
    return pd.to_datetime([d for d in last_dates if d.weekday() < 5][:days])


def save_history(file_path, new_data, subset=None):
    if not os.path.exists(file_path):
        history = pd.DataFrame()
    else:
        history = pd.read_pickle(file_path)

    history = pd.concat([new_data, history], ignore_index=True).drop_duplicates(
        subset=subset
    )
    history.to_pickle(file_path)

    print(f"History saved to {file_path}. Current record count: {len(history)}")


def SHOW_DATA(df):
    print(tabulate(df, headers="keys", tablefmt="pretty", showindex=False))


def DOWNLOAD_CAF_PRICES_BY_CODE(
    p_codesource="vnindex", code_int="INDVNINDEX", ToHistory=False
):
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
                if not ToHistory:
                    break
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
        if code_int:
            data_all["code"] = code_int.upper()
        else:
            data_all["code"] = f"STKVN{p_codesource}"

        SHOW_DATA(data_all)
        return data_all
    else:
        return pd.DataFrame()  # Return empty DataFrame if no data


def DOWNLOAD_CAF_FOREIGN_BY_CODE(
    p_codesource="vnindex", code_int="INDVNINDEX", ToHistory=False
):
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
                if not ToHistory:
                    break
            else:
                to_continue = False  # Stop if there's no more data

        except Exception as e:
            print(f"Error retrieving or processing data: {e}")
            to_continue = False  # Stop in case of error

    # Combine all pages into a single DataFrame
    if data_list:
        data_all = pd.concat(data_list, ignore_index=True)
        data_all = data_all.sort_values(by="date").reset_index(drop=True)
        if code_int:
            data_all["code"] = code_int.upper()
        else:
            data_all["code"] = f"STKVN{p_codesource}"
        SHOW_DATA(data_all)
        return data_all
    else:
        return pd.DataFrame()  # Return empty DataFrame if no data


def DOWNLOAD_CAF_ORDER_BY_CODE(
    p_codesource="vnindex", code_int="INDVNINDEX", ToHistory=False
):
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
                if not ToHistory:
                    break
            else:
                to_continue = False  # Stop if there's no more data

        except Exception as e:
            print(f"Error retrieving or processing data: {e}")
            to_continue = False  # Stop in case of error

    # Combine all pages into a single DataFrame
    if data_list:
        data_all = pd.concat(data_list, ignore_index=True)
        data_all = data_all.sort_values(by="date").reset_index(drop=True)
        if code_int:
            data_all["code"] = code_int.upper()
        else:
            data_all["code"] = f"STKVN{p_codesource}"
        # SHOW_DATA(data_all)
        return data_all
    else:
        return pd.DataFrame()  # Return empty DataFrame if no data


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
        df["open"] = df["o"] * 1000
        df["high"] = df["h"] * 1000
        df["low"] = df["l"] * 1000
        df["close"] = df["c"] * 1000
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


def DOWNLOAD_IMBALANCE_DATA_BY_LIST(list_codes=None, do_history=False):
    """
    Downloads and processes imbalance data for a list of stock codes.

    Parameters:
        list_codes (list): A list of stock codes to process.

    Returns:
        pd.DataFrame: Final processed data with imbalance information.
    """
    # Split list into chunks of 100
    if len(list_codes) > 0:
        split_lists = [list_codes[i : i + 100] for i in range(0, len(list_codes), 100)]
        final_data = pd.DataFrame()

        for k, sublist in enumerate(split_lists, start=1):
            lock_file = Path(f"D:/python_r_DA/waiting_{k}.lock")

            # Check if lock file exists
            if lock_file.exists():
                lock_age = time.time() - lock_file.stat().st_mtime
                if lock_age > 600:  # Lock older than 10 minutes
                    print(f"Lock file exists too long, removing: {lock_file}")
                    lock_file.unlink()

            # Create lock file if it doesn't exist
            if not lock_file.exists():
                lock_file.touch()
                try:
                    x_list = []
                    code = "FPT"
                    for code in sublist:
                        try:
                            # Fetch foreign data
                            foreign_data = DOWNLOAD_CAF_FOREIGN_BY_CODE(
                                p_codesource=code,
                                code_int="",
                                ToHistory=do_history,
                            )

                            # Fetch imbalance data based on time
                            # if check_time_between("09:15", "23:59"):
                            #     imbalance_data = download_funan_order_data_by_code(code)
                            # else:
                            imbalance_data = DOWNLOAD_CAF_ORDER_BY_CODE(
                                p_codesource=code,
                                code_int="",
                                ToHistory=do_history,
                            )

                            if not imbalance_data.empty:
                                # Merge foreign data
                                if not foreign_data.empty:
                                    imbalance_data = pd.merge(
                                        imbalance_data,
                                        foreign_data[
                                            [
                                                "code",
                                                "date",
                                                "buy_volume_foreign",
                                                "sell_volume_foreign",
                                                "buy_turnover_foreign",
                                                "sell_turnover_foreign",
                                            ]
                                        ],
                                        on=["code", "date"],
                                        how="left",
                                    )
                                    imbalance_data["date"] = pd.to_datetime(
                                        imbalance_data["date"]
                                    )
                                # Fetch price data
                                # price_data = DOWNLOAD_ENT_PRICES_BY_CODE(
                                #     type="stock",
                                #     codesource=code,
                                #     code=f"STKVN{code}",
                                #     NbMinutes=11 * 365 * 24 * 60,
                                #     OffsetDays=0,
                                #     pInterval="1",
                                # )
                                price_data = DOWNLOAD_CAF_PRICES_BY_CODE(
                                    p_codesource=code,
                                    code_int="",
                                    ToHistory=do_history,
                                )
                                if not price_data.empty and price_data.shape[0] > 5:
                                    # price_data = price_data.sort_values(
                                    #     "timestamp_vn", ascending=False
                                    # ).drop_duplicates(subset=["date"])
                                    price_data[["open", "low", "high", "close"]] *= 1000
                                    price_data["date"] = pd.to_datetime(
                                        price_data["date"]
                                    )

                                    imbalance_data = pd.merge(
                                        imbalance_data,
                                        price_data[["code", "date", "high", "low"]],
                                        on=["code", "date"],
                                        how="left",
                                    )

                                    SHOW_DATA(imbalance_data)

                                    # Calculate turnovers
                                    mid_price = (
                                        imbalance_data["high"] + imbalance_data["low"]
                                    ) / 2
                                    imbalance_data["buy_turnover"] = (
                                        mid_price * imbalance_data["buy_volume_order"]
                                    )
                                    imbalance_data["sell_turnover"] = (
                                        mid_price * imbalance_data["sell_volume_order"]
                                    )

                                    # Filter rows with valid data
                                    imbalance_data = imbalance_data.dropna(
                                        subset=["low", "high"]
                                    )

                                x_list.append(imbalance_data)
                        except Exception as e:
                            print(f"Error processing code {code}: {e}")

                    # Combine data and save
                    if x_list:
                        combined_data = pd.concat(x_list, ignore_index=True)
                        if not os.path.exists(
                            "D:/python_r_DA/data/dbl_download_data_imbalance_day.pkl"
                        ):
                            old_data = pd.DataFrame()
                        else:
                            old_data = pd.read_pickle(
                                "D:/python_r_DA/data/dbl_download_data_imbalance_day.pkl"
                            )

                        final_data = pd.concat(
                            [combined_data, old_data], ignore_index=True
                        ).drop_duplicates(subset=["code", "date"])

                        # Filter for the last 10 weekdays
                        if not do_history:
                            last_weekdays = get_last_weekdays(10)
                            final_data = final_data[
                                final_data["date"] >= last_weekdays.min()
                            ]
                            final_data.to_pickle(
                                "D:/python_r_DA/data/dbl_download_data_imbalance_day.pkl"
                            )
                finally:
                    lock_file.unlink()

        ToMergeHistory = CHECK_SAVE_LOCK(
            p_option="MERGE_DATA_IMBALANCE_HISTORY",
            p_action="COMPARE",
            nb_seconds=3600,
            to_print=False,
        )

        if ToMergeHistory:
            if not os.path.exists(
                "D:/python_r_DA/data/dbl_download_data_imbalance_history.pkl"
            ):
                history = pd.DataFrame()
            else:
                history = pd.read_pickle(
                    "D:/python_r_DA/data/dbl_download_data_imbalance_history.pkl"
                )

            history = pd.concat(
                [final_data, history], ignore_index=True
            ).drop_duplicates(subset=["code", "date"])
            history.to_pickle(
                "D:/python_r_DA/data/dbl_download_data_imbalance_history.pkl"
            )
            CHECK_SAVE_LOCK(
                p_option="MERGE_DATA_IMBALANCE_HISTORY",
                p_action="SAVE",
                nb_seconds=3600,
                to_print=False,
            )

        return final_data
    else:
        return pd.DataFrame()


# file_path = "D:/python_r_DA/list_file/list_stkvn.txt"
# with open(file_path, "r") as file:
#     # Skip the first line (column name)
#     next(file)

#     # Read the rest of the lines and strip any leading/trailing whitespace
#     codes_list = [line.strip() for line in file]

# dataa = DOWNLOAD_IMBALANCE_DATA_BY_LIST(list_codes=codes_list, do_history=True)


def CACULATE_IMBALANCE_INDEX(ToHistory=False, ratio_accept=0.001, ToSave=True):
    if ToHistory:
        old_data = pd.read_pickle(
            "D:/python_r_DA/data/dbl_download_data_imbalance_history.pkl"
        )
    else:
        old_data = pd.read_pickle(
            "D:/python_r_DA/data/dbl_download_data_imbalance_day.pkl"
        )
    # Calculate imf
    old_data.loc[
        old_data["buy_volume_foreign"] + old_data["sell_volume_foreign"] != 0, "imf"
    ] = (
        (old_data["buy_volume_foreign"] - old_data["sell_volume_foreign"])
        / (old_data["buy_volume_foreign"] + old_data["sell_volume_foreign"])
        * 100
    )
    old_data.loc[
        old_data["buy_volume_foreign"] + old_data["sell_volume_foreign"] == 0, "imf"
    ] = np.nan

    # Calculate imf_turnover
    old_data.loc[
        old_data["buy_turnover_foreign"] + old_data["sell_turnover_foreign"] != 0,
        "imf_turnover",
    ] = (
        (old_data["buy_turnover_foreign"] - old_data["sell_turnover_foreign"])
        / (old_data["buy_turnover_foreign"] + old_data["sell_turnover_foreign"])
        * 100
    )
    old_data.loc[
        old_data["buy_turnover_foreign"] + old_data["sell_turnover_foreign"] == 0,
        "imf_turnover",
    ] = np.nan

    # Calculate imv
    old_data.loc[
        old_data["buy_volume_order"] + old_data["sell_volume_order"] != 0, "imv"
    ] = (
        (old_data["buy_volume_order"] - old_data["sell_volume_order"])
        / (old_data["buy_volume_order"] + old_data["sell_volume_order"])
        * 100
    )
    old_data.loc[
        old_data["buy_volume_order"] + old_data["sell_volume_order"] == 0, "imv"
    ] = np.nan

    # Calculate imo
    old_data.loc[old_data["buy_order"] + old_data["sell_order"] != 0, "imo"] = (
        (old_data["buy_order"] - old_data["sell_order"])
        / (old_data["buy_order"] + old_data["sell_order"])
        * 100
    )
    old_data.loc[old_data["buy_order"] + old_data["sell_order"] == 0, "imo"] = np.nan

    # Calculate imt
    old_data.loc[old_data["buy_turnover"] + old_data["sell_turnover"] != 0, "imt"] = (
        (old_data["buy_turnover"] - old_data["sell_turnover"])
        / (old_data["buy_turnover"] + old_data["sell_turnover"])
        * 100
    )
    old_data.loc[old_data["buy_turnover"] + old_data["sell_turnover"] == 0, "imt"] = (
        np.nan
    )

    old_data = old_data.dropna(subset=["imo", "imv"])

    old_data["code_und"] = old_data["code"]

    cols = ["imo", "imv", "imf", "imt", "imf_turnover"]
    old_data["imbalance_number"] = old_data[cols].fillna(0).sum(axis=1) / old_data[
        cols
    ].notna().sum(axis=1)

    old_data.loc[
        (old_data["imbalance_number"] >= -100) & (old_data["imbalance_number"] < -60),
        "imbalance_level",
    ] = "VERY FEAR"
    old_data.loc[
        (old_data["imbalance_number"] >= -60) & (old_data["imbalance_number"] < -20),
        "imbalance_level",
    ] = "FEAR"
    old_data.loc[
        (old_data["imbalance_number"] >= -20) & (old_data["imbalance_number"] < 20),
        "imbalance_level",
    ] = "NEUTRAL"
    old_data.loc[
        (old_data["imbalance_number"] >= 20) & (old_data["imbalance_number"] < 60),
        "imbalance_level",
    ] = "GREED"
    old_data.loc[
        (old_data["imbalance_number"] >= 60) & (old_data["imbalance_number"] <= 100),
        "imbalance_level",
    ] = "VERY GREED"

    old_data["code"] = "INDIMB" + old_data["code_und"]
    # old_data["name_und"] = old_data["name"]
    # old_data["name"] = "IMBALANCE " + old_data["name_und"]
