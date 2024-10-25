import requests
from bs4 import BeautifulSoup
import yfinance as yf
import pandas as pd

def trainee_download_yah_shares(pCode='IBM'):
    # Initialize final data
    final_data = pd.DataFrame()
    
    # Step 1: Scrape Yahoo Finance for key statistics
    pURL = f"https://au.finance.yahoo.com/quote/{pCode}/key-statistics"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0',
        'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8'
    }
    
    response = requests.get(pURL, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Step 2: Extract float and shares outstanding values using XPath or HTML structure
    try:
        float_value = soup.select_one('#Col1-0-KeyStatistics-Proxy section div:nth-of-type(2) div:nth-of-type(2) div div:nth-of-type(2) div div table tbody tr:nth-of-type(5) td:nth-of-type(2)').text.strip()
        float_value = convert_number(float_value)
    except:
        float_value = None
    
    try:
        sharesout_value = soup.select_one('#Col1-0-KeyStatistics-Proxy section div:nth-of-type(2) div:nth-of-type(2) div div:nth-of-type(2) div div table tbody tr:nth-of-type(3) td:nth-of-type(2)').text.strip()
        sharesout_value = convert_number(sharesout_value)
    except:
        sharesout_value = None

    # Step 3: Get market capitalization using yfinance
    stock = yf.Ticker(pCode)
    market_cap = stock.info.get('marketCap', None)

    # If yfinance didn't retrieve marketCap, fall back to scraping the page for it
    if market_cap is None:
        pURL = f"https://au.finance.yahoo.com/quote/{pCode}/"
        response = requests.get(pURL, headers=headers)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        try:
            market_cap_value = soup.select_one('#quote-summary div:nth-of-type(2) table tbody tr:nth-of-type(1) td:nth-of-type(2)').text.strip()
            market_cap = convert_number(market_cap_value)
        except:
            market_cap = None

    # Step 4: Create the final dataframe
    final_data = pd.DataFrame({
        'codesource': [pCode],
        'share_outstanding': [sharesout_value],
        'float': [float_value],
        'market_cap': [market_cap]
    })

    # Display the final data (You could use print or logging)
    print(final_data)
    
    return final_data

def convert_number(value):
    """Helper function to convert strings like '1.2B' into a numeric value."""
    try:
        if 'B' in value:
            return float(value.replace('B', '').replace(',', '')) * 1e9
        elif 'M' in value:
            return float(value.replace('M', '').replace(',', '')) * 1e6
        elif 'K' in value:
            return float(value.replace('K', '').replace(',', '')) * 1e3
        else:
            return float(value.replace(',', ''))
    except ValueError:
        return None

# Example usage:
x = trainee_download_yah_shares('AAPL')
