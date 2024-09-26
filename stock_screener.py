import os
import pandas as pd
import datetime
import yfinance as yf
import requests
import time
import tkinter as tk
from tkinter import messagebox
import pyautogui
from jinja2 import Environment, FileSystemLoader
import webbrowser
import sys

# global dict
glob_dict = {}
history_master_df = pd.DataFrame()

# Path variables
root_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(root_dir, 'data\\')
output_dir = os.path.join(root_dir, 'output\\')
# template_path = os.path.join(root_dir, "template.html")
output_path = os.path.join(output_dir, "Stock_Screener_api.html")
history_master_path = os.path.join(data_dir, f'history_master.csv')

# Create directories if not exists
dir_list = [data_dir, output_dir]
status = [os.makedirs(_dir, exist_ok=True) for _dir in dir_list if not os.path.exists(_dir)]

# Define Jinja2 environment
env = Environment(loader=FileSystemLoader(root_dir))

# Helper functions for stock data
def nifty_spot_data(inst_name):
    url = "http://172.16.47.54:8006/livedataname"
    headers = {'esegment': '1', 'inst_name': inst_name}
    response = requests.get(url, headers=headers)
    return response.json()

# def Downloadcurrentdata(historical_data):
#     for symbol in historical_data:
#         data = historical_data[symbol]
#         new_data = nifty_spot_data(symbol)
#         date = new_data[5]
#         open = new_data[7]
#         high = new_data[8]
#         low = new_data[9]
#         close = new_data[2]
#         data.loc[len(data)] = [date, open, high, low, close, 0, 0, 0, symbol]
#         data.set_index("index", inplace=True)
#         data.index = pd.to_datetime(data.index, utc=True)
#         historical_data[symbol] = data
#     return historical_data

def Downloadcurrentdata(historical_df):
    for each_symbol in historical_df['Name'].unique().tolist():
        # data = historical_data[symbol] # value
        # key is the symbol name
        # value has the historical df for the symbol
        today_data = nifty_spot_data(each_symbol)
        today_date = today_data[5]
        open = today_data[7]
        high = today_data[8]
        low = today_data[9]
        close = today_data[2]
        historical_df.loc[len(historical_df)] = [today_date, open, high, low, close, 0, 0, 0, each_symbol]

        # historical_data[key] = value
    historical_df.set_index('index', inplace=True)
    historical_df.index = pd.to_datetime(historical_df.index, format='ISO8601', utc=True)
    return historical_df

def DownloadHistoricalData(sym):
    # data = {}
    # if not os.path.isfile(history_master_df_path):
    #     for sym in sym_list:
    #         # name = df['Stocks'][i]
    #         ticker_name = sym + '.NS'
    #         start_date = (datetime.datetime.now().date() - datetime.timedelta(days=360)).strftime('%Y-%m-%d')
    #         end_date = datetime.datetime.now().date().strftime('%Y-%m-%d')
    #         # ----------------------------------------------------------------
    #         df1 = yf.Ticker(ticker_name)
    #         history_df = df1.history(start=start_date, end=end_date)
    #         # one_minute_data = yf.download(ticker_name, interval='1m', progress=False) # we get one minute data for the past 7 busdays
    #         # one_minute_data.index = pd.to_datetime(one_minute_data.index)
    #         # yesterdays_data = one_minute_data[one_minute_data.index.date == (datetime.datetime.now().date() - datetime.timedelta(days=1))]
    #         # yesterdays_data = yesterdays_data.resample('1D').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
    #         # history = pd.concat([history, yesterdays_data])
    #         # history = history[~history.index.duplicated(keep='last')]
    #         history_df['Name'] = sym
    #         history_df.reset_index(inplace=True)
    #         history_df.rename(columns={'Date': 'index'}, inplace = True)
    #         # data[name] = round(history, 2) # makes dictionary in format Symbol(str):history(df)
    #         # ----------------------------------------------------------------
    #         # if name not in list(glob_dict.keys()):
    #         #     df1 = yf.Ticker(ticker_name)
    #         #     history = df1.history(start=start_date, end=end_date)
    #         #     one_minute_data = yf.download(ticker_name, interval='1m', progress=False) # we get one minute data for the past 7 busdays
    #         #     one_minute_data.index = pd.to_datetime(one_minute_data.index)
    #         #     yesterdays_data = one_minute_data[one_minute_data.index.date == (datetime.datetime.now().date() - datetime.timedelta(days=1))]
    #         #     yesterdays_data = yesterdays_data.resample('1D').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
    #         #     history = pd.concat([history, yesterdays_data])
    #         #     history = history[~history.index.duplicated(keep='last')]
    #         #     history['Name'] = name
    #         #     history.reset_index(inplace=True)
    #         #     Downloadcurrentdata(name)
    #         #     # ----------------------------------------------------------------
    #         #     # data[name] = round(history, 2) # makes dictionary in format Symbol(str):history(df)
    #         #     glob_dict[name] = round(history, 2)
    #         #     print(f'{ticker_name} added to the global dict')
    #         break
    #
    # return history_df
    ticker_name = sym + '.NS'
    start_date = (datetime.datetime.now().date() - datetime.timedelta(days=360)).strftime('%Y-%m-%d')
    end_date = datetime.datetime.now().date().strftime('%Y-%m-%d')
    # ----------------------------------------------------------------
    df1 = yf.Ticker(ticker_name)
    history_df = df1.history(start=start_date, end=end_date)
    # one_minute_data = yf.download(ticker_name, interval='1m', progress=False) # we get one minute data for the past 7 busdays
    # one_minute_data.index = pd.to_datetime(one_minute_data.index)
    # yesterdays_data = one_minute_data[one_minute_data.index.date == (datetime.datetime.now().date() - datetime.timedelta(days=1))]
    # yesterdays_data = yesterdays_data.resample('1D').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
    # history = pd.concat([history, yesterdays_data])
    # history = history[~history.index.duplicated(keep='last')]
    history_df['Name'] = sym
    history_df.reset_index(inplace=True)
    history_df.rename(columns={'Date': 'index'}, inplace=True)
    # data[name] = round(history, 2) # makes dictionary in format Symbol(str):history(df)
    # ----------------------------------------------------------------
    # if name not in list(glob_dict.keys()):
    #     df1 = yf.Ticker(ticker_name)
    #     history = df1.history(start=start_date, end=end_date)
    #     one_minute_data = yf.download(ticker_name, interval='1m', progress=False) # we get one minute data for the past 7 busdays
    #     one_minute_data.index = pd.to_datetime(one_minute_data.index)
    #     yesterdays_data = one_minute_data[one_minute_data.index.date == (datetime.datetime.now().date() - datetime.timedelta(days=1))]
    #     yesterdays_data = yesterdays_data.resample('1D').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
    #     history = pd.concat([history, yesterdays_data])
    #     history = history[~history.index.duplicated(keep='last')]
    #     history['Name'] = name
    #     history.reset_index(inplace=True)
    #     Downloadcurrentdata(name)
    #     # ----------------------------------------------------------------
    #     # data[name] = round(history, 2) # makes dictionary in format Symbol(str):history(df)
    #     glob_dict[name] = round(history, 2)
    #     print(f'{ticker_name} added to the global dict')
    return history_df

def PerformanceScanner(df):
    df['% Chg'] = round(df['Close'].pct_change() * 100, 2)
    df['52WH'] = df['High'].shift(1).max()
    df['52WL'] = df['Low'].shift(1).min()
    df['10MV'] = round(df['Close'].rolling(10).mean(), 2)
    df['20MV'] = round(df['Close'].rolling(20).mean(), 2)
    df['PrevClose'] = df['Close'].shift(1)
    df = df[['Name', 'Close', 'PrevClose', 'High', 'Low', '% Chg', '10MV', '20MV', '52WH', '52WL']]
    df.rename(columns={'Close': 'LTP'}, inplace=True)
    return df

# Function to apply conditional formatting for the table
def Scanner(row):
    formatted_row = [''] * len(row)
    current_close = row["LTP"]
    previous_close = row["PrevClose"]
    moving_avg_10 = row["10MV"]
    moving_avg_20 = row["20MV"]
    if current_close > moving_avg_20 and previous_close < moving_avg_20:
        formatted_row[row.index.get_loc('LTP')] = 'background-color: lightgreen;'
    if current_close < moving_avg_20 and previous_close > moving_avg_20:
        formatted_row[row.index.get_loc('LTP')] = 'background-color: lightcoral;'
    return formatted_row

def fetch_data():
    all_df = pd.read_excel(os.path.join(data_dir, 'All.xlsx'))
    symbol_list = all_df['Stocks']
    # ----------------------------------------------------------------
    if os.path.isfile(history_master_path):
        historical_df = pd.read_csv(history_master_path, index_col=False)
        hist_list = historical_df['Name'].unique().tolist()
        for sym in symbol_list:
            if sym not in hist_list:
                res = DownloadHistoricalData(sym)
                # historical_df = pd.concat([historical_df, res])
                res.to_csv(history_master_path, mode='a', index=False, header=False)
    else:
        # dfa = pd.DataFrame(columns=['index', 'open', 'high', 'low', 'close', ''])
        # dfa.to_csv(history_master_path, index=False)
        i=0
        for sym in symbol_list:
            if i == 0:
                res = DownloadHistoricalData(sym)
                res.to_csv(history_master_path, mode='a', index=False, )
                i=1
            else:
                res = DownloadHistoricalData(sym)
                res.to_csv(history_master_path, mode='a', index=False, header=False)

    historical_df = pd.read_csv(history_master_path, index_col=False)
    # ----------------------------------------------------------------
    current_df = Downloadcurrentdata(historical_df)
    # current_df.set_index("index", inplace=True)
    # current_df.index = pd.to_datetime(current_df.index, utc=True)
    Alert_DF = []
    for name in current_df['Name'].unique().tolist():
        DF = current_df.query('Name == @name')
        Alerts = PerformanceScanner(DF).iloc[-1]
        Alert_DF.append(Alerts)
    Alert_DF = pd.DataFrame(Alert_DF)
    Alert_DF.sort_values('% Chg', ascending=False, inplace=True)
    return Alert_DF

def render_html(data_frame):
    # Add meta tag to auto-refresh every 10 seconds
    template = env.get_template('template.html')
    rows = [list(zip(row, Scanner(row))) for _, row in data_frame.iterrows()]
    html_content = template.render(columns=data_frame.columns, data=rows)

    # Add auto-refresh meta tag
    html_content = f'''
    <meta http-equiv="refresh" content="10">
    {html_content}
    '''

    # Write the HTML content to the file
    with open(output_path, 'w') as file:
        file.write(html_content)

# Main loop
start_time = datetime.datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
end_time = datetime.datetime.now().replace(hour=17, minute=31, second=0, microsecond=0)

# Open the file in the browser once, initially
# abs_path = os.path.abspath(output_path)
webbrowser.open(f'file://{output_path}')

while True:
    if datetime.datetime.now() < end_time:
        try:
            data_frame = fetch_data()
            render_html(data_frame)
            time.sleep(10)
        except Exception as e:
            # pyautogui.alert(text=f'Error: {e}', title='Error in Stock Screener')
            print(e)

    else:
        print(f'deleting history master file at {history_master_path} . . . ')
        os.remove(history_master_path)
        sys.exit()
        break
# res = fetch_data()