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

# global variables
history_master_df = pd.DataFrame()
index_map = {
    'NIFTY': '^NSEI',
    'BANKNIFTY': '^NSEBANK',
    'FINNIFTY': '^CNXFIN',
    'MIDCPNIFTY': '^NSEMDCP50',
    'NIFTYNXT50': '^NSMIDCP'
}
index_list = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50']

# Path variables
root_dir = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(root_dir, 'data\\')
output_dir = os.path.join(root_dir, 'output\\')
output_path = os.path.join(output_dir, "Stock_Screener_api.html")
history_master_path = os.path.join(data_dir, f'history_master.csv')

# Create directories if not exists
dir_list = [data_dir, output_dir]
status = [os.makedirs(_dir, exist_ok=True) for _dir in dir_list if not os.path.exists(_dir)]

# Define Jinja2 environment
env = Environment(loader=FileSystemLoader(root_dir))

# get spot data
def spot_data(inst_name):
    url = "http://172.16.47.54:8006/livedataname"
    headers = {'esegment': '1', 'inst_name': inst_name}
    response = requests.get(url, headers=headers)
    return response.json()

# gets current data from XTS
def Downloadcurrentdata(historical_df):
    for each_symbol in historical_df['Name'].unique().tolist():
        today_data = spot_data(each_symbol)
        today_date = pd.to_datetime(today_data[5]).strftime("%Y-%m-%d")
        open = today_data[7]
        high = today_data[8]
        low = today_data[9]
        close = today_data[2]
        historical_df.loc[len(historical_df)] = [today_date, open, high, low, close, 0, 0, 0, each_symbol]
    historical_df.set_index('index', inplace=True)
    historical_df.index = pd.to_datetime(historical_df.index, utc=True)
    return historical_df

# gets ytd historical EOD data from YFinance
def DownloadHistoricalData(sym):
    # if sym not in ['^NSEI', '^NSEBANK', '^CNXFIN', '^NSEMDCP50', '^NSMIDCP']:
    #     ticker_name = sym + '.NS'
    # else:
    #     ticker_name = sym
    ticker_name = sym + '.NS'
    start_date = (datetime.datetime.now().date() - datetime.timedelta(days=360)).strftime('%Y-%m-%d')
    end_date = datetime.datetime.now().date().strftime('%Y-%m-%d')
    df1 = yf.Ticker(ticker_name)
    history_df = df1.history(start=start_date, end=end_date)
    # # one_minute_data = yf.download(ticker_name, interval='1m', progress=False) # we get one minute data for the past 7 busdays
    # # one_minute_data.index = pd.to_datetime(one_minute_data.index)
    # # yesterdays_data = one_minute_data[one_minute_data.index.date == (datetime.datetime.now().date() - datetime.timedelta(days=1))]
    # # yesterdays_data = yesterdays_data.resample('1D').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
    # # history = pd.concat([history, yesterdays_data])
    # # history = history[~history.index.duplicated(keep='last')]
    # if sym not in index_map.values():
    #     history_df['Name'] = sym
    # else:
    #     history_df['Name'] = [k for k, v in index_map.items() if v == sym][0]
    history_df['Name'] = sym
    history_df.index = pd.to_datetime(history_df.index).strftime('%Y-%m-%d')
    history_df.reset_index(inplace=True)
    history_df.rename(columns={'Date': 'index'}, inplace=True)
    return history_df

def chartink_link(url):
    return f'<a href="{url}" target="_blank">{url}</a>'

def PerformanceScanner(df):
    name = df['Name'].unique().tolist()[0].lower()
    hyperlink = f'https://chartink.com/stocks/{name}.html'
    df['% Chg'] = round(df['Close'].pct_change() * 100, 2)
    df['52WH'] = df['High'].shift(1).max()
    df['52WL'] = df['Low'].shift(1).min()
    df['5MV'] = round(df['Close'].rolling(5).mean(), 2)
    df['10MV'] = round(df['Close'].rolling(10).mean(), 2)
    df['20MV'] = round(df['Close'].rolling(20).mean(), 2)
    df['30MV'] = round(df['Close'].rolling(30).mean(), 2)
    df['40MV'] = round(df['Close'].rolling(40).mean(), 2)
    df['50MV'] = round(df['Close'].rolling(50).mean(), 2)
    df['100MV'] = round(df['Close'].rolling(100).mean(), 2)
    df['200MV'] = round(df['Close'].rolling(200).mean(), 2)
    df['PrevHigh'] = df['High'].shift(1)
    df['PrevLow'] = df['Low'].shift(1)
    df['PrevClose'] = df['Close'].shift(1)
    df['Prev5MV'] = df['5MV'].shift(1)
    df['Prev10MV'] = df['10MV'].shift(1)
    df['Prev20MV'] = df['20MV'].shift(1)
    df['Prev30MV'] = df['30MV'].shift(1)
    df['Prev40MV'] = df['40MV'].shift(1)
    df['Prev50MV'] = df['50MV'].shift(1)
    df['Prev100MV'] = df['100MV'].shift(1)
    df['Prev200MV'] = df['200MV'].shift(1)
    df['%_H'] = round(((df['Close'] - df['High']) / df['Close']) * 100, 2)
    df['%_L'] = round(((df['Close'] - df['Low']) / df['Close']) * 100, 2)
    df['%_52WH'] = round(((df['Close'] - df['52WH']) / df['Close']) * 100, 2)
    df['%_52WL'] = round(((df['Close'] - df['52WL']) / df['Close']) * 100, 2)
    df['URL'] = hyperlink
    df['Link'] = df['URL'].apply(chartink_link)
    df = round(df, 2)
    df = df[['Name', 'Close', 'PrevClose', 'High', 'Low', '% Chg', '%_H', '%_L',
                 '10MV', '20MV', 'Prev10MV', 'Prev20MV', '52WH', '52WL',
                 '%_52WH', '%_52WL', 'Link']]
    df.rename(columns={'Close': 'LTP'}, inplace=True)
    return df

# conditional formatting for the table
def Scanner(row):
    formatted_row = [''] * len(row)
    current_close = row["LTP"]
    previous_close = row["PrevClose"]
    moving_avg_10 = row["10MV"]
    moving_avg_20 = row["20MV"]
    previous_moving_avg_10 = row["Prev10MV"]
    previous_moving_avg_20 = row["Prev20MV"]
    distance_from_52_whigh = row["%_52WH"]
    distance_from_52_wlow = row["%_52WL"]
    distance_from_thigh = row["%_H"]
    distance_from_tlow = row["%_L"]
    if current_close > moving_avg_20 and previous_close < previous_moving_avg_20:
        formatted_row[row.index.get_loc('LTP')] = 'background-color: lightgreen;'
    if current_close < moving_avg_20 and previous_close > previous_moving_avg_20:
        formatted_row[row.index.get_loc('LTP')] = 'background-color: lightcoral;'
    if moving_avg_10 > moving_avg_20 and previous_moving_avg_10 < previous_moving_avg_20:
        formatted_row[row.index.get_loc('10MV')] = 'background-color: lightgreen;'
    if moving_avg_10 < moving_avg_20 and previous_moving_avg_10 > previous_moving_avg_20:
        formatted_row[row.index.get_loc('10MV')] = 'background-color: lightcoral;'
    if distance_from_52_whigh > 0:
        formatted_row[row.index.get_loc('%_52WH')] = 'background-color: lightgreen;'
    if distance_from_52_wlow < 0:
        formatted_row[row.index.get_loc('%_52WL')] = 'background-color: lightcoral;'
    if distance_from_thigh > -0.09:
        formatted_row[row.index.get_loc('%_H')] = 'background-color: lightgreen;'
    if distance_from_tlow < 0.09 and distance_from_tlow > 0:
        formatted_row[row.index.get_loc('%_L')] = 'background-color: lightcoral;'
    return formatted_row
def fetch_data():
    all_df = pd.read_excel(os.path.join(data_dir, 'All.xlsx'))
    ssymbol_list = all_df['Stocks'].tolist()
    # # index_list = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'NIFTYNXT50']
    # for each in symbol_list:
    #     # if each in symbol_list:
    #     #     symbol_list.remove(each)
    #     #     if each.upper() == 'NIFTY':
    #     #         symbol_list.append('^NSEI')
    #     #     elif each.upper() == 'BANKNIFTY':
    #     #         symbol_list.append('^NSEBANK')
    #     #     elif each.upper() == 'FINNIFTY':
    #     #         symbol_list.append('^CNXFIN')
    #     #     elif each.upper() == 'MIDCPNIFTY':
    #     #         symbol_list.append('^NSEMDCP50')
    #     #     else:
    #     #         symbol_list.append('^NSMIDCP')
    #     if each in index_map.keys():
    #         symbol_list.remove(each)
    #         # symbol_list.append(index_map[each])
    symbol_list = [value for value in ssymbol_list if value not in index_list]
    # ----------------------------------------------------------------
    if os.path.isfile(history_master_path):
        historical_df = pd.read_csv(history_master_path, index_col=False)
        hist_list = historical_df['Name'].unique().tolist()
        for sym in symbol_list:
            if sym not in hist_list:
                res = DownloadHistoricalData(sym)
                res.to_csv(history_master_path, mode='a', index=False, header=False)
    else:
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
    html_content = f'''
    <meta http-equiv="refresh" content="10">
    {html_content}
    '''

    # Write the HTML content to the file
    with open(output_path, 'w') as file:
        file.write(html_content)

# Main loop
def main():
    start_time = datetime.datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
    end_time = datetime.datetime.now().replace(hour=17, minute=31, second=0, microsecond=0)

    # Open the file in the browser once, initially
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

if __name__ == '__main__':
    main()