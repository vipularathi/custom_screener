# # import pandas as pd
# # import datetime
# # import yfinance as yf
# # import requests
# # import time
# # from jinja2 import Environment, FileSystemLoader
# # from IPython.display import display, clear_output, HTML
# # import tkinter as tk
# # from tkinter import messagebox
# # import pyautogui
# #
# # # Path variables
# # path = r"D:\screener\custom_screener_test\\"
# # template_path = path + "template.html"
# # output_path = path + "Stock_Screener_api.html"
# #
# # # Define Jinja2 environment
# # env = Environment(loader=FileSystemLoader(path))
# #
# # # Helper functions for stock data
# # def nifty_spot_data(inst_name):
# #     url = "http://172.16.47.54:8006/livedataname"
# #     headers = {'esegment': '1', 'inst_name': inst_name}
# #     response = requests.get(url, headers=headers)
# #     return response.json()
# #
# # def Downloadcurrentdata(historical_data):
# #     for symbol in historical_data:
# #         data = historical_data[symbol]
# #         new_data = nifty_spot_data(symbol)
# #         date = new_data[5]
# #         open = new_data[7]
# #         high = new_data[8]
# #         low = new_data[9]
# #         close = new_data[2]
# #         data.loc[len(data)] = [date, open, high, low, close, 0, 0, 0, symbol]
# #         data.set_index("index", inplace=True)
# #         data.index = pd.to_datetime(data.index, utc=True)
# #         historical_data[symbol] = data
# #     return historical_data
# #
# # def DownloadHistoricalData(df):
# #     data = {}
# #     for i in range(len(df)):
# #         name = df['Stocks'][i]
# #         start_date = (datetime.datetime.now().date() - datetime.timedelta(days=360)).strftime('%Y-%m-%d')
# #         end_date = datetime.datetime.now().date().strftime('%Y-%m-%d')
# #         df1 = yf.Ticker(df['Ticker'][i])
# #         history = df1.history(start=start_date, end=end_date)
# #         one_minute_data = yf.download(df['Ticker'][i], interval='1m', progress=False)
# #         one_minute_data.index = pd.to_datetime(one_minute_data.index)
# #         yesterdays_data = one_minute_data[one_minute_data.index.date == (datetime.datetime.now().date() - datetime.timedelta(days=1))]
# #         yesterdays_data = yesterdays_data.resample('1D').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
# #         history = pd.concat([history, yesterdays_data])
# #         history = history[~history.index.duplicated(keep='last')]
# #         history['Name'] = name
# #         history.reset_index(inplace=True)
# #         data[name] = round(history, 2)
# #     return data
# #
# # # def PerformanceScanner(data):
# # #     data['% Chg'] = round(data['Close'].pct_change() * 100, 2)
# # #     data['52WH'] = data['High'].shift(1).max()
# # #     data['52WL'] = data['Low'].shift(1).min()
# # #     data['10MV'] = round(data['Close'].rolling(10).mean(), 2)
# # #     data['20MV'] = round(data['Close'].rolling(20).mean(), 2)
# # #     data['PrevClose'] = data['Close'].shift(1)
# # #     data = data[['Name', 'Close', 'PrevClose', 'High', 'Low', '% Chg', '10MV', '20MV', '52WH', '52WL']]
# # #     data.rename(columns={'Close': 'LTP'}, inplace=True)
# # #     return data
# #
# # def PerformanceScanner(data):
# #     todays_date = datetime.datetime.now().date().strftime('%Y-%m-%d')
# #     previous_day = data.index[-2]
# #     previous_day = previous_day.strftime('%Y-%m-%d')
# #     name = data['Name'][0].lower()
# #     hyperlink = f'https://chartink.com/stocks/{name}.html'
# #     data['% Chg'] = round(data['Close'].pct_change() * 100, 2)
# #     data['52WH'] = data['High'].shift(1).max()
# #     data['52WL'] = data['Low'].shift(1).min()
# #     data['5MV'] = round(data['Close'].rolling(5).mean(), 2)
# #     data['10MV'] = round(data['Close'].rolling(10).mean(), 2)
# #     data['20MV'] = round(data['Close'].rolling(20).mean(), 2)
# #     data['30MV'] = round(data['Close'].rolling(30).mean(), 2)
# #     data['40MV'] = round(data['Close'].rolling(40).mean(), 2)
# #     data['50MV'] = round(data['Close'].rolling(50).mean(), 2)
# #     data['100MV'] = round(data['Close'].rolling(100).mean(), 2)
# #     data['200MV'] = round(data['Close'].rolling(200).mean(), 2)
# #
# #     data['PrevHigh'] = data['High'].shift(1)
# #     data['PrevLow'] = data['Low'].shift(1)
# #     data['PrevClose'] = data['Close'].shift(1)
# #     data['Prev5MV'] = data['5MV'].shift(1)
# #     data['Prev10MV'] = data['10MV'].shift(1)
# #     data['Prev20MV'] = data['20MV'].shift(1)
# #     data['Prev30MV'] = data['30MV'].shift(1)
# #     data['Prev40MV'] = data['40MV'].shift(1)
# #     data['Prev50MV'] = data['50MV'].shift(1)
# #     data['Prev100MV'] = data['100MV'].shift(1)
# #     data['Prev200MV'] = data['200MV'].shift(1)
# #
# #     data['%_H'] = round(((data['Close'] - data['High']) / data['Close']) * 100, 2)
# #     data['%_L'] = round(((data['Close'] - data['Low']) / data['Close']) * 100, 2)
# #     data['%_52WH'] = round(((data['Close'] - data['52WH']) / data['Close']) * 100, 2)
# #     data['%_52WL'] = round(((data['Close'] - data['52WL']) / data['Close']) * 100, 2)
# #     data['URL'] = hyperlink
# #     data['Link'] = data['URL'].apply(make_clickable)
# #     data = round(data, 2)
# #
# #     data = data[['Name', 'Close', 'PrevClose', 'High', 'Low', '% Chg', '%_H', '%_L',
# #                  '10MV', '20MV', 'Prev10MV', 'Prev20MV', '52WH', '52WL',
# #                  '%_52WH', '%_52WL', 'Link']]
# #     data.rename(columns={'Close': 'LTP'}, inplace=True)
# #     return data
# #
# # # Function to apply conditional formatting for the table
# # def Scanner(row):
# #     formatted_row = [''] * len(row)
# #     current_close = row["LTP"]
# #     previous_close = row["PrevClose"]
# #     moving_avg_10 = row["10MV"]
# #     moving_avg_20 = row["20MV"]
# #     previous_moving_avg_10 = row["Prev10MV"]
# #     previous_moving_avg_20 = row["Prev20MV"]
# #     distance_from_52_whigh = row["%_52WH"]
# #     distance_from_52_wlow = row["%_52WL"]
# #     distance_from_thigh = row["%_H"]
# #     distance_from_tlow = row["%_L"]
# #
# #     if current_close > moving_avg_20 and previous_close < previous_moving_avg_20:
# #         formatted_row[row.index.get_loc('LTP')] = 'background-color: lightgreen;'
# #     if current_close < moving_avg_20 and previous_close > previous_moving_avg_20:
# #         formatted_row[row.index.get_loc('LTP')] = 'background-color: lightcoral;'
# #     if moving_avg_10 > moving_avg_20 and previous_moving_avg_10 < previous_moving_avg_20:
# #         formatted_row[row.index.get_loc('10MV')] = 'background-color: lightgreen;'
# #     if moving_avg_10 < moving_avg_20 and previous_moving_avg_10 > previous_moving_avg_20:
# #         formatted_row[row.index.get_loc('10MV')] = 'background-color: lightcoral;'
# #     if distance_from_52_whigh > 0:
# #         formatted_row[row.index.get_loc('%_52WH')] = 'background-color: lightgreen;'
# #     if distance_from_52_wlow < 0:
# #         formatted_row[row.index.get_loc('%_52WL')] = 'background-color: lightcoral;'
# #     if distance_from_thigh > -0.09:
# #         formatted_row[row.index.get_loc('%_H')] = 'background-color: lightgreen;'
# #     if distance_from_tlow < 0.09 and distance_from_tlow > 0:
# #         formatted_row[row.index.get_loc('%_L')] = 'background-color: lightcoral;'
# #     return formatted_row
# #
# # def fetch_data(path=path):
# #     screener_list = pd.read_excel(path + 'All.xlsx')
# #     historical_data = DownloadHistoricalData(screener_list)
# #     historical_data = Downloadcurrentdata(historical_data)
# #     Alert_DF = []
# #     for name in historical_data:
# #         DF = historical_data[name]
# #         Alerts = PerformanceScanner(DF).iloc[-1]
# #         Alert_DF.append(Alerts)
# #     Alert_DF = pd.DataFrame(Alert_DF)
# #     Alert_DF.sort_values('%_52WH', ascending=False, inplace=True)
# #     return Alert_DF
# #
# # def render_html(data_frame):
# #     template = env.get_template('template.html')
# #     rows = [list(zip(row, Scanner(row))) for _, row in data_frame.iterrows()]
# #     html_content = template.render(columns=data_frame.columns, data=rows)
# #     with open(output_path, 'w') as file:
# #         file.write(html_content)
# #
# # # Main loop
# # start_time = datetime.datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
# # end_time = datetime.datetime.now().replace(hour=17, minute=31, second=0, microsecond=0)
# #
# # while datetime.datetime.now() < end_time:
# #     try:
# #         data_frame = fetch_data()
# #         render_html(data_frame)
# #         clear_output(wait=True)
# #         display(HTML(f'<a href="{output_path}" target="_blank">Click here to view the updated table</a>'))
# #         time.sleep(10)
# #     except Exception as e:
# #         print(e)
#
# import pandas as pd
# import datetime
# import yfinance as yf
# import requests
# import time
# import tkinter as tk
# from tkinter import messagebox
# import pyautogui
# import webbrowser
# import os
# from jinja2 import Environment, FileSystemLoader
#
# # Path variables
# path = r"D:\screener\custom_screener_test\\"
# template_path = path + "template.html"
# output_path = path + "Stock_Screener_api.html"
#
# # Define Jinja2 environment
# env = Environment(loader=FileSystemLoader(path))
#
# # Helper functions for stock data
# def nifty_spot_data(inst_name):
#     url = "http://172.16.47.54:8006/livedataname"
#     headers = {'esegment': '1', 'inst_name': inst_name}
#     response = requests.get(url, headers=headers)
#     return response.json()
#
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
#
# def DownloadHistoricalData(df):
#     data = {}
#     for i in range(len(df)):
#         name = df['Stocks'][i]
#         start_date = (datetime.datetime.now().date() - datetime.timedelta(days=360)).strftime('%Y-%m-%d')
#         end_date = datetime.datetime.now().date().strftime('%Y-%m-%d')
#         df1 = yf.Ticker(df['Ticker'][i])
#         history = df1.history(start=start_date, end=end_date)
#         one_minute_data = yf.download(df['Ticker'][i], interval='1m', progress=False)
#         one_minute_data.index = pd.to_datetime(one_minute_data.index)
#         yesterdays_data = one_minute_data[one_minute_data.index.date == (datetime.datetime.now().date() - datetime.timedelta(days=1))]
#         yesterdays_data = yesterdays_data.resample('1D').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
#         history = pd.concat([history, yesterdays_data])
#         history = history[~history.index.duplicated(keep='last')]
#         history['Name'] = name
#         history.reset_index(inplace=True)
#         data[name] = round(history, 2)
#     return data
#
# def PerformanceScanner(data):
#     data['% Chg'] = round(data['Close'].pct_change() * 100, 2)
#     data['52WH'] = data['High'].shift(1).max()
#     data['52WL'] = data['Low'].shift(1).min()
#     data['10MV'] = round(data['Close'].rolling(10).mean(), 2)
#     data['20MV'] = round(data['Close'].rolling(20).mean(), 2)
#     data['PrevClose'] = data['Close'].shift(1)
#     data = data[['Name', 'Close', 'PrevClose', 'High', 'Low', '% Chg', '10MV', '20MV', '52WH', '52WL']]
#     data.rename(columns={'Close': 'LTP'}, inplace=True)
#     return data
#
# # Function to apply conditional formatting for the table
# def Scanner(row):
#     formatted_row = [''] * len(row)
#     current_close = row["LTP"]
#     previous_close = row["PrevClose"]
#     moving_avg_10 = row["10MV"]
#     moving_avg_20 = row["20MV"]
#     if current_close > moving_avg_20 and previous_close < moving_avg_20:
#         formatted_row[row.index.get_loc('LTP')] = 'background-color: lightgreen;'
#     if current_close < moving_avg_20 and previous_close > moving_avg_20:
#         formatted_row[row.index.get_loc('LTP')] = 'background-color: lightcoral;'
#     return formatted_row
#
# def fetch_data(path=path):
#     screener_list = pd.read_excel(path + 'All.xlsx')
#     historical_data = DownloadHistoricalData(screener_list)
#     historical_data = Downloadcurrentdata(historical_data)
#     Alert_DF = []
#     for name in historical_data:
#         DF = historical_data[name]
#         Alerts = PerformanceScanner(DF).iloc[-1]
#         Alert_DF.append(Alerts)
#     Alert_DF = pd.DataFrame(Alert_DF)
#     Alert_DF.sort_values('% Chg', ascending=False, inplace=True)
#     return Alert_DF
#
# def render_html(data_frame):
#     template = env.get_template('template.html')
#     rows = [list(zip(row, Scanner(row))) for _, row in data_frame.iterrows()]
#     html_content = template.render(columns=data_frame.columns, data=rows)
#     with open(output_path, 'w') as file:
#         file.write(html_content)
#
#     # # Open the generated HTML in the default web browser
#     # webbrowser.open('file://' + output_path)
#
#     abs_path = os.path.abspath(output_path)
#
#     # Open the file in the default web browser
#     webbrowser.open(f'file://{abs_path}')
#
# # Main loop
# start_time = datetime.datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
# end_time = datetime.datetime.now().replace(hour=17, minute=31, second=0, microsecond=0)
#
# while datetime.datetime.now() < end_time:
#     try:
#         data_frame = fetch_data()
#         render_html(data_frame)
#         time.sleep(10)
#     except Exception as e:
#         pyautogui.alert(text=f'Error: {e}', title='Error in Stock Screener')
#         print(e)

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

# global dict
glob_dict = {}

# Path variables
path = os.path.dirname(os.path.abspath(__file__))
template_path = os.path.join(path, "template.html")
output_path = os.path.join(path, "Stock_Screener_api.html")

# Define Jinja2 environment
env = Environment(loader=FileSystemLoader(path))

# Helper functions for stock data
def nifty_spot_data(inst_name):
    url = "http://172.16.47.54:8006/livedataname"
    headers = {'esegment': '1', 'inst_name': inst_name}
    response = requests.get(url, headers=headers)
    return response.json()

def Downloadcurrentdata(historical_data):
    for symbol in historical_data:
        data = historical_data[symbol]
        new_data = nifty_spot_data(symbol)
        date = new_data[5]
        open = new_data[7]
        high = new_data[8]
        low = new_data[9]
        close = new_data[2]
        data.loc[len(data)] = [date, open, high, low, close, 0, 0, 0, symbol]
        data.set_index("index", inplace=True)
        data.index = pd.to_datetime(data.index, utc=True)
        historical_data[symbol] = data
    return historical_data

def DownloadHistoricalData(df):
    data = {}
    for i in range(len(df)):
        name = df['Stocks'][i]
        ticker_name = name + '.NS'
        start_date = (datetime.datetime.now().date() - datetime.timedelta(days=360)).strftime('%Y-%m-%d')
        end_date = datetime.datetime.now().date().strftime('%Y-%m-%d')
        # ----------------------------------------------------------------
        df1 = yf.Ticker(ticker_name)
        history = df1.history(start=start_date, end=end_date)
        one_minute_data = yf.download(ticker_name, interval='1m', progress=False) # we get one minute data for the past 7 busdays
        one_minute_data.index = pd.to_datetime(one_minute_data.index)
        yesterdays_data = one_minute_data[one_minute_data.index.date == (datetime.datetime.now().date() - datetime.timedelta(days=1))]
        yesterdays_data = yesterdays_data.resample('1D').agg({'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
        history = pd.concat([history, yesterdays_data])
        history = history[~history.index.duplicated(keep='last')]
        history['Name'] = name
        history.reset_index(inplace=True)
        data[name] = round(history, 2) # makes dictionary in format Symbol(str):history(df)
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
        break

    return data


def PerformanceScanner(data):
    data['% Chg'] = round(data['Close'].pct_change() * 100, 2)
    data['52WH'] = data['High'].shift(1).max()
    data['52WL'] = data['Low'].shift(1).min()
    data['10MV'] = round(data['Close'].rolling(10).mean(), 2)
    data['20MV'] = round(data['Close'].rolling(20).mean(), 2)
    data['PrevClose'] = data['Close'].shift(1)
    data = data[['Name', 'Close', 'PrevClose', 'High', 'Low', '% Chg', '10MV', '20MV', '52WH', '52WL']]
    data.rename(columns={'Close': 'LTP'}, inplace=True)
    return data

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

def fetch_data(path=path):
    screener_list = pd.read_excel(os.path.join(path, 'All.xlsx'))
    historical_data = DownloadHistoricalData(screener_list)
    current_data = Downloadcurrentdata(historical_data)
    Alert_DF = []
    for name in current_data:
        DF = current_data[name]
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
abs_path = os.path.abspath(output_path)
webbrowser.open(f'file://{abs_path}')

while datetime.datetime.now() < end_time:
    try:
        data_frame = fetch_data()
        render_html(data_frame)
        time.sleep(10)
    except Exception as e:
        # pyautogui.alert(text=f'Error: {e}', title='Error in Stock Screener')
        print(e)

