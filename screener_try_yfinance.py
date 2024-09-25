import pandas as pd
import datetime
import yfinance as yf
import requests
import time
from IPython.display import HTML, display, clear_output

import pandas as pd
import datetime
import yfinance as yf
import requests
import time
from IPython.display import HTML, display, clear_output

# Define the base URL
url = "http://172.16.47.54:8006/livedataname"
path = 'D:/custom_screener/' #change_this


def nifty_spot_data(inst_name):
    headers = {
        'esegment': '1',
        'inst_name': inst_name
    }
    response = requests.request("GET", url, headers=headers)
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
#         historical_data[symbol] = data
#     return historical_data

def DownloadHistoricalData(df):
    data = {}
    for i in range(len(df)):
        name = df['Stocks'][i]
        end_date = datetime.datetime.now().date()
        start_date = end_date - datetime.timedelta(days=360)
        start_date = start_date.strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')
        df1 = yf.Ticker(df['Ticker'][i])
        history = df1.history(start=start_date, end=end_date)
        one_minute_data = yf.download(df['Ticker'][i], interval='1m', progress=False)
        one_minute_data.index = pd.to_datetime(one_minute_data.index)
        date_list = list(set(one_minute_data.index.date))
        one_minute_data['Date'] = one_minute_data.index.date
        date_list.sort()
        yesterdays_data = one_minute_data[one_minute_data['Date'] == date_list[-2]]
        yesterdays_data = yesterdays_data.resample('1D').agg(
            {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
        yesterdays_data['Dividends'] = 0.0
        yesterdays_data['Stock Splits'] = 0.0
        history = pd.concat([history, yesterdays_data])
        history = history[~history.index.duplicated(keep='last')]
        current_data = one_minute_data[one_minute_data['Date'] > date_list[-2]]
        current_data = current_data.resample('1D').agg(
            {'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last', 'Volume': 'sum'})
        current_data['Dividends'] = 0.0
        current_data['Stock Splits'] = 0.0
        history = pd.concat([history, current_data])
        history['Name'] = name
        data[name] = round(history, 2)
    return data


def make_clickable(url):
    return f'<a href="{url}" target="_blank">{url}</a>'


def PerformanceScanner(data):
    todays_date = datetime.datetime.now().date().strftime('%Y-%m-%d')
    previous_day = data.index[-2]
    previous_day = previous_day.strftime('%Y-%m-%d')
    name = data['Name'][0].lower()
    hyperlink = f'https://chartink.com/stocks/{name}.html'
    data['% Chg'] = round(data['Close'].pct_change() * 100, 2)
    data['52WH'] = data['High'].shift(1).max()
    data['52WL'] = data['Low'].shift(1).min()
    data['5MV'] = round(data['Close'].rolling(5).mean(), 2)
    data['10MV'] = round(data['Close'].rolling(10).mean(), 2)
    data['20MV'] = round(data['Close'].rolling(20).mean(), 2)
    data['30MV'] = round(data['Close'].rolling(30).mean(), 2)
    data['40MV'] = round(data['Close'].rolling(40).mean(), 2)
    data['50MV'] = round(data['Close'].rolling(50).mean(), 2)
    data['100MV'] = round(data['Close'].rolling(100).mean(), 2)
    data['200MV'] = round(data['Close'].rolling(200).mean(), 2)

    data['PrevHigh'] = data['High'].shift(1)
    data['PrevLow'] = data['Low'].shift(1)
    data['PrevClose'] = data['Close'].shift(1)
    data['Prev5MV'] = data['5MV'].shift(1)
    data['Prev10MV'] = data['10MV'].shift(1)
    data['Prev20MV'] = data['20MV'].shift(1)
    data['Prev30MV'] = data['30MV'].shift(1)
    data['Prev40MV'] = data['40MV'].shift(1)
    data['Prev50MV'] = data['50MV'].shift(1)
    data['Prev100MV'] = data['100MV'].shift(1)
    data['Prev200MV'] = data['200MV'].shift(1)

    data['%_H'] = round(((data['Close'] - data['High']) / data['Close']) * 100, 2)
    data['%_L'] = round(((data['Close'] - data['Low']) / data['Close']) * 100, 2)
    data['%_52WH'] = round(((data['Close'] - data['52WH']) / data['Close']) * 100, 2)
    data['%_52WL'] = round(((data['Close'] - data['52WL']) / data['Close']) * 100, 2)
    data['URL'] = hyperlink
    data['Link'] = data['URL'].apply(make_clickable)
    data = round(data, 2)

    data = data[['Name', 'Close', 'PrevClose', 'High', 'Low', '% Chg', '%_H', '%_L',
                 '10MV', '20MV', 'Prev10MV', 'Prev20MV', '52WH', '52WL',
                 '%_52WH', '%_52WL', 'Link']]
    data.rename(columns={'Close': 'LTP'}, inplace=True)
    return data


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


def fetch_data(path=path):
    screener_list = pd.read_excel(path + 'All.xlsx')
    historical_data = DownloadHistoricalData(screener_list)
    Alert_DF = []
    for name in historical_data:
        DF = historical_data[name]
        Alerts = PerformanceScanner(DF).iloc[-1]
        Alert_DF.append(Alerts)

    Alert_DF = pd.DataFrame(Alert_DF)
    Alert_DF.sort_values('%_52WH', ascending=False, inplace=True)
    Alert_DF.reset_index(drop=True, inplace=True)
    return Alert_DF


start_time = datetime.datetime.now().replace(hour=9, minute=15, second=0, microsecond=0)
end_time = datetime.datetime.now().replace(hour=17, minute=31, second=0, microsecond=0)

while datetime.datetime.now() < end_time:
    data_frame = fetch_data()
    html_str = '''
    <meta http-equiv="refresh" content="10">
    <style>
        table {
            font-family: Arial, sans-serif;
            border-collapse: collapse;
            width: 100%;
        }
        th, td {
            border: 1px solid #dddddd;
            text-align: left;
            padding: 8px;
        }
        th {
            cursor: pointer;
        }
        tr:nth-child(even) {
            background-color: #f2f2f2;
        }
        tr:hover {
            background-color: #ddd;
        }
        .filter-box {
            padding: 3px;
            margin-bottom: 5px;
            width: 100%;
            box-sizing: border-box;
        }
    </style>
    '''

    html_str += '''
    <script>
        function filterTable() {
            var input, filter, table, tr, td, i, txtValue;
            input = document.getElementById("filter");
            filter = input.value.toUpperCase();
            table = document.getElementById("myTable");
            tr = table.getElementsByTagName("tr");
            for (i = 1; i < tr.length; i++) {
                td = tr[i].getElementsByTagName("td")[0];
                if (td) {
                    txtValue = td.textContent || td.innerText;
                    if (txtValue.toUpperCase().indexOf(filter) > -1) {
                        tr[i].style.display = "";
                    } else {
                        tr[i].style.display = "none";
                    }
                }
            }
        }

        function adjustColumnWidths() {
            var table = document.getElementById("myTable");
            var rows = table.rows;
            var cellWidths = [];
            for (var i = 0; i < rows[0].cells.length; i++) {
                cellWidths.push(0);
            }
            for (var i = 0; i < rows.length; i++) {
                var cells = rows[i].getElementsByTagName("td");
                for (var j = 0; j < cells.length; j++) {
                    var cellWidth = cells[j].scrollWidth;
                    if (cellWidth > cellWidths[j]) {
                        cellWidths[j] = cellWidth;
                    }
                }
            }
            for (var i = 0; i < rows[0].cells.length; i++) {
                rows[0].cells[i].style.width = cellWidths[i] + "px";
            }
        }
    </script>
    '''

    html_str += '<div class="filter-box">'
    html_str += f'<input type="text" id="filter" onkeyup="filterTable()" placeholder="Filter Name" style="margin-right: 5px;">'
    html_str += '</div>'

    html_str += '<table id="myTable">'
    html_str += '<tr>'
    for col in data_frame.columns:
        html_str += f'<th onclick="sortTable(event, {data_frame.columns.get_loc(col)})">{col}</th>'
    html_str += '</tr>'

    for _, row in data_frame.iterrows():
        formatted_row = Scanner(row)
        html_str += '<tr>'
        for i, val in enumerate(row):
            html_str += f'<td style="{formatted_row[i]}">{val}</td>'
        html_str += '</tr>'
    html_str += '</table>'

    html_str += '''
    <script>
        function sortTable(event, n) {
          var table, rows, switching, i, x, y, xVal, yVal, shouldSwitch, dir, switchcount = 0;
          table = event.target.closest("table");
          switching = true;
          dir = "asc";
          while (switching) {
            switching = false;
            rows = table.rows;
            for (i = 1; i < (rows.length - 1); i++) {
              shouldSwitch = false;
              x = rows[i].getElementsByTagName("TD")[n];
              y = rows[i + 1].getElementsByTagName("TD")[n];
              xVal = isNaN(parseFloat(x.innerHTML)) ? x.innerHTML.toLowerCase() : +x.innerHTML;
              yVal = isNaN(parseFloat(y.innerHTML)) ? y.innerHTML.toLowerCase() : +y.innerHTML;
              if (dir == "asc") {
                if (xVal > yVal) {
                  shouldSwitch = true;
                  break;
                }
              } else if (dir == "desc") {
                if (xVal < yVal) {
                  shouldSwitch = true;
                  break;
                }
              }
            }
            if (shouldSwitch) {
              rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
              switching = true;
              switchcount++;
            } else {
              if (switchcount == 0 && dir == "asc") {
                dir = "desc";
                switching = true;
              }
            }
          }
          adjustColumnWidths();
        }

        function adjustColumnWidths() {
            var table = document.getElementById("myTable");
            var rows = table.rows;
            var cellWidths = [];
            for (var i = 0; i < rows[0].cells.length; i++) {
                cellWidths.push(0);
            }
            for (var i = 0; i < rows.length; i++) {
                var cells = rows[i].getElementsByTagName("td");
                for (var j = 0; j < cells.length; j++) {
                    var cellWidth = cells[j].scrollWidth;
                    if (cellWidth > cellWidths[j]) {
                        cellWidths[j] = cellWidth;
                    }
                }
            }
            for (var i = 0; i < rows[0].cells.length; i++) {
                rows[0].cells[i].style.width = cellWidths[i] + "px";
            }
        }
    </script>
    '''
    #     html_str = '''"""
    #             <script>
    #                 function autoReload() {
    #                 location.reload(true);
    #                 }
    #             setInterval(autoReload, 10000); // Reload every 10 seconds (10000 milliseconds)
    #             </script>
    #             """'''

    file_path = path + "Stock Screener.html" #change_this
    with open(file_path, "w") as file:
        file.write(html_str)

    clear_output(wait=True)
    display(HTML(f'<a href="{file_path}" target="_blank">Click here to view the updated table</a>'))
    time.sleep(10)
