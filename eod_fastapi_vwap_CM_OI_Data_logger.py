import traceback
from fastapi.responses import JSONResponse
import requests
from fastapi import FastAPI, Response
import pandas as pd
from starlette.datastructures import MutableHeaders
import logging
from pydantic import BaseModel
import redis
import gzip
import json
import io
###########################################################################
import requests
import os
from datetime import date, datetime, time
from sqlalchemy import create_engine, text
from fastapi.responses import StreamingResponse
from starlette.requests import Request


# log_file_name = f"EOD_log_file_{datetime.now().strftime('%Y-%m-%d')}.log"
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=log_file_name)


# Define the directory for logs
log_dir = "logger"

# Create the directory if it doesn't exist
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Create the log file name with today's date
log_file_name = f"EOD_log_file_{datetime.now().strftime('%Y-%m-%d')}.log"

# Full path to the log file inside the logger folder
log_file_path = os.path.join(log_dir, log_file_name)

# Configure logging to store logs in the file
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_file_path
)

# data_center_api = 0
data_center_api = 1

###########################################################################
db_url = 'postgresql://postgres:Vivek001@172.16.47.217:5430/algo_backend'
# db_url = 'postgresql://postgres:E6ymrG80or51s7y@192.168.50.82:5432/algo_backend'
host = "http://192.168.50.81:3000"

data_api_key = '6119cc0726e5f93d37b435'
data_api_secret = 'Kxuc232$MU'
source = "WEBAPI"

# Connect to Redis server
redis_host = '172.16.47.54'
redis_port = 6381
redis_password = None
db_index = 0
redis_client = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, db=db_index)
inst_to_scrip_map = json.loads(redis_client.get("inst_to_scrip"))

exchangeInstrumentID = list(inst_to_scrip_map.values())

app = FastAPI(title="fastapi host")
###################### ag
# set_marketDataToken = "aabbccddaagg"


logging.info("Logger initialized and log is being saved to the logger folder.")

############################################################################################# api oi cmt
if not data_center_api:

    print("COLO:")
    # engine = create_engine(db_url)
    # table_name = 'vendortoken'  # Replace with the name of your table
    # # Construct the SQL query to select all rows from the table
    # query = f"SELECT * FROM {table_name};"
    # # Execute the query and fetch the results
    # with engine.connect() as conn:
    #     result = conn.execute(text(query))
    #     # Fetch all rows from the result set
    #     rows = result.fetchall()
    #
    # # Process the fetched rows (e.g., print the data)
    # for row in rows:
    #     set_marketDataToken = row[8]
    #     set_muserID = row[2]
    #
    #     print(row)
    #
    #     break

    xts_payload = {"appKey": data_api_key, "secretKey": data_api_secret, "source": source}

    xts_url = f"{host}/apimarketdata/auth/login"
    xts_response = requests.post(url=xts_url, json=xts_payload)
    xts_data = xts_response.json()

    if 'result' in xts_data and 'token' in xts_data['result']:
        set_marketDataToken = xts_data['result']['token']
        print("Market Data Token:", set_marketDataToken)

    if "result" in xts_data and "userID" in xts_data['result']:
        set_muserID = xts_data['result']['userID']
        print(f"{set_muserID} Login:", xts_data["type"])
else:
    print("Data center:")
    # data center api key
    host = "https://algozy.rathi.com:3000"
    API_KEY = '186da63fba34a173c0e489'
    API_SECRET = 'Ybgu222$@d'

    xts_payload = {"appKey": API_KEY, "secretKey": API_SECRET,
                   "source": "WEBAPI"}

    xts_url = f"https://algozy.rathi.com:3000/apimarketdata/auth/login"

    xts_response = requests.post(url=xts_url, json=xts_payload)

    xts_data = xts_response.json()

    if 'result' in xts_data and 'token' in xts_data['result']:
        # XTS login successful, proceed with local login
        set_marketDataToken = xts_data['result']['token']
        print("Market Data Token: ", set_marketDataToken)

    if "result" in xts_data and "userID" in xts_data['result']:
        set_muserID = xts_data['result']['userID']

if not set_marketDataToken and set_muserID:
    logging.error(f"Exception occurred for {set_marketDataToken} and {set_muserID}")
    raise Exception("error")
############################################################################################# api oi end
########################################################################################################################
# # Get today's date
today = datetime.today().strftime('%Y-%m-%d')


@app.get("/download_master")
def download_master(download: bool = True, compressed: bool = False):
    exchangesegments = ["NSECM", "NSEFO"]
    url = f'{host}/apimarketdata/instruments/master'
    payload = {
        "exchangeSegmentList": exchangesegments
    }
    headers = {
        "Content-Type": "application/json",
        "authorization": set_marketDataToken
    }
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        if download:
            data_string = response.json()['result']
            rows = data_string.split('\n')
            data = [row.split('|') for row in rows]
            data = [[row[0], row[1], row[3], row[5], row[16], row[17], row[15]] for row in data]
            df = pd.DataFrame(data, columns=['ExchangeSegment', 'ExchangeInstrumentID', 'Name', "Series",
                                             "ContractExpiration",
                                             "StrikePrice", "FullName"])

            if not compressed:
                return df.to_dict(orient='records')
            else:
                json_string = json.dumps(df.to_dict(orient='records'))

                compressed_data = gzip.compress(json_string.encode("utf-8"))
                return StreamingResponse(io.BytesIO(compressed_data), media_type="application/octet-stream",
                                         headers={"Content-Disposition": "attachment; filename=data.json.gz"})

        else:
            return response

    else:
        logging.error(f"Error in fetching instrument master. Status code: {response.status_code}")
        print(f"Error in fetching instrument master. Status code: {response.status_code}")
        return None


@app.get("/download_full_fo")
def download_master_full(download: bool = True, compressed: bool = False):
    exchangesegments = ["NSECM", "NSEFO"]
    url = f'{host}/apimarketdata/instruments/master'
    payload = {
        "exchangeSegmentList": exchangesegments
    }
    headers = {
        "Content-Type": "application/json",
        "authorization": set_marketDataToken
    }
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        if download:
            data_string = response.json()['result']
            rows = data_string.split('\n')
            data = [row.split('|') for row in rows]
            # data = [[row[0], row[1], row[3], row[5], row[16], row[17], row[15]] for row in data]
            # data = [[row[0], row[1], row[3], row[5], row[16], row[17], row[15]] for row in data]
            columns = ['ExchangeSegment', 'ExchangeInstrumentID', 'InstrumentType', 'Name', 'Description', 'Series',
                       'NameWithSeries', 'InstrumentID', 'PriceBand.High', 'PriceBand.Low', 'FreezeQty', 'TickSize',
                       'LotSize', 'Multiplier', 'UnderlyingInstrumentId', 'UnderlyingIndexName', 'ContractExpiration',
                       'StrikePrice', 'OptionType', 'DisplayName', 'PriceNumerator', 'pricedenominotor', 'Description']

            # data = [[row[0], row[1], row[3], row[5], row[16], row[17], row[15]] for row in data]
            #
            # cols = ["ExchangeSegment", "ExchangeInstrumentID", "Name", "Series", "FullName","Description",
            #         "ContractExpiration", "StrikePrice"]
            # df = pd.DataFrame(data, columns=['ExchangeSegment', 'ExchangeInstrumentID', 'Name', "Series",
            #                                  "ContractExpiration",
            #                                  "StrikePrice", "FullName"])

            df = pd.DataFrame(data, columns=columns)

            ##########################
            fo_df = df[df['ExchangeSegment'] == 'NSEFO']

            fo_df.columns = columns

            fo_df_type1 = fo_df[fo_df['InstrumentType'].isin([1, '1'])]
            fo_df_type1 = fo_df_type1[['ExchangeInstrumentID', 'InstrumentType', 'Name', 'StrikePrice', 'LotSize']]
            fo_df_type1 = fo_df_type1[~(fo_df_type1['Name'].str.endswith("NSETEST"))]
            fo_df_type1[['symbol', 'expiry']] = fo_df_type1['StrikePrice'].str.split(pat=" ", n=2, expand=True)
            fo_df_type1['expiry'] = pd.to_datetime(fo_df_type1['expiry']).dt.strftime("%d%b%Y")
            fo_df_type1['expiry'] = fo_df_type1['expiry'].str.upper()

            fo_df_type1['instname'] = fo_df_type1['symbol'] + "_" + fo_df_type1['expiry'] + "_XX_0"
            fo_df_type1['opt_type'] = "XX"
            fo_df_type1['strike'] = "0"
            fo_df_type1.rename(
                columns={"ExchangeInstrumentID": "scripcode", 'InstrumentType': 'exchange', "LotSize": "lotsize"},
                inplace=True)
            fo_df_type1.drop(columns=['Name', 'StrikePrice'], inplace=True)

            fo_df_type2 = fo_df[fo_df['InstrumentType'].isin([2, '2'])]
            fo_df_type2 = fo_df_type2[['ExchangeInstrumentID', 'InstrumentType', 'Name', 'DisplayName', 'LotSize']]

            fo_df_type2 = fo_df_type2[~(fo_df_type2['Name'].str.endswith("NSETEST"))]
            fo_df_type2[['symbol', 'expiry', 'opt_type', 'strike']] = fo_df_type2['DisplayName'].str.split(pat=" ", n=3,
                                                                                                           expand=True)
            fo_df_type2['expiry'] = pd.to_datetime(fo_df_type2['expiry']).dt.strftime("%d%b%Y")
            fo_df_type2['expiry'] = fo_df_type2['expiry'].str.upper()

            fo_df_type2['instname'] = fo_df_type2['symbol'] + "_" + fo_df_type2['expiry'] + "_" + fo_df_type2[
                'opt_type'] + "_" + fo_df_type2['strike']

            fo_df_type2.rename(
                columns={"ExchangeInstrumentID": "scripcode", 'InstrumentType': 'exchange', "LotSize": "lotsize"},
                inplace=True)
            fo_df_type2.drop(columns=['Name', 'DisplayName'], inplace=True)

            # If required add in the final df
            fo_df_type4 = fo_df[fo_df['InstrumentType'].isin([4, '4'])]

            fo_df = pd.concat([fo_df_type1, fo_df_type2], axis=0)
            fo_df.drop_duplicates().reset_index(inplace=True)
            fo_df['scripcode'] = fo_df['scripcode'].astype(float).astype(int)
            fo_df['created_at'] = datetime.now()

            # ['ExchangeSegment', 'ExchangeInstrumentID', 'InstrumentType', 'Name', 'Description', 'Series',
            #  'NameWithSeries', 'InstrumentID', 'PriceBand.High', 'PriceBand.Low', 'FreezeQty', 'TickSize', 'LotSize',
            #  'Multiplier', 'UnderlyingInstrumentId', 'UnderlyingIndexName', 'ContractExpiration', 'StrikePrice',
            #  'OptionType', 'DisplayName', 'PriceNumerator', 'pricedenominotor', 'Description']

            ##########################
            # df = df[df['ExchangeSegment'] == 'NSEFO']

            # df = pd.DataFrame(data, columns=cols)
            # df = pd.DataFrame(data, columns=cols)
            df = fo_df[['scripcode', 'instname']]
            # df = df
            print(df.head(5))
            # ['ExchangeSegment', 'ExchangeInstrumentID', 'InstrumentType', 'Name', 'Description', 'Series',
            #  'NameWithSeries', 'InstrumentID', 'PriceBand.High', 'PriceBand.Low', 'FreezeQty', 'TickSize',
            #  'LotSize', 'Multiplier', 'UnderlyingInstrumentId', 'UnderlyingIndexName', 'ContractExpiration',
            #  'StrikePrice', 'OptionType', 'DisplayName', 'PriceNumerator', 'pricedenominotor', 'Description']

            if not compressed:
                return df.to_dict(orient='records')
            else:
                json_string = json.dumps(df.to_dict(orient='records'))

                compressed_data = gzip.compress(json_string.encode("utf-8"))
                return StreamingResponse(io.BytesIO(compressed_data), media_type="application/octet-stream",
                                         headers={"Content-Disposition": "attachment; filename=data.json.gz"})

        else:
            return response

    else:
        logging.error(f"Error in fetching instrument master. Status code: {response.status_code}")
        print(f"Error in fetching instrument master. Status code: {response.status_code}")
        return None


def download_master_for_memory(download: bool = True, compressed: bool = False):
    exchangesegments = ["NSECM", "NSEFO"]
    url = f'{host}/apimarketdata/instruments/master'
    payload = {
        "exchangeSegmentList": exchangesegments
    }
    headers = {
        "Content-Type": "application/json",
        "authorization": set_marketDataToken
    }
    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 200:
        if download:
            data_string = response.json()['result']
            rows = data_string.split('\n')
            data = [row.split('|') for row in rows]
            data = [[row[0], row[1], row[2], row[3], row[5], row[16], row[15], row[17], row[19]] for row in data]
            df = pd.DataFrame(data, columns=['ExchangeSegment', 'ExchangeInstrumentID', 'itype', 'Name', "Series",
                                             "ContractExpiration",
                                             "StrikePrice", "FullName", 'InstName'])

            if not compressed:
                return df.to_dict(orient='records')
            else:
                json_string = json.dumps(df.to_dict(orient='records'))

                compressed_data = gzip.compress(json_string.encode("utf-8"))
                return StreamingResponse(io.BytesIO(compressed_data), media_type="application/octet-stream",
                                         headers={"Content-Disposition": "attachment; filename=data.json.gz"})

        else:
            return response

    else:

        logging.error(f"Error in fetching instrument master. Status code: {response.status_code}")
        print(f"Error in fetching instrument master. Status code: {response.status_code}")

        return None


file_name = f"data_{today}.csv"
if os.path.exists(file_name):
    print(f"File {file_name} already exists. No need to create it for today.")
else:
    response = download_master_for_memory(download=False)
    # print(response.keys)
    data_string = response.json()['result']
    rows = data_string.split('\n')
    data = [row.split('|') for row in rows]
    data = [[row[0], row[1], row[2], row[3], row[5], row[16], row[15], row[17], row[19]] for row in data]
    df = pd.DataFrame(data, columns=['ExchangeSegment', 'ExchangeInstrumentID', 'itype', 'Name', "Series",
                                     "ContractExpiration",
                                     "StrikePrice", "FullName", 'InstName'])

    df.to_csv(file_name, index=False)
    print(f"DataFrame saved as {file_name}")


########################################################################################################################

def MasterData_mapping():
    file_name = f"data_{today}.csv"
    if os.path.exists(file_name):
        print(f"File {file_name} already exists. No need to create it for today.")
        df = pd.read_csv(file_name)
    else:
        res = download_master_for_memory()
        df = pd.DataFrame(res)

    optdf = df[df['Series'].isin(['OPTSTK', 'OPTIDX'])]
    optdf[['symbol', 'expiry', 'opt_type', 'strike']] = optdf['InstName'].str.split(pat=" ", n=3, expand=True)

    optdf['expiry'] = pd.to_datetime(optdf['expiry']).dt.strftime("%d%b%Y")
    optdf['expiry'] = optdf['expiry'].str.upper()

    optdf['opt_type'] = optdf['opt_type'].fillna("XX")
    optdf['strike'] = optdf['strike'].fillna("0")

    optdf['inst_name'] = optdf['symbol'] + "_" + optdf['expiry'] + "_" + optdf['opt_type'] + "_" + \
                         optdf['strike']

    futdf = df[df['itype'].isin([1, '1'])]

    futdf[['symbol', 'expiry']] = futdf['FullName'].str.split(pat=" ", n=2, expand=True)
    futdf['expiry'] = pd.to_datetime(futdf['expiry']).dt.strftime("%d%b%Y")
    futdf['expiry'] = futdf['expiry'].str.upper()

    futdf['inst_name'] = futdf['symbol'] + "_" + futdf['expiry']
    scrip_to_futinst_dict = futdf[['ExchangeInstrumentID', 'inst_name']].set_index("inst_name").to_dict()[
        'ExchangeInstrumentID']

    futidxdf = df[df['Series'].isin(['FUTIDX'])]
    futidxdf['expiry'] = pd.to_datetime(futidxdf['ContractExpiration']).dt.strftime("%d%b%Y")
    futidxdf['expiry'] = futidxdf['expiry'].str.upper()
    futidxdf['inst_name'] = futidxdf['Name'] + "_" + futidxdf['expiry']
    scrip_to_futidxinst_dict = futidxdf[['ExchangeInstrumentID', 'inst_name']].set_index("inst_name").to_dict()[
        'ExchangeInstrumentID']

    scrip_to_inst_dict = optdf[['ExchangeInstrumentID', 'inst_name']].set_index("inst_name").to_dict()[
        'ExchangeInstrumentID']
    eq_scrip_mapping = df[df['Series'].isin(['EQ'])][['Name', 'ExchangeInstrumentID']].set_index('Name').to_dict()[
        'ExchangeInstrumentID']

    scrip_to_inst_dict.update(scrip_to_futidxinst_dict)
    scrip_to_inst_dict.update(scrip_to_futinst_dict)

    # eq_scrip_mapping['NIFTY'] = 26000
    # eq_scrip_mapping['BANKNIFTY'] = 26001
    # eq_scrip_mapping['FINNIFTY'] = 26017

    # {"NIFTY 50": "NIFTY", "NIFTY BANK": "BANKNIFTY", "NIFTY FIN SERVICE": "FINNIFTY",
    #        "NIFTY MID SELECT": "MIDCPNIFTY",
    #        "NIFTY NEXT 50": "NIFTYNXT50"
    #        }
    additional_entries_eq_scrip_mapping = {'NIFTY': 26000, 'BANKNIFTY': 26001, 'INDIA VIX': 26002, 'NIFTY IT': 26003,
                                           'NIFTY 100': 26004,
                                           'NIFTY MIDCAP 50': 26005, 'NIFTY GS 11 15YR': 26006, 'NIFTY INFRA': 26007,
                                           'NIFTY100 LIQ 15': 26008,
                                           'NIFTY REALTY': 26009, 'NIFTY CPSE': 26010, 'NIFTY GS COMPSITE': 26011,
                                           'NIFTY OIL AND GAS': 26012,
                                           'NIFTY50 TR 1X INV': 26013, 'NIFTY PHARMA': 26014, 'NIFTY PSE': 26015,
                                           'NIFTY MIDCAP 150': 26016,
                                           'NIFTY MIDCAP 100': 26017, 'NIFTY SERV SECTOR': 26018, 'NIFTY 500': 26019,
                                           'NIFTY ALPHA 50': 26020,
                                           'NIFTY50 VALUE 20': 26021, 'NIFTY200 QUALTY30': 26022,
                                           'NIFTY SMLCAP 250': 26023, 'NIFTY GROWSECT 15': 26024,
                                           'NIFTY50 PR 1X INV': 26025, 'NIFTY50 EQL WGT': 26026,
                                           'NIFTY PSU BANK': 26027, 'NIFTY SMLCAP 100': 26028,
                                           'NIFTY LARGEMID250': 26029, 'NIFTY100 EQL WGT': 26030,
                                           'NIFTY SMLCAP 50': 26031, 'NIFTY ENERGY': 26032,
                                           'NIFTY GS 10YR': 26033, 'FINNIFTY': 26034, 'NIFTY MIDSML 400': 26035,
                                           'NIFTY METAL': 26036,
                                           'NIFTY CONSR DURBL': 26037, 'NIFTY DIV OPPS 50': 26038,
                                           'NIFTY GS 15YRPLUS': 26039,
                                           'NIFTY MEDIA': 26040, 'NIFTY FMCG': 26041, 'NIFTY PVT BANK': 26042,
                                           'NIFTY200MOMENTM30': 26043,
                                           'HANGSENG BEES-NAV': 26044, 'NIFTY100 LOWVOL30': 26045,
                                           'NIFTY50 TR 2X LEV': 26046,
                                           'NIFTY CONSUMPTION': 26047, 'NIFTY GS 8 13YR': 26048,
                                           'NIFTY100ESGSECLDR': 26049,
                                           'NIFTY GS 10YR CLN': 26050, 'NIFTY GS 4 8YR': 26051, 'NIFTY AUTO': 26052,
                                           'NIFTY COMMODITIES': 26053,
                                           'NIFTYNXT50': 26054, 'NIFTY MNC': 26055, 'NIFTY MID LIQ 15': 26056,
                                           'NIFTY HEALTHCARE': 26057,
                                           'NIFTY500 MULTICAP': 26058, 'NIFTY ALPHALOWVOL': 26059,
                                           'NIFTY FINSRV25 50': 26060, 'NIFTY50 PR 2X LEV': 26061,
                                           'NIFTY100 QUALTY30': 26062, 'NIFTY50 DIV POINT': 26063, 'NIFTY 200': 26064,
                                           'MIDCPNIFTY': 26121,
                                           'NIFTY MIDSML HLTH': 26122, 'NIFTY MULTI INFRA': 26123,
                                           'NIFTY MULTI MFG': 26124, 'NIFTY TATA 25 CAP': 26125}

    eq_scrip_mapping.update(additional_entries_eq_scrip_mapping)

    del df, optdf, scrip_to_futidxinst_dict, scrip_to_futinst_dict

    return scrip_to_inst_dict, eq_scrip_mapping


scrip_to_inst_dict, eq_scrip_mapping = MasterData_mapping()


class DataPayload(BaseModel):
    exchange_segment: str
    exchange_instrument_id: int
    compression_value: int
    start_date: str
    end_date: str
    start_time: str
    end_time: str


class DPRPayload(BaseModel):
    exchange_segment: str
    exchange_instrument_id: int


class livedataPayloadName(BaseModel):
    esegment: int = 1
    spot_flag: bool = True
    inst_name: str = 'NIFTY'


class livedataPayload(BaseModel):
    esegment: int = 1
    scripcode: int = 26000


@app.post("/DPR")
def dpr_claculation(data: DPRPayload):
    try:
        q_response = requests.post(url=f'{host}/apimarketdata/instruments/quotes',
                                   headers={'authorization': set_marketDataToken},
                                   json={
                                       'instruments': [
                                           {'exchangeSegment': data.exchange_segment,
                                            'exchangeInstrumentID': data.exchange_instrument_id}],
                                       'xtsMessageCode': 1501, "publishFormat": "JSON"
                                   })

        if q_response.status_code == 200:
            q_response = q_response.json()

            q_response = q_response.get("result").get("listQuotes")
            df_dpr = pd.DataFrame([json.loads(q_response[0])])
            return df_dpr.to_json(orient='records')
        return None
    except Exception as E:
        # traceback.print_stack()
        logging.error(E)
        print(E)
        return None


@app.post("/livedatacode")
def livedata(data: livedataPayload):
    try:
        q_response = requests.post(url=f'{host}/apimarketdata/instruments/quotes',
                                   headers={'authorization': set_marketDataToken},
                                   json={
                                       'instruments': [
                                           {'exchangeSegment': data.esegment,
                                            'exchangeInstrumentID': data.scripcode}],
                                       'xtsMessageCode': 1501, "publishFormat": "JSON"
                                   })

        if q_response.status_code == 200:
            try:
                q_response = q_response.json()
                ltp = json.loads(q_response.get("result").get("listQuotes")[0]).get('LastTradedPrice')
            except Exception as E:
                logging.error(E)
                ltp = 0
            return [data.scripcode, ltp]
        return [data.scripcode, -1]
    except Exception as E:
        # traceback.print_stack()
        logging.error(E)
        print(E)
        return None


# @app.post("/livedataname")
# def livedata(data: livedataPayloadName):
#     try:
#
#         if data.spot_flag:
#             scripcode = eq_scrip_mapping.get(data.inst_name, 0)
#         else:
#             scripcode = scrip_to_inst_dict.get(data.inst_name, 0)
#
#         if scripcode == '':
#             return {'msg': "Enter correct instName"}
#
#         q_response = requests.post(url=f'{host}/apimarketdata/instruments/quotes',
#                                    headers={'authorization': set_marketDataToken},
#                                    json={
#                                        'instruments': [
#                                            {'exchangeSegment': data.esegment,
#                                             'exchangeInstrumentID': scripcode}],
#                                        'xtsMessageCode': 1501, "publishFormat": "JSON"
#                                    })
#
#         if q_response.status_code == 200:
#             try:
#                 q_response = q_response.json()
#                 ltp = json.loads(q_response.get("result").get("listQuotes")[0]).get('LastTradedPrice')
#             except:
#                 ltp = 0
#             return [data.inst_name, scripcode, ltp]
#         return [data.inst_name, scripcode, -1]
#     except Exception as E:
#         traceback.print_stack()
#         print(E)
#         return None


#######################################
def calculate_vwap(exchange_segment, one_token):
    # Get the current time
    current_time = datetime.now().time()

    # Define 3:30 PM time
    cutoff_time = time(15, 30)
    # cutoff_time = time(10, 30)

    # Check if current time is before 3:30 PM
    if current_time < cutoff_time:
        return None
    start_date = None
    end_date = None
    compression_value = 60
    # Create the payload dictionary
    payload = {
        "exchange_segment": exchange_segment,
        "exchange_instrument_id": one_token,
        "compression_value": compression_value,
        "start_date": pd.to_datetime('today').strftime("%b %d %Y") if start_date is None else pd.to_datetime(
            start_date).strftime("%b %d %Y"),
        "end_date": pd.to_datetime('today').strftime("%b %d %Y") if end_date is None else pd.to_datetime(
            end_date).strftime("%b %d %Y"),
        "start_time": "150000",
        # "start_time": "090000",
        "end_time": "153000",
    }

    # Create a DataPayload instance
    data_payload = DataPayload(**payload)

    # Call the make_csv_from_tbt_data function
    res_df = make_csv_from_tbt_data(data_payload)

    if "message" not in res_df:
        # df = pd.DataFrame(data_dict)
        # df = response_df
        res_df.sort_values(by='Datetime', inplace=True)

        column_mapping = {"Datetime": "datetime", "Open": "open", "High": "high", "Low": "low", "Close": "close",
                          "Volume": "volume", "OI": "oi", "dt_time": "dttime",

                          }

        res_df.rename(columns=column_mapping, inplace=True)

        res_df['open'] = res_df['open'].astype(float)
        res_df['high'] = res_df['high'].astype(float)
        res_df['low'] = res_df['low'].astype(float)
        res_df['close'] = res_df['close'].astype(float)
        res_df['volume'] = res_df['volume'].astype(int)
        res_df['dttime'] = pd.to_datetime(res_df['dttime'])

        res_df['date'] = res_df['dttime'].dt.strftime('%d/%m/%Y')
        res_df['time'] = res_df['dttime'].dt.strftime('%H:%M:%S')
        # print(df)

        res_df['time'] = pd.to_datetime(res_df['time'], format='%H:%M:%S').dt.time

        start_time = pd.to_datetime('15:00:59', format='%H:%M:%S').time()  # todo:change it
        # start_time = pd.to_datetime('09:00:59', format='%H:%M:%S').time()  # todo:change it
        end_time = pd.to_datetime('15:29:59', format='%H:%M:%S').time()

        filtered_df = res_df[(res_df['time'] >= start_time) & (res_df['time'] <= end_time)]

        filtered_df['cls_vol'] = filtered_df['close'] * filtered_df['volume']

        def calculate_res_final_value(df):
            sum_cls_vol = df['cls_vol'].sum()
            sum_volume = df['volume'].sum()
            return sum_cls_vol / sum_volume if sum_volume != 0 else 0

        res_final_value = calculate_res_final_value(filtered_df)

        # print(res_final_value)
        # print(res_final_value.round(2))
        # return res_final_value.round(2)
        return round(float(res_final_value), 2)
    else:
        return None


def get_oi_data(exchange_segment, one_token):
    # {'MessageCode': 1501, 'ExchangeSegment': 1, 'ExchangeInstrumentID': 11262, 'LastTradedPrice': 472.7,
    #   'LastTradedQunatity': 200, 'TotalBuyQuantity': 0, 'TotalSellQuantity': 2702,
    #   'TotalTradedQuantity': 3892841, 'AverageTradedPrice': 469.44, 'LastTradedTime': 1401378815,
    #   'LastUpdateTime': 1401379173, 'PercentChange': 0, 'Open': 461.4, 'High': 477, 'Low': 458.45,
    #   'Close': 472.7, 'TotalValueTraded': None,
    #   'AskInfo': {'Size': 2702, 'Price': 472.7, 'TotalOrders': 12, 'BuyBackMarketMaker': 0},
    #   'BidInfo': {'Size': 0, 'Price': 0, 'TotalOrders': 0, 'BuyBackMarketMaker': 0}, 'XMarketType': 1,
    #   'BookType': 1}
    # print("OIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII")
    q_response_oi = requests.post(
        url=f"{host}/apimarketdata/instruments/quotes",
        headers={"authorization": set_marketDataToken},
        json={
            "instruments": [
                {
                    "exchangeSegment": exchange_segment,
                    "exchangeInstrumentID": one_token,
                }
            ],
            "xtsMessageCode": 1510,
            "publishFormat": "JSON",
        },
    )
    if q_response_oi.status_code == 200:
        try:
            q_response = q_response_oi.json()
            print(q_response)
            q_response = json.loads(q_response.get("result").get("listQuotes")[0])
            open_interest = q_response.get("OpenInterest")
            # open = q_response.get('Open')
            # high = q_response.get('High')
            # low = q_response.get('Low')
            # close = q_response.get('Close')
            # cum_volume = q_response.get('TotalTradedQuantity')
        except Exception as e:
            logging.error(e)
            # print("EXCEPTION IS OCCURED_here livedata_vwap", e)
            # ltp = -1
            open_interest = -1
    else:
        # ltp = -1
        open_interest = -1
    return open_interest

@app.get("/livedataname")
def livedata_vwap(request: Request):
    try:
        data = request.headers

        if data.get('esegment') == '1':
            scripcode = eq_scrip_mapping.get(data.get("inst_name"), 0)
        elif data.get('esegment') == '2':
            scripcode = scrip_to_inst_dict.get(data.get("inst_name"), 0)
        else:
            return JSONResponse(content={"msg": "Enter correct esegment"})

        # print("OI ======>",data.get('oi'))
        # print("OI Type======>",type(data.get('oi')))

        if data.get('oi','false').lower() == "true" and data.get('esegment') =='2':
            open_interest_res =  get_oi_data(data.get('esegment'),scripcode)
            # open_interest_res =  get_oi_data("2",scripcode)
        else:
            open_interest_res = -1
            print("No OI nedded. for ",data.get('inst_name'))

        q_response = requests.post(
            url=f"{host}/apimarketdata/instruments/quotes",
            headers={"authorization": set_marketDataToken},
            json={
                "instruments": [
                    {
                        "exchangeSegment": data.get('esegment'),
                        "exchangeInstrumentID": scripcode,
                    }
                ],
                "xtsMessageCode": 1501,
                "publishFormat": "JSON",
            },
        )

        if q_response.status_code == 200:
            try:
                q_response = q_response.json()
                print(q_response)
                q_response = json.loads(q_response.get("result").get("listQuotes")[0])
                ltp = q_response.get("LastTradedPrice")
                ask_price = q_response.get("AskInfo").get('Price')
                bid_price = q_response.get("BidInfo").get('Price')

                ltt = str(datetime.fromtimestamp(q_response.get("LastTradedTime") + 315513000))

                res = calculate_vwap(data.get('esegment'), scripcode)
                # Assigning res to vwap if res is not None, otherwise assigning 0
                vwap = res if res is not None else 0

                # datat_to_sent = {'MessageCode': 1501, 'ExchangeSegment': 1, 'ExchangeInstrumentID': 11262, 'LastTradedPrice': 472.7,
                #  'LastTradedQunatity': 200, 'TotalBuyQuantity': 0, 'TotalSellQuantity': 2702,
                #  'TotalTradedQuantity': 3892841, 'AverageTradedPrice': 469.44, 'LastTradedTime': 1401378815,
                #  'LastUpdateTime': 1401379173, 'PercentChange': 0, 'Open': 461.4, 'High': 477, 'Low': 458.45,
                #  'Close': 472.7, 'TotalValueTraded': None,
                #  'AskInfo': {'Size': 2702, 'Price': 472.7, 'TotalOrders': 12, 'BuyBackMarketMaker': 0},
                #  'BidInfo': {'Size': 0, 'Price': 0, 'TotalOrders': 0, 'BuyBackMarketMaker': 0}, 'XMarketType': 1,
                #  'BookType': 1}
                open = q_response.get('Open')
                high = q_response.get('High')
                low = q_response.get('Low')
                close = q_response.get('Close')
                cum_volume = q_response.get('TotalTradedQuantity')
                open_interest = open_interest_res
            except Exception as e:
                print("EXCEPTION IS OCCURED_here livedata_vwap", e)
                logging.error(f"EXCEPTION IS OCCURED_here livedata_vwap {e}")
                ltp = -1
                ask_price = -1
                bid_price = -1
                ltt = ''
                vwap = -1
                open = -1
                high = -1
                low = -1
                close = -1
                cum_volume = -100
                open_interest = open_interest_res

        else:
            ltp = -1
            ask_price = -1
            bid_price = -1
            ltt = ''
            vwap = -1
            open = -1
            high = -1
            low = -1
            close = -1
            cum_volume = -100
            open_interest = open_interest_res

        return JSONResponse(
            content=[data["inst_name"], scripcode, ltp, ask_price, bid_price, ltt, vwap, open, high, low, close,
                     cum_volume,open_interest])

        # return JSONResponse(
        #     content=[data["inst_name"], scripcode, ltp, ask_price, bid_price, ltt, vwap, open, high, low, close])


    except Exception as E:
        # print(traceback.format_exc())
        print(E)
        logging.error(E)
        return JSONResponse(content={"error": "An error occurred"})


##############################


@app.get("/livedataname_old")
def livedata(request: Request):
    try:
        data = request.headers

        if data.get('esegment') == '1':
            scripcode = eq_scrip_mapping.get(data.get("inst_name"), 0)
        elif data.get('esegment') == '2':
            scripcode = scrip_to_inst_dict.get(data.get("inst_name"), 0)
        else:
            return JSONResponse(content={"msg": "Enter correct esegment"})

        q_response = requests.post(
            url=f"{host}/apimarketdata/instruments/quotes",
            headers={"authorization": set_marketDataToken},
            json={
                "instruments": [
                    {
                        "exchangeSegment": data.get('esegment'),
                        "exchangeInstrumentID": scripcode,
                    }
                ],
                "xtsMessageCode": 1501,
                "publishFormat": "JSON",
            },
        )

        if q_response.status_code == 200:
            try:
                q_response = q_response.json()
                q_response = json.loads(q_response.get("result").get("listQuotes")[0])
                ltp = q_response.get("LastTradedPrice")
                ask_price = q_response.get("AskInfo").get('Price')
                bid_price = q_response.get("BidInfo").get('Price')

                ltt = str(datetime.fromtimestamp(q_response.get("LastTradedTime") + 315513000))

                res = calculate_vwap(data.get('esegment'), scripcode)
                # Assigning res to vwap if res is not None, otherwise assigning 0
                vwap = res if res is not None else 0
            except Exception as e:
                logging.error(e)
                ltp = -1
                ask_price = -1
                bid_price = -1

                ltt = ''
                vwap = -1

        else:
            ltp = -1
            ask_price = -1
            bid_price = -1
            ltt = ''
            vwap = -1

        # return JSONResponse(content=[data["inst_name"], scripcode, ltp, ask_price, bid_price, ltt])
        return JSONResponse(content=[data["inst_name"], scripcode, ltp, ask_price, bid_price, ltt, vwap])
        # content = {'info': [data["inst_name"], scripcode, ltp]}
        # new_headers = request.headers.mutablecopy()
        # new_headers['info'] = json.dumps([data["inst_name"], scripcode, ltp])
        #
        # request._headers = new_headers  # solves first point
        # request.scope.update(headers=request.headers.raw)  # solves second point
        # return None
        # response = Response(content=content)
        #
        # # Setting custom header
        # response.headers["Content-Type"] = "application/json"

        # return None

        # request.scope['context'] = json.dumps([data["inst_name"], scripcode, ltp])
        # return JSONResponse(content=[data["inst_name"], scripcode, ltp])

    except Exception as E:
        # traceback.format_exc()
        print(E)
        logging.error(E)
        return JSONResponse(content={"error": "An error occurred"})


@app.post("/make_csv_from_tbt_data")
def make_csv_from_tbt_data(data: DataPayload):
    try:

        params = {
            'exchangeSegment': data.exchange_segment,
            'exchangeInstrumentID': data.exchange_instrument_id,
            'startTime': pd.to_datetime(data.start_date).strftime("%b %d %Y") + " " + data.start_time,
            'endTime': pd.to_datetime(data.end_date).strftime("%b %d %Y") + " " + data.end_time,
            'compressionValue': data.compression_value
        }
        response = requests.get(url=f'{host}/apimarketdata/instruments/ohlc',
                                headers={'authorization': set_marketDataToken},
                                params=params
                                )

        if response.status_code != 200:
            return response.status_code
        res = response.json()

        # print(res)
        if res["result"]['dataReponse'] != "":

            final_response = pd.DataFrame(res["result"]['dataReponse'].split(","))
            final_response = final_response[0].str.split("|", expand=True)
            # final_response.columns = ["Datetime", "Open", "High", "Low", "Close", "Volume", "1", "2"]
            final_response.columns = ["Datetime", "Open", "High", "Low", "Close", "Volume", "OI", "2"]
            final_response.drop("2", axis=1, inplace=True)

            # final_response['dt_time'] = final_response['Datetime'].apply(lambda x: datetime.utcfromtimestamp(int(x)))
            # final_response['dt_time'] = pd.to_datetime(final_response['dt_time']).strftime("%d%b%Y %H%M%S")

            final_response['dt_time'] = pd.to_datetime(final_response['Datetime'], unit='s').dt.strftime(
                "%d%b%Y %H%M%S")

            return final_response
        else:
            empty_data = {
                "message": "Data is empty"
            }
            print(f"Data is empty for {data.exchange_instrument_id}")
            logging.error(f"Data is empty for {data.exchange_instrument_id}")
            return empty_data
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        # print(f"An error occurred: {e}")
        # traceback.print_exc()
        return None
