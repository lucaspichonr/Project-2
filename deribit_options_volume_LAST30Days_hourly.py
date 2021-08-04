import asyncio
import websockets
import json
import pandas as pd
import datetime as dt
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import schedule
import pygsheets

async def call_api(msg2):
    async with websockets.connect('wss://test.deribit.com/ws/api/v2') as websocket:
       await websocket.send(msg2)
       while websocket.open:
           response = await websocket.recv()
           return response
def async_loop(api, message):
    return asyncio.get_event_loop().run_until_complete(api(message))
def retrieve_historic_data(currency, kind, expired):
    msg2 = \
        {
            "jsonrpc": "2.0",
            "id": 7617,
            "method": "public/get_instruments",
            "params": {
                "currency": currency,
                "kind": kind,
                "expired": expired
            }
        }
    resp = async_loop(call_api, json.dumps(msg2))
    return resp
def json_to_dataframe(json_resp):
#    print(json_resp)
    res = json.loads(json_resp)
    df2 = pd.DataFrame(res['result'])
    return df2
currency = 'BTC'
kind = 'option'
expired = False
master_df2 = pd.DataFrame()

for knd in kind:
    try:
        
        json_resp = retrieve_historic_data(currency, kind, expired)
    #    print("***************** Inside main ********************")
    #    print(json_resp)
        df2 = json_to_dataframe(json_resp)
        df2["kind"] = knd
        master_df2 = pd.concat([master_df2, df2])
        
        df2 = pd.DataFrame()
    except:
        print("Error with "+ knd)
        continue

active_instruments = master_df2['instrument_name']

instruments_list = active_instruments.values.tolist()

yesterday = dt.datetime.now() - dt.timedelta(days = 1)
last_30days = dt.datetime.now() - dt.timedelta(days = 30)
four_hs = dt.datetime.now() - dt.timedelta(hours = 4)
four_hs_tuple = (int(time.mktime(four_hs.timetuple()))*1000)
now = dt.datetime.now()
now_tuple = (int(time.mktime(now.timetuple()))*1000)

yesterday_beginning = dt.datetime(yesterday.year, yesterday.month, yesterday.day,0,0,0,0)
yesterday_beginning_time = (int(time.mktime(yesterday_beginning.timetuple()))*1000)
yesterday_end = dt.datetime(yesterday.year, yesterday.month, yesterday.day,23,59,59,999)
yesterday_end_time = (int(time.mktime(yesterday_end.timetuple()))*1000)

last_30_beginning = dt.datetime(last_30days.year, last_30days.month, last_30days.day,0,0,0,0)
last_30_beginning_time = (int(time.mktime(last_30_beginning.timetuple()))*1000)
last_30_end = dt.datetime(last_30days.year, last_30days.month, last_30days.day,23,59,59,999)
last_30_end_time = (int(time.mktime(last_30_end.timetuple()))*1000)

async def call_api(msg):
    async with websockets.connect('wss://test.deribit.com/ws/api/v2') as websocket:
       await websocket.send(msg)
       while websocket.open:
           response = await websocket.recv()
           return response
def async_loop(api, message):
    return asyncio.get_event_loop().run_until_complete(api(message))
def retrieve_historic_data(start, end, instrument, timeframe):
    #raw_data = []
    #pool = ThreadPoolExecutor(max_workers=5)
    msg = \
        {
            "jsonrpc": "2.0",
            "id": 833,
            "method": "public/get_tradingview_chart_data",
            "params": {
                "instrument_name": instrument,
                "start_timestamp": start,
                "end_timestamp": end,
                "resolution": timeframe
            }
        }
    resp = async_loop(call_api, json.dumps(msg))
    return resp
def json_to_dataframe(json_resp):
#    print(json_resp)
    res = json.loads(json_resp)
    df = pd.DataFrame(res['result'])
    df['ticks'] = df.ticks / 1000
    df['timestamp'] = [dt.datetime.fromtimestamp(date) for date in df.ticks]
    return df
start = last_30_beginning_time
end = now_tuple
instrument = instruments_list
timeframe = '60'
master_df = pd.DataFrame()

#raw_data = []
#pool = ThreadPoolExecutor(max_workers=5)

for ins in instrument:

    try:
        print(ins)
        json_resp = retrieve_historic_data(start, end, ins, timeframe)
    #    print("***************** Inside main ********************")
    #    print(json_resp)
        df = json_to_dataframe(json_resp)
        df["Instrument"] = ins
        master_df = pd.concat([master_df, df])
        print(master_df.tail(2))
        df = pd.DataFrame()
    except:
        print("Error with "+ ins)
        continue

master_df_clean = master_df.drop_duplicates()

master_df_clean2 = master_df_clean[['volume','Instrument','timestamp']]
master_df_clean3 = master_df_clean2[['volume','Instrument','timestamp']][master_df_clean2['volume'] > 0].dropna()
master_df_clean4 = master_df_clean3.sort_values('volume', ascending=False)

master_df_clean4.to_csv(r'C:\Users\lucas\Desktop\Fintech Bootcamp\Project#2\Options_Last30Days_hourly.csv.csv', index = False)