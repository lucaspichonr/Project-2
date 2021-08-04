import asyncio
import websockets
import json
import pandas as pd
import datetime as dt
import time

yesterday = dt.datetime.now() - dt.timedelta(days = 1)
last_30days = dt.datetime.now() - dt.timedelta(days = 30)

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
    res = json.loads(json_resp)

    df = pd.DataFrame(res['result'])


    return df



if __name__ == '__main__':
    start = last_30_beginning_time
    end = yesterday_end_time
    instrument = "BTC-PERPETUAL"
    timeframe = '60'
    json_resp = retrieve_historic_data(start, end, instrument, timeframe)

    df = json_to_dataframe(json_resp)
    print(df.head(-10))


df['ticks'] = pd.to_datetime(df['ticks'], unit='ms')
df

df.to_csv(r'C:\Users\lucas\Desktop\Fintech Bootcamp\Project#2\Perp30Days.csv', index = False)