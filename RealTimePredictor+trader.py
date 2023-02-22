# this code is implementation of long-only strategy on Binance spot market.
# with fix size, the bot will buy when the price prediction of the model is higher than the actual close.
# I also implented an alert on telegram to watch the state of the bot live & easily
# to run this code you need API key and secret from binance and store it in config file.


import config, csv, talib, pickle, websocket, json, pprint, telegram_send
from binance.client import Client
from binance.enums import *
import pandas as pd
from datetime import datetime
import numpy as np

#define Trade Symbol
TRADE_SYMBOL = 'BTCUSDT'
TRADE_QOUTE = 'BTC'

#define size
BUY_QUANTITY = 0.000400
SELL_QUANTITY = 0.000399

#open a report file
now = datetime.now()
REPORT_FILE_PATH = 'report_' + now.strftime('%Y-%m-%d_%H:%M:%S') +'.csv'
fieldnames = ['side', 'price', 'qty', 'commission', 'commissionAsset', 'tradeId']
with open (REPORT_FILE_PATH, 'w', encoding='UTF8', newline='') as fhand:
    writer = csv.DictWriter(fhand, fieldnames=fieldnames)
    writer.writeheader()

in_position = False

SOCKET = "wss://stream.binance.com:9443/ws/btcusdt@kline_1m"

closes = []

client = Client(config.API_KEY, config.API_SECRET, {"verify": True, "timeout": 20})

# a function to put market order on binance
def order(side, qty, symbol, order_type= ORDER_TYPE_MARKET):
    try:
        print("sending order")
        order = client.create_order(symbol=symbol, side=side, type=order_type, quantity=qty)
        # print(order)
        fills = order['fills']
        for part in fills:
            report = order['side'] + " " + part['qty'] + " " + order['symbol'] + " at " + part['price'] + " cost " + part['commission'] + " " + part['commissionAsset'] + " for commission."
            print(report)
            with open (REPORT_FILE_PATH, 'a', encoding='UTF8', newline='') as fhand:
                writer = csv.DictWriter(fhand, fieldnames=fieldnames)
                writer.writerow({**{'side':order['side']},**part})
            telegram_send.send(messages=[report])
    except Exception as e:
        b = bytes(str(e), encoding = 'utf-8')
        print("Error {0}".format(str(b, encoding = 'utf-8')))
        telegram_send.send(messages=["Error {0}".format(str(b, encoding = 'utf-8'))])
        return False

    return True

# convert unix time for human readable
def UnixtoHuman(utime):
    utime = utime /1000
    return (datetime.utcfromtimestamp(utime).strftime('%Y-%m-%d %H:%M:%S'))

# opening connection with binance server
def on_open(ws):
    print(' ')
    print('opened connection')
    print(' ')
    telegram_send.send(messages=['Bot Launched! 🚀'])

    info = client.get_account()
    balances = info ['balances']
    print('your initial balances are:')
    telegram_send.send(messages=['your initial balances are: '])
    for balance in balances:
        if float(balance['free']) != 0 or float(balance['locked']) != 0:
            print(balance)
            telegram_send.send(messages=[balance])
    print(' ')

# this function will run on every message the server releasing
def on_message(ws, message):
    global closes, in_position

    # print("message recieved")
    json_message=json.loads(message)
    # pprint.pprint(json_message)

    candle = json_message['k']

    is_candle_closed=candle['x']
    close = candle['c']
    time  = candle['t']

    # waiting for the candle to close
    if is_candle_closed:
        print(" ")
        print("candle closed at {} in {}".format(close, UnixtoHuman(time)))
        print(" ")
        closes.append(float(close))
        #print("closes are : ")
        #print(closes)
        #print(' ')

        if (in_position):
            # if in position is ture, it means we should sell
            print("SELL! SELL! SELL!")
            # put Binance order logic here
            order_succeeded = order(SIDE_SELL, SELL_QUANTITY, TRADE_SYMBOL)
            if order_succeeded:
                in_position = False
                print('order is filled.')
        else:

            # getting data
            csvfile = open('cache.csv', 'w', newline='')
            candlestick_writer = csv.writer(csvfile, delimiter=',')
            candlesticks = client.get_historical_klines("BTCUSDT", Client.KLINE_INTERVAL_1MINUTE, "30 minutes ago UTC")

            for candlestick in candlesticks:
                candlestick[0] = candlestick[0] / 1000
                candlestick_writer.writerow(candlestick)

            csvfile.close()


            # date pre process
            df = pd.read_csv('cache.csv')
            df.columns = ['Open time', 'Open', 'Low', 'High', 'Close', 'Volume', 'Close time', 'Quote asset volume',
                              'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore.']

            df['Open time'] = df['Open time'].apply(datetime.utcfromtimestamp)
            df['Close time'] /= 1000
            df['Close time'] = df['Close time'].apply(datetime.utcfromtimestamp)
            df['Date'] = pd.to_datetime(df['Open time']).dt.date
            df['Time'] = pd.to_datetime(df['Open time']).dt.time
            del df['Ignore.']
            del df['Open time']
            del df['Close time']
            del df['Quote asset volume']
            del df['Number of trades']
            del df['Taker buy base asset volume']
            del df['Taker buy quote asset volume']
            cols = list(df.columns.values)
            cols = cols[-2:] + cols[:-2]
            df = df[cols]

            df_agg = df.groupby(["Date", "Time"])["Close"].last()
            df_agg = df_agg.reset_index().sort_values(by=["Date", "Time"], ascending=True)

            # generating MA & RSI as features
            for i in range(1, 7):
                df_agg['close_lag_' + str(i)] = df_agg['Close'].shift(i)
            for n in [6, 12, 18, 24]:
                df_agg['ma_' + str(n)] = talib.SMA(df_agg['Close'].values, timeperiod=n) / df_agg['Close']
                df_agg['rsi_' + str(n)] = talib.RSI(df_agg['Close'].values, timeperiod=n)

            df_agg.fillna(df_agg.mean(), inplace=True)

            df_n = df_agg[df_agg.columns.difference(['Close', 'Date', 'Time'])]
            df_n = (df_n - df_n.mean()) / df_n.std()
            df_final = pd.concat([df_n, df_agg.loc[:, 'Close']], axis=1)

            X = df_final.loc[:, df_final.columns != 'Close']
            Y = df_final['Close']
            X = np.hstack((np.ones((len(X), 1)), X))

            # loading the previously trained model
            with open('model','rb') as file:
                model = pickle.load(file)

            # prediction
            prediction = model.predict(X[-3:-1])
            # print(Y[-3:-1])
            # print(prediction)
            signal = "Buy" if prediction[-1] > prediction[-2] else "Sell"
            print("model advise to ", signal)
            print(" ")

            if signal == "Buy":
                #Buy
                print("BUY! BUY! BUY!")
                order_succeeded = order( SIDE_BUY, BUY_QUANTITY, TRADE_SYMBOL)
                if order_succeeded:
                    in_position = True
                    print('order is filled.')

        if len(closes) % 30 == 0:

            listen_key = client.stream_get_listen_key()
            client.stream_keepalive(listen_key)
            print('keepalive sent with a ping')

# handeling errors
def on_error(ws, error):
    # print(type(error))
    print(error)
    telegram_send.send(messages=[erro])
    # TODO if error was 'connection is already closed', relaunch the app

# closing the connection to the server and printing statement
def on_close(ws):

    info = client.get_account()
    balances = info ['balances']
    print('your final balances are:')
    telegram_send.send(messages=['your final balances are: '])
    for balance in balances:
        if float(balance['free']) != 0 or float(balance['locked']) != 0:
            print(balance)
            telegram_send.send(messages=[balance])
    # TODO if in_position is True, close the position
    print('closed connection')
    print(" ")
    telegram_send.send(messages=['Connection Closed.'])



ws = websocket.WebSocketApp(SOCKET,
                    on_open=on_open, on_message=on_message, on_error=on_error, on_close = on_close)
ws.run_forever()
