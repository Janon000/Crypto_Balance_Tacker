import krakenex
from pykrakenapi import KrakenAPI
import time
import datetime as dt
import time
import pandas as pd
import pickle

api = krakenex.API()
api.load_key('apikey.txt')
k = KrakenAPI(api)


def get_asset_ticker(asset):
    """
    Convert Kraken crypto asset to Ticker, drop staking labels .S and Etherium2
    :param asset:
    :return:
    """
    time.sleep(2)
    asset = k.get_asset_info(asset=asset)
    asset = asset['altname'][0]
    asset = asset.split('.')[0]
    asset = asset.split('2')[0]
    return asset


def get_coin_history(ticker_dict, currency='USD'):
    """
    Get the 1 year price history for a cryptocurrency
    :param crypto:
    :param currency:
    :return: OHLC dataframe
    """
    asset_history = {}
    for crypto in set(ticker_dict.values()):
        if crypto != 'USD':
            time.sleep(2)
            pair = crypto + currency
            print('Retrieving 1 year data for ', pair)
            ohlc, last = k.get_ohlc_data(pair, interval=1440)
            ohlc['date'] = pd.to_datetime(ohlc["time"], unit="s").dt.strftime("%Y-%m-%d")
            asset_history[crypto] = ohlc[:360]

    history = open("cryptohistory2.pickle", "wb")
    pickle.dump(asset_history, history)
    return asset_history


def get_ledger_history():
    """
    Read the exchange ledger history as a dataframe
    :return:
    """
    ledger, count = k.get_ledgers_info()
    offset = 50
    while True:
        try:
            time.sleep(2)
            ledger2, count = k.get_ledgers_info(ofs=offset)
            ledger = pd.concat([ledger, ledger2])
            print(ledger)
            end = ledger2['time'].iloc[-1]
            offset += 50
        except KeyError:
            print('end of ledger')
            break
    print('Converting assets to tickers..(limited by kraken rate limit of 2 seconds per query)')
    asset_list = ledger['asset'].unique().tolist()
    ticker_dict = {asset: get_asset_ticker(asset) for asset in asset_list}
    ledger['ticker'] = ledger['asset'].map(ticker_dict)
    ledger['date'] = pd.to_datetime(ledger["time"], unit="s").dt.strftime("%Y-%m-%d")
    ledger['usdbalance'] = ''
    history = get_coin_history(ticker_dict)
    ledger_dict = ledger.to_dict('index')
    for values in ledger_dict.values():
        df = history[values['ticker']]
        print(df.head())
    print(ledger_dict)




    return ledger


ledger = get_ledger_history()

#print(ledger[['asset', 'ticker', 'balance','date']])

# print(get_asset_ticker('ZUSD'))
