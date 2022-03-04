"""
An implementation of the pykrakenapi and Krakenex api
for displaying crypto performance in chart form
"""


import krakenex
from pykrakenapi import KrakenAPI
import datetime as dt
import time
import pandas as pd
import pickle
import plotly.graph_objs as go


api = krakenex.API()
api.load_key('apikey.txt')
k = KrakenAPI(api)


def get_asset_ticker(ledger):
    """
    Convert Kraken crypto asset to Ticker, Kraken uses names that are unique to the platform, hence
    certain assets have to be manually handled.
    :param ledger:
    :return: Dictionary containing correct tickers
    """
    # List of altnames that will not return an ohlc against USD trading pair
    renames = {'ETH2': 'ETH', 'ETH2.S': 'ETH', 'FLOWH': 'FLOW', 'FLOWH.S': 'FLOW', 'USD.HOLD': 'USD', 'USD.M': 'USD',
               'ZUSD': 'USD'}
    exclusions = ['CHF', 'KFEE', 'ZCAD', 'ZJPY']

    # Get the list of assets available on Kraken exchange
    assets = k.get_asset_info()

    # Grab the list of assets in the ledger
    ledger_asset_list = ledger['asset'].unique().tolist()

    # Retrieve altname for each asset and store as dictionary
    asset_dict = {asset: assets['altname'][asset] for asset in ledger_asset_list}

    # Reduce specific altnames to queryable ticker and return ticker dict
    ticker_dict = {}
    for asset, altname in asset_dict.items():
        if asset not in exclusions:
            if asset in renames:
                ticker = renames[asset]
            else:
                ticker = altname.split('.')[0]
        ticker_dict[asset] = ticker

    return ticker_dict


def get_coin_history(ticker_dict, currency='USD'):
    """
    Get the 1 year price history for a cryptocurrency. Stablecoin price pricehistory is handled separately.
    :param ticker_dict: Dictionary containg kraken asset name and it's altname which is used to query data
    :param currency: Default currency to pair for
    :return: Dictionary containing the ticker and it's relevant ohlc dataframe
    """
    asset_history = {}

    for asset, altname in ticker_dict.items():
        if altname != 'USD':
            time.sleep(2)
            pair = altname + currency
            print('Retrieving 1 year data for ', asset)
            ohlc, last = k.get_ohlc_data(pair, interval=1440)
            ohlc['date'] = pd.to_datetime(ohlc["time"], unit="s").dt.strftime("%Y-%m-%d")
            asset_history[asset] = ohlc[:365]

    history = open("historycache.pickle", "wb")
    pickle.dump(asset_history, history)
    return asset_history


def get_ledger_history():
    """
    Read the exchange ledger history as a dataframe
    :return: Dataframe containing ledger of a kraken exchange wallet
    """
    # Get Unix timestamp for 1 year ago
    year = dt.date.today() - dt.timedelta(365)
    unix = time.mktime(year.timetuple())

    # Get ledger dataframe
    ledger, count = k.get_ledgers_info()
    offset = 50
    while True:
        try:
            time.sleep(2)
            ledger2, count = k.get_ledgers_info(ofs=offset, start=unix)
            ledger = pd.concat([ledger, ledger2])
            print(ledger)
            end = ledger2['time'].iloc[-1]
            offset += 50
        except KeyError:
            print('end of ledger')
            break

    return ledger


def process_ledger(ledger):
    """
    Return Dataframe showing asset balances over time
    :param ledger:
    :return:
    """

    # Grab the list of 'tickers' in the ledger
    ticker_dict = get_asset_ticker(ledger)

    # Map correct tickers to kraken asset name in dataframe
    ledger['ticker'] = ledger['asset'].map(ticker_dict)

    # Add date field and usd balance field
    ledger['date'] = pd.to_datetime(ledger["time"], unit="s").dt.strftime("%Y-%m-%d")
    ledger['usdbalance'] = ''
    ledger.to_excel('ledger.xlsx')

    # Get 1 year data for each coin in ledger
    history = get_coin_history(ticker_dict)

    # Create Dataframe containing the daily balance for each asset
    dfs = []
    for crypto, ohlc in history.items():
        # Create subset of ledger for each crypto, drop duplicates on the same date to get the most recent balance
        df = ledger.loc[ledger['asset'] == crypto]
        df2 = df.drop_duplicates('date', keep='first')
        # Merge the ohlc data for the year with the subset in the ledger to return the balance for each day of the year
        out = ohlc.merge(df2[['date', 'balance']], on='date', how='left')
        out.interpolate(method='backfill', axis=0, inplace=True)
        # Calculate the daily usd balance for crypto and append to list
        out[crypto] = out['balance'] * out['vwap']
        out[crypto+'_open'] = out['balance'] * out['open']
        out[crypto+'_high'] = out['balance'] * out['high']
        out[crypto+'_low'] = out['balance'] * out['low']
        out[crypto+'_close'] = out['balance'] * out['close']
        dfs.append(out[['date', crypto+'_open', crypto+'_high', crypto+'_low', crypto+'_close']])

    # Create final dataframe using the dataframe for each crypto
    combined = pd.concat([
        df.set_index('date') for df in dfs], axis=1, join='outer'
    )

    # Get the combined open, high low and close data
    combined['open'] = combined[[col for col in combined.columns if col.endswith('_open')]].sum(axis=1)
    combined['high'] = combined[[col for col in combined.columns if col.endswith('_high')]].sum(axis=1)
    combined['low'] = combined[[col for col in combined.columns if col.endswith('_low')]].sum(axis=1)
    combined['close'] = combined[[col for col in combined.columns if col.endswith('_close')]].sum(axis=1)
    combined.to_excel('combined.xlsx')
    return combined


def chart_data(data):
    # declare figure
    fig = go.Figure()

    # Candlestick
    fig.add_trace(go.Candlestick(x=data.index,
                                 open=data['open'],
                                 high=data['high'],
                                 low=data['low'],
                                 close=data['close'], name='market data'))
    # Add titles
    fig.update_layout(
        title='Portfolio Balance',
        yaxis_title='Balance (US Dollars)')

    # X-Axes
    fig.update_xaxes(
        rangeslider_visible=True,
        rangeselector=dict(
            buttons=list([
                dict(count=7, label="7d", step="minute", stepmode="backward"),
                dict(count=30, label="1m", step="minute", stepmode="backward"),
                dict(count=1, label="6m", step="day", stepmode="todate"),
                dict(count=365, label="year", step="day", stepmode="backward"),
                dict(step="all")
            ])
        )
    )

    # Launches browser window to show chart
    fig.show()


ledge = get_ledger_history()
df = process_ledger(ledge)
chart_data(df)



