import krakenex
from pykrakenapi import KrakenAPI
import datetime
import time
import pandas as pd
import json
import pickle
import plotly.graph_objs as go

api = krakenex.API()
api.load_key('apikey.txt')
k = KrakenAPI(api)


def get_ledger_history():
    """
    Read the exchange ledger history as a dataframe
    :return:
    """
    ledger, count = k.get_ledgers_info()
    print(ledger)
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
    process_ledger(ledger)
    # ledger.to_excel('ledger2.xlsx')


def process_ledger(ledger):
    """
    Get the asset balances in the ledger
    :param ledger:
    :return:
    """
    days = {}
    asset_dict = {}
    for idx in reversed(ledger.index):
        # Get Time, Asset and Balance for each row
        unix_time = ledger.loc[idx]['time']
        date = datetime.datetime.fromtimestamp(unix_time).strftime('%Y-%m-%d')

        asset = ledger.loc[idx]['asset']
        asset_balance = ledger.loc[idx]['balance']
        asset_dict[asset] = asset_balance
        days[date] = asset_dict.copy()

    with open("asset_dict.json", "w") as outfile:
        json.dump(asset_dict, outfile)

    with open("ledger.json", "w") as outfile:
        json.dump(days, outfile)

    ledger_to_balance(days)


def get_coin_history(ticker_dict, currency='USD'):
    """
    Get the 1 year price history for a cryptocurrency
    :param crypto:
    :param currency:
    :return: OHLC dataframe
    """
    asset_history = {}
    for crypto in set(ticker_dict.values()):
        if crypto is not None:
            time.sleep(2)
            pair = crypto + currency
            print('Retrieving 1 year data for ', pair)
            ohlc, last = k.get_ohlc_data(pair, interval=1440)
            asset_history[crypto] = ohlc[:360]

    history = open("cryptohistory.pickle", "wb")
    pickle.dump(asset_history, history)
    return asset_history


def get_asset_ticker(asset):
    """
    Convert Kraken crypto asset to Ticker, drop staking labels .S and Etherium2
    :param asset:
    :return:
    """
    if asset != 'ZUSD':
        asset = k.get_asset_info(asset=asset)
        asset = asset['altname'][0]
        asset = asset.split('.')[0]
        asset = asset.split('2')[0]
        return asset


def asset_price_fetch(asset, date, coin_history):
    """
    Fetch asset price from Yahaoo finance
    :param asset:
    :param date:
    :return: Adjusted closing price
    """

    # Get the history for the asset pair and date
    history = coin_history[asset]
    row = history[date]
    if len(row) == 0:
        price = 0
    else:
        price = row['vwap'].item()
    return price


def chart_data(data):
    # declare figure
    fig = go.Figure()

    # Candlestick
    fig.add_trace(go.Candlestick(x=data.index,
                                 balance=data['Balance'], name='market data'))

    # Add titles
    fig.update_layout(
        title='Portfolio Balance',
        yaxis_title='Balance (US Dollars)')

    # X-Axes
    fig.update_xaxes(
        rangeslider_visible=True,
        rangeselector=dict(
            buttons=list([
                dict(count=7, label="7d", step="day", stepmode="backward"),
                dict(count=30, label="1m", step="day", stepmode="backward"),
                dict(count=1, label="6m", step="day", stepmode="todate"),
                dict(count=365, label="year", step="day", stepmode="backward"),
                dict(step="all")
            ])
        )
    )

    # Show
    fig.show()



def ledger_to_balance(ledger_dict):
    """
    :param ledger_dict:
    :return: Chart data
    """
    # Get the last asset list from the most recent day
    last_day = next(reversed(ledger_dict.items()))
    asset_list = last_day[1].keys()
    ticker_dict = {}

    # Use kraken api to convert kraken asset name to proper ticker
    for asset in asset_list:
        time.sleep(2)
        ticker = get_asset_ticker(asset)
        ticker_dict[asset] = ticker
    print(ticker_dict)

    # Use the ticker list to download the ohlc history for each coin in ledger
    coin_history = get_coin_history(ticker_dict)

    # Create dataframe for each crypto and corresponding balance for each day
    ledger_dfs = {}
    for date, bal_dict in ledger_dict.items():
        for crypto, balance in bal_dict.items():
            row = {date: balance}
            if crypto in ledger_dfs:
                ledger_dfs[crypto].update(row.copy())
            else:
                ledger_dfs[crypto] = row.copy()
    for crypto, dict in ledger_dfs.items():
        df = pd.DataFrame({crypto: dict})


    # Build dataframe containing the USD balance for each asset for each day of the year
    ledger_balance = {}
    for date, asset_dict in ledger_dict.items():
        balance_dict = {}
        for asset, balance in asset_dict.items():
            ticker = ticker_dict[asset]
            if ticker is None:
                balance_dict[asset] = balance.item()
            elif ticker is not None:
                price = asset_price_fetch(ticker, date, coin_history)
                balance_dict[asset] = price * balance.item()

        ledger_balance[date] = sum(balance_dict.values())
    print(ledger_balance)
    
"""testdict = {}
    datelist = []
    balancelist = []
    for date,balance in ledger_balance.items():
        datelist.append(date)
        balancelist.append(balance)
    testdict['Date'] = datelist
    testdict['Balance'] = balancelist
    df = pd.DataFrame(testdict, columns=['Date', 'Balance'])
    df = df.set_index('Date')
    print(df)
    chart_data(df)"""


get_ledger_history()


#eth = get_coin_history({'ETH':'ETH'})
#asset_price_fetch('ETH', '1-2-2019', eth)

