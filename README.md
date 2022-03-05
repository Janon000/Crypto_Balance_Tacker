
# Kraken Crypto Balance Chart

Display the performance of a Kraken exchange account using an ohlc chart
Default currency pair is USD




## Example


    from kraken_chart import kraken_chart
    # Load key and secret from file.
    # Expected file format is key and secret on separate lines.
    apikey_path = 'apikey.txt'

    # Get an instance of kraken chart class
    kc = kraken_chart(apikey_path)

    # Returns ledger as a dataframe, query speed
    ledger = kc.get_ledger_history()

    # Get ledger as an ohlc candle chart showing crypto performance against USD
    fig = kc.chart_data(ledger)
    fig.show()


![Screenshot](https://i.imgur.com/3ANti88.png)