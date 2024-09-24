from TAI.source import alpaca
from datetime import datetime

alpaca_handler = alpaca.Alpaca()

data = alpaca_handler.get_stock_historical(
    symbol_or_symbols='BABA',
    lookback_period=20*365,
    end=datetime.now(),
    timeframe='Day',
    ohlc=True
)

print(data)
