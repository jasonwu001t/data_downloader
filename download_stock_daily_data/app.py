import copy

# Create a template for stock entries
stock_entry_template = {
    "date": None,
    "stock_symbol": None,
    "fundamentals": {
        "pe_ratio": None,
        "eps": None,
        "market_cap": None,
        "week_52_range": {
            "low": None,
            "high": None
        }
    },
    "prices": {
        "current_price": None,
        "open": None,
        "high": None,
        "low": None,
        "close": None,
        "previous_close": None
    },
    "volume": None,
    "returns": {
        "daily_return": None,
        "drawdown": None
    },
    "indicators": {
        "rsi": None,
        "sma_50": None,
        "sma_200": None
    }
}

# Initialize an empty list to hold stock data
stock_data = []

# Sample data for Apple Inc. (AAPL)
aapl_entry = copy.deepcopy(stock_entry_template)
aapl_entry["date"] = "2023-10-05"
aapl_entry["stock_symbol"] = "AAPL"
aapl_entry["fundamentals"]["pe_ratio"] = 28.5
aapl_entry["fundamentals"]["eps"] = 5.2
aapl_entry["fundamentals"]["market_cap"] = 2500000000000
aapl_entry["fundamentals"]["week_52_range"]["low"] = 100.0
aapl_entry["fundamentals"]["week_52_range"]["high"] = 150.0
aapl_entry["prices"]["current_price"] = 142.5
aapl_entry["prices"]["open"] = 140.0
aapl_entry["prices"]["high"] = 143.0
aapl_entry["prices"]["low"] = 139.5
aapl_entry["prices"]["close"] = 142.5
aapl_entry["prices"]["previous_close"] = 141.0
aapl_entry["volume"] = 50000000
aapl_entry["returns"]["daily_return"] = 1.06
aapl_entry["returns"]["drawdown"] = 0.0
aapl_entry["indicators"]["rsi"] = 55.0
aapl_entry["indicators"]["sma_50"] = 138.0
aapl_entry["indicators"]["sma_200"] = None

# Append the entry to the stock data list
stock_data.append(aapl_entry)

# Sample data for Alphabet Inc. (GOOG) with some keys missing
goog_entry = copy.deepcopy(stock_entry_template)
goog_entry["date"] = "2023-10-05"
goog_entry["stock_symbol"] = "GOOG"
goog_entry["fundamentals"]["pe_ratio"] = 30.1
# 'eps' is left as None
goog_entry["fundamentals"]["market_cap"] = 1800000000000
# 'week_52_range' is left as None
goog_entry["prices"]["current_price"] = 2750.0
goog_entry["prices"]["open"] = 2700.0
# 'high', 'low', 'close', 'previous_close' are left as None
goog_entry["volume"] = 1500000
goog_entry["returns"]["daily_return"] = 0.36
goog_entry["returns"]["drawdown"] = -0.5
# 'indicators' are left as None

# Append the entry to the stock data list
stock_data.append(goog_entry)

print(stock_data)
