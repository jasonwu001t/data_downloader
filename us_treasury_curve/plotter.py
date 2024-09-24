import pandas as pd
import os
from TAI.data import DataMaster
from TAI.analytics import QuickPlot

def generate_interest_rate_plot():
    dm = DataMaster()
    qp = QuickPlot()

    # Load rates and process NaN values
    full_path = os.path.join(dm.get_current_dir(), 'data','treasury_yield_all.csv')
    print (full_path)

    rates = pd.read_csv(full_path)
    for col in rates.columns:
        rates[col] = rates[col].fillna(rates[rates.columns[rates.columns.get_loc(col) - 1]])

    # Convert and sort by date
    rates['date'] = pd.to_datetime(rates['Date'])
    rates = rates.sort_values(by='date', ascending=True)
    rates = rates[['date', '1 Mo', '2 Mo', '3 Mo', '4 Mo', '6 Mo', '1 Yr', '2 Yr', '3 Yr', '5 Yr', '7 Yr', '10 Yr', '20 Yr', '30 Yr']]
    rates.set_index('date', inplace=True)

    # Get the latest rate date
    latest_rate_date = rates.index[-1]

    # Define time periods to look back
    time_periods = {
        'Last Week': pd.DateOffset(weeks=1),
        'Last Month': pd.DateOffset(months=1),
        'Last 3 Months': pd.DateOffset(months=3),
        'Last Year': pd.DateOffset(years=1),
        '3 Years Ago': pd.DateOffset(years=3),
        '5 Years Ago': pd.DateOffset(years=5),
        '10 Years Ago': pd.DateOffset(years=10),
        '20 Years Ago': pd.DateOffset(years=20),
        '30 Years Ago': pd.DateOffset(years=30)
    }

    # Get rates for each time period
    rates_data = {'Today': rates.tail(1)}
    for period_name, offset in time_periods.items():
        rates_data[period_name] = rates.loc[[rates.index.asof(latest_rate_date - offset)]]

    # Plot the rates
    fig_rates = qp.plot_interest_rates(rates_data, hidden_labels=['3 Years Ago','5 Years Ago', '10 Years Ago','20 Years Ago', '30 Years Ago'])
    
    return fig_rates

if __name__ == "__main__":
    # If this script is run directly, generate the plot and display it
    fig = generate_interest_rate_plot()
    fig.show()