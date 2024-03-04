import datetime
import functools
import pandas as pd
import os

from backtest.Analysis import Analysis
from backtest.PairBackTest import PairBackTest


class BackTest:
    def __init__(self, db, pairs, backtestConfig):
        self.db = db
        self.pairs = pairs
        self.z_upper_limit = backtestConfig.z_upper_limit
        self.z_lower_limit = backtestConfig.z_lower_limit
        self.short_lookback = backtestConfig.short_window
        self.long_lookback = backtestConfig.long_window
        self.initial_capital = backtestConfig.initial_capital
        self.config = backtestConfig

    def run(self):
        print("******* Starting Backtesting ****************")
        print ("Cointegrated Pairs found - ")
        for pair in self.pairs:
            print(pair)

        for cointegrated_pair in self.pairs:
            start_date = cointegrated_pair.start_date
            end_date = cointegrated_pair.end_date
            trading_last_day_start = self.db.fetch_last_day_for_month(end_date.year, 11)
            trading_start_date = datetime.date(start_date.year, start_date.month - 1, trading_last_day_start)

            trading_last_day_end = self.db.fetch_last_day_for_month(end_date.year, 12)
            trading_end_date = datetime.date(end_date.year, end_date.month - 1, trading_last_day_end)

            for pair in cointegrated_pair.pairs:
                print("starting trading for - ", pair)
                data_array = self.db.load_df_stock_data_array(pair, trading_start_date, trading_end_date)
                merged_data = functools.reduce(lambda left, right: pd.merge(left, right, on='Date'), data_array)
                merged_data.set_index('Date', inplace=True)
                z_threshold = [self.z_upper_limit, self.z_lower_limit]
                lookback_periods = [self.short_lookback, self.long_lookback]
                new_pair = PairBackTest(pair, merged_data, z_threshold, lookback_periods, self.initial_capital)
                new_pair.backtest()

        print("********* Backtesting done *************")
        cur_path = os.getcwd()
        results_file = cur_path + "/PairsResults" + self.config.trading_folder_suffix + "/MasterResults.txt"
        print("********** Starting Analysis of trading data **********")
        analysis = Analysis(self.db, results_file, self.config)
        analysis.get_trade_stats()
        analysis.get_daily_stats()



