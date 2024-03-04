import datetime
import functools
import pandas as pd
import numpy as np
import statsmodels.tsa.stattools as ts
import matplotlib.pyplot as plt
import seaborn

from backtest.CointegratedPairs import CointegratedPairs


class PairFinder:
    def __init__(self, db, start_year, end_year, p_value=0.1, sector_dict=None):
        self.start_year = start_year
        self.end_year = end_year
        self.db = db
        self.sector_dict = sector_dict
        self.p_value = p_value

    def _show_heat_map(self, pvalues, ticker_arr):
        confidence_level = 1 - 0.01
        plt.figure(figsize=(min(10, len(pvalues)), min(10, len(pvalues))))
        seaborn.heatmap(pvalues, xticklabels=ticker_arr,
                        yticklabels=ticker_arr, cmap='RdYlGn_r',
                        mask=(pvalues >= confidence_level))
        plt.show(block=True)

    def find_pairs(self):
        cointegrated_pairs = []
        year = self.start_year
        end_year = self.end_year
        last_trading_day_start = self.db.fetch_last_day(year)
        last_trading_day_end = self.db.fetch_last_day(end_year)

        start_date = datetime.date(year, 12, last_trading_day_start)
        end_date = datetime.date(end_year, 12, last_trading_day_end)

        list_of_stocks = self.db.load_db_tickers_sectors(start_date)
        if self.sector_dict:
            sector_dict = self.sector_dict
        else:
            sector_dict = {}
            for stock_sectors in list_of_stocks:
                sector = stock_sectors[1]
                ticker = stock_sectors[0]

                if sector not in sector_dict:
                    sector_dict[sector] = [ticker]
                else:
                    sector_dict[sector].append(ticker)

        self._find_cointegrated_pairs(sector_dict, start_date, end_date, cointegrated_pairs)

        return cointegrated_pairs

    def _find_cointegrated_pairs(self, sector_dict, start_date, end_date, cointegrated_pairs):
        for sector, ticker_arr in sector_dict.items():
            print("getting data array for %s sector and %s ticker_array" % (sector, ticker_arr))
            data_array = self.db.load_df_stock_data_array(ticker_arr, start_date, end_date)
            print("data array completed")
            print(data_array)
            merged_data = functools.reduce(lambda left,right: pd.merge(left,right,on='Date'), data_array)
            merged_data.set_index('Date', inplace=True)
            print("********* merged data **************")
            print(merged_data)
            print("data array merged")

            n = merged_data.shape[1]
            score_matrix = np.zeros((n, n))
            pvalue_matrix = np.ones((n, n))
            keys = merged_data.keys()
            pairs = []
            for i in range(n):
                for j in range(i + 1, n):
                    s1 = merged_data[keys[i]]
                    s2 = merged_data[keys[j]]
                    print("s1 = ", s1)
                    print("s2 = ", s2)
                    result = ts.coint(s1, s2)
                    print("*********************")
                    print("result = ", result)
                    score = result[0]
                    pvalue = result[1]
                    score_matrix[i, j] = score
                    pvalue_matrix[i, j] = pvalue
                    if pvalue < self.p_value:
                        pairs.append((keys[i], keys[j]))

            self._show_heat_map(pvalue_matrix, ticker_arr)
            plt.figure(figsize=(10, 10))
            for i, ticker in enumerate(ticker_arr):
                plt.plot(data_array[i]['Date'], data_array[i][ticker], label=ticker)
            plt.axhline(y=pvalue_matrix[0][1], color='r', linestyle='-', label='pValue')
            plt.legend()
            plt.show(block=True)

            cointegrated_pairs.append(CointegratedPairs(sector, start_date, end_date, pairs))
