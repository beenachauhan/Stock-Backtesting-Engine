import os
import pandas as pd

class NewTrade():
    def __init__(self, trd_id, ticker_1, ticker_2, params):
        self.trd_id = trd_id
        self.cur_dir = os.getcwd()
        self.ticker1 = ticker_1
        self.ticker2 = ticker_2
        self.params = params
        # load our trd data into a df
        self.daily_trd_df = self.load_trd_history()
        self.exit_date = self.daily_trd_df.iloc[-1]['Date']

    def load_trd_history(self):
        # need to load our trd history into a pd dataframe
        ticker_dir = self.ticker1 + "_" + self.ticker2 + "/"
        path_load = self.cur_dir + "/PairsResults" + self.params + "/" + ticker_dir + self.trd_id + ".txt"
        daily_trd_df = pd.read_csv(path_load, sep=',', header=0)
        return daily_trd_df

    def fetch_day_pnl(self, date_int):
        # given a date, we need to fetch day's PnL
        try:
            day_pnl = self.daily_trd_df.loc[self.daily_trd_df['Date'] == date_int, 'PnL'].iloc[0]
        except:
            # most likely holiday
            day_pnl = 0
        return day_pnl

    def fetch_trade_id(self):
        return self.trd_id

    def exit_date_check(self, date_int):
        if self.exit_date == date_int:
            return True
        else:
            return False