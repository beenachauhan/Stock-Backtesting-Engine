import datetime
import pandas as pd
from dateutil.rrule import rrule, DAILY, MO, TU, WE, TH, FR
import matplotlib.pyplot as plt

from backtest.NewTrade import NewTrade


class Analysis:
    def __init__(self, db, results_file, backtest_config):
        self.result_data = pd.read_table(results_file,
                           delimiter =",",
                           names = ('Trade_Id', 'Entry_Date', 'Position',
                                    'Ticker1', 'Ticker2',
                                    'Pos1', 'Pos2', 'Ratio',
                                    'Exit_Date', 'Avg_Day',
                                    'Max_Day', 'Min_Day', 'Tr_Length',
                                    'Total_PnL'),
                           index_col = False)
        self.start_year = backtest_config.analysis_start_year
        self.end_year = backtest_config.analysis_end_year
        self.config = backtest_config
        self.db = db

    def _trade_stats(self, formatting, col_name, result_data):
        if formatting == 'daily':
            result_data = result_data[result_data['PnL'] != 0]
            print(len(result_data))

        total_trades = len(result_data[col_name])
        print("Total {0}: {1}".format(formatting, total_trades))

        mask = result_data[col_name] > 0
        all_winning_trades = result_data[col_name].loc[mask]

        total_win_tr = len(all_winning_trades)
        print("Total winning {0}: {1}".format(formatting, total_win_tr))

        win_percent = float(total_win_tr) / float(total_trades)
        print("Win percentage: {}".format(win_percent))

        total_pnl = result_data[col_name].sum()
        print("Total {0} PnL: {1}".format(formatting, total_pnl))

        avg_trade = result_data[col_name].mean()
        print("Avg {0} PnL: {1}".format(formatting, avg_trade))

        notional = 100000.0
        rounded_val = round((avg_trade / notional) * 100.0, 4)
        print("Avg {0} with 100k notional: {1} %".format(formatting, rounded_val))

        max_winner = result_data[col_name].max()
        print("Max winning {0} PnL: {1}".format(formatting, max_winner))

        max_loser = result_data[col_name].min()
        print("Max losing {0} PnL: {1}".format(formatting, max_loser))

        avg_winner = all_winning_trades.mean()
        print("Avg winning {0} PnL: {1}".format(formatting, avg_winner))

        mask = result_data[col_name] <= 0
        all_losing_trades = result_data[col_name].loc[mask]
        avg_loser = all_losing_trades.mean()
        print("Avg losing {0} PnL: {1}".format(formatting, avg_loser))

        return [total_trades, total_win_tr, total_pnl, win_percent, avg_trade,
                max_winner, max_loser, avg_winner, avg_loser]

    def _daily_stats(self, df_trds, start, end, params):
        trd_dict = {}
        for row in df_trds.itertuples():
            if row[2] in trd_dict:
                trd_dict[row[2]].append([row[1], row[3], row[4]])
            else:
                trd_dict[row[2]] = [[row[1], row[3], row[4]]]

        params = params
        start_date = start
        end_date = end

        trd_holder = {}
        daily_pnl = []
        curr_year = 0
        daterange = rrule(DAILY, dtstart=start_date, until=end_date, byweekday=(MO, TU, WE, TH, FR))
        for tr_date in daterange:

            date_int = int(tr_date.strftime("%Y%m%d"))
            year = int(str(date_int)[0:4])
            if year != curr_year:
                curr_year = year
                trd_holder = {}

            days_pnl = 0.0
            no_trades = 0

            trds_to_delete = []

            for k, trd in trd_holder.copy().items():
                if k != "deleted":
                    days_pnl += trd.fetch_day_pnl(date_int)
                    no_trades += 1

                    if trd.exit_date_check(date_int):
                        trd_id_to_remove = trd.fetch_trade_id()
                        trds_to_delete.append(trd_id_to_remove)

            for trd_del in trds_to_delete:
                trd_holder["deleted"] = trd_holder.pop(trd_del)

            daily_pnl.append([date_int, days_pnl, no_trades])

            if date_int in trd_dict:
                trd_ids = trd_dict[date_int]

                for trade in trd_ids:
                    trd_id = trade[0]
                    ticker_1 = trade[1]
                    ticker_2 = trade[2]
                    new_trd = NewTrade(trd_id, ticker_1, ticker_2, params)
                    trd_holder[trd_id] = new_trd

        daily_df = pd.DataFrame(daily_pnl, columns=['Date', 'PnL', 'TradeCount'])
        daily_stats = self._trade_stats('daily', 'PnL', daily_df)
        plt.figure(figsize=(10, 10))

        plt.xlabel("date")
        plt.ylabel("profit")
        plt.title('daily profit or lose')
        daily_df["SMA1"] = daily_df['PnL'].rolling(window=50).mean()
        daily_df["SMA2"] = daily_df['PnL'].rolling(window=200).mean()

        plt.plot(daily_df['SMA1'], 'g--', label="SMA1")
        plt.plot(daily_df['SMA2'], 'r--', label="SMA2")
        plt.plot(daily_df['PnL'], label="close")
        plt.legend()
        plt.show(block=True)
        return daily_pnl, daily_stats

    def get_daily_stats(self):
        daily_stats_data = self.result_data[['Trade_Id','Entry_Date', 'Ticker1', 'Ticker2']]
        start_date_day = self.db.fetch_last_day(self.start_year)
        end_date_day = self.db.fetch_last_day(self.end_year)

        start_date = datetime.date(self.start_year, 12, start_date_day)
        end_date = datetime.date(self.end_year, 12, end_date_day)

        self._daily_stats(
            daily_stats_data,
            start_date,
            end_date,
            self.config.trading_folder_suffix
        )

    def get_trade_stats(self):
        return self._trade_stats('trade', 'Total_PnL', self.result_data)
