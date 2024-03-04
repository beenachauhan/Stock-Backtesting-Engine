import os


class PairBackTest():
    def __init__(self, pair, merged_data, z_threshold, lookback_periods, initial_capital):
        self.pair = pair
        self.stock_1 = self.pair[0]
        self.stock_2 = self.pair[1]
        self.merged_data = merged_data
        self.z_upper_limit = z_threshold[0]
        self.z_lower_limit = z_threshold[1]
        self.short_lookback = lookback_periods[0]
        self.long_lookback = lookback_periods[1]
        self.params = "_{0}_{1}".format(self.short_lookback, self.long_lookback)
        self.initial_capital = initial_capital
        self.ratios = self.merged_data[self.stock_1] / self.merged_data[self.stock_2]
        self.ma_short = self.ratios.rolling(window=self.short_lookback, center=False).mean()
        self.ma_long = self.ratios.rolling(window=self.long_lookback, center=False).mean()
        self.std = self.ratios.rolling(window=self.long_lookback, center=False).std()
        self.zscore = (self.ma_short - self.ma_long) / self.std
        self.total_dollars_per_trade = self.initial_capital * 2.0
        self.long_pos = False
        self.short_pos = False
        self.daily_data = []
        self.daily_data.append(
            "Date,Position,Ticker1,Ticker2,ZScore,Ticker1_Shares,Ticker2_Shares,Ratio,Ticker1_P,Ticker2_P,Days,PnL")
        self.directory_pair = "PairsResults" + self.params + "/{0}_{1}".format(self.stock_1, self.stock_2)
        self.create_directories()

        self.TrdID = ""
        self.position = ""
        self.pos1 = 0.0
        self.pos2 = 0.0
        self.pEntryStock1 = 0.0
        self.pEntryStock2 = 0.0
        self.pYestStock1 = 0.0
        self.pYestStock2 = 0.0
        self.pCurrentStock1 = 0.0
        self.pCurrentStock2 = 0.0
        self.pExitStock1 = 0.0
        self.pExitStock2 = 0.0
        self.ProfitLoss = 0.0
        self.ProfitLossHistory = []
        self.TradeProfitLoss = 0.0
        self.OrigTradeRatio = 0.0
        self.TrRatio = 0.0
        self.EntryDateStr = ""
        self.EntryDate = None
        self.ExitDateStr = ""
        self.days_in_trade = 0

    def create_directories(self):
        main_directory = "PairsResults" + self.params

        if not os.path.exists(main_directory):
            os.makedirs(main_directory)
        if not os.path.exists(self.directory_pair):
            os.makedirs(self.directory_pair)

    def collect_data(self, ii, position, pos1, pos2, tr_ratio, stock1_p, stock2_p):
        self.daily_data.append(
            self.merged_data.index[ii].strftime('%Y%m%d') + "," +
            position + "," + self.stock_1 + "," +
            self.stock_2 + "," + repr(self.zscore[ii - 1]) + "," +
            repr(pos1) + "," + repr(pos2) + "," + repr(tr_ratio) + "," +
            repr(stock1_p) + "," + repr(stock2_p) + "," +
            repr(self.days_in_trade) + "," + repr(self.ProfitLoss)
        )

    def write_all_data(self):
        self.write_file()
        self.write_trade_master()

    def write_file(self):
        with open((self.directory_pair + "/{0}.txt".format(self.TrdID)), "w") as ff:
            for item in self.daily_data:
                item = "".join(item) + "\n"
                ff.write(item)
        ff.close()

    def write_trade_master(self):
        main_file = "PairsResults" + self.params + "/MasterResults.txt"
        if len(self.ProfitLossHistory) > 0:
            trd_mean = sum(self.ProfitLossHistory) / len(self.ProfitLossHistory)
            max_day = max(self.ProfitLossHistory)
            min_day = min(self.ProfitLossHistory)
        else:
            trd_mean, max_day, min_day = [0.0, 0.0, 0.0]
        trade_details = [self.TrdID, self.EntryDateStr, self.position, self.stock_1, self.stock_2,
                         repr(self.pos1), repr(self.pos2), repr(self.OrigTradeRatio), self.ExitDateStr,
                         repr(trd_mean), repr(max_day), repr(min_day),
                         repr(self.days_in_trade), repr(self.TradeProfitLoss)]
        item = ",".join(trade_details) + "\n"
        with open(main_file, "a") as ff:
            ff.write(item)
        ff.close()

    def calc_day_PnL(self):
        self.ProfitLoss = ((self.pCurrentStock1 - self.pYestStock1) * self.pos1) + ((self.pCurrentStock2 - self.pYestStock2) * self.pos2)
        self.TradeProfitLoss += self.ProfitLoss
        self.ProfitLossHistory.append(self.ProfitLoss)

    def reset_trade(self):
        self.daily_data = []
        self.daily_data.append(
            "Date,Position,Ticker1,Ticker2,ZScore,Ticker1_Shares,Ticker2_Shares,Ratio,Ticker1_P,Ticker2_P,Days,PnL")
        self.ProfitLossHistory = []
        self.TradeProfitLoss = 0.0
        self.ProfitLoss = 0.0
        self.pos1 = 0.0
        self.pos2 = 0.0
        self.short_pos = False
        self.long_pos = False
        self.position = ""
        self.OrigTradeRatio = 0.0
        self.TrdID = ""
        self.days_in_trade = 0

    def set_new_trade(self, ii, EntryD):
        if self.position == "Long":
            self.pos1 = self.initial_capital / self.merged_data[self.stock_1][ii]
        else:
            self.pos1 = self.initial_capital / self.merged_data[self.stock_1][ii] * -1.0
        self.OrigTradeRatio = self.TrRatio
        self.pos2 = self.pos1 * self.ratios[ii - 1] * -1.0
        self.pEntryStock1 = self.pCurrentStock1
        self.pEntryStock2 = self.pCurrentStock2
        self.ProfitLoss = 0.0
        self.EntryDateStr = EntryD.strftime('%Y%m%d')
        self.EntryDate = EntryD
        self.TrdID = self.EntryDateStr + "_" + self.position + "{0}{1}".format(self.stock_1, self.stock_2)

    def backtest(self):
        for ii in range(1, len(self.ratios)):
            self.pYestStock1 = self.merged_data[self.stock_1][ii - 1]
            self.pYestStock2 = self.merged_data[self.stock_2][ii - 1]
            self.pCurrentStock1 = self.merged_data[self.stock_1][ii]
            self.pCurrentStock2 = self.merged_data[self.stock_2][ii]
            self.TrRatio = self.ratios[ii - 1]

            current_date = self.merged_data.index[ii]
            current_z_score = self.zscore[ii - 1]

            if current_z_score < -self.z_upper_limit and not self.long_pos:
                self.position = "Long"
                self.long_pos = True
                self.set_new_trade(ii, current_date)
                self.collect_data(ii, self.position, self.pos1, self.pos2, self.TrRatio, self.pEntryStock1, self.pEntryStock2)

            if current_z_score > -self.z_lower_limit and self.long_pos:
                self.days_in_trade += 1
                self.pExitStock1 = self.pCurrentStock1
                self.pExitStock2 = self.pCurrentStock2
                self.calc_day_PnL()
                self.collect_data(ii, self.position, self.pos1, self.pos2, self.TrRatio, self.pExitStock1, self.pExitStock2)
                self.ExitDateStr = current_date.strftime('%Y%m%d')
                self.write_all_data()
                self.reset_trade()

            if current_z_score > self.z_upper_limit and not self.short_pos:
                self.position = "Short"
                self.short_pos = True
                self.set_new_trade(ii, current_date)
                self.collect_data(ii, self.position, self.pos1, self.pos2, self.TrRatio, self.pEntryStock1, self.pEntryStock2)

            if current_z_score < self.z_lower_limit and self.short_pos:
                self.days_in_trade += 1
                self.pExitStock1 = self.pCurrentStock1
                self.pExitStock2 = self.pCurrentStock2
                self.calc_day_PnL()
                self.collect_data(ii, self.position, self.pos1, self.pos2, self.TrRatio, self.pExitStock1, self.pExitStock2)
                self.ExitDateStr = current_date.strftime('%Y%m%d')
                self.write_all_data()
                self.reset_trade()

            if ii == (len(self.ratios) - 1):
                if self.long_pos or self.short_pos:
                    self.days_in_trade += 1
                    self.calc_day_PnL()
                    self.pExitStock1 = self.merged_data[self.stock_1][ii]
                    self.pExitStock2 = self.merged_data[self.stock_2][ii]
                    self.ExitDateStr = current_date.strftime('%Y%m%d')
                    self.collect_data(ii, self.position, self.pos1, self.pos2, self.TrRatio, self.pExitStock1,
                                      self.pExitStock2)
                    self.write_all_data()
                else:
                    continue
                print("Finished trading for {0}".format(self.pair))

            if (self.long_pos or self.short_pos) and (current_date != self.EntryDate):
                self.days_in_trade += 1
                self.calc_day_PnL()
                self.collect_data(ii, self.position, self.pos1, self.pos2, self.TrRatio, self.pCurrentStock1,
                                  self.pCurrentStock2)