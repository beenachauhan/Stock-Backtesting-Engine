from backtest.BackTestConfig import BackTestConfig
from db.db import db
from db.DBConfig import DBConfig

from backtest.PairFinder import PairFinder
from backtest.BackTest import BackTest

dbconfig = DBConfig('dbinfo.yaml')
db = db(dbconfig)
db.create_db()
pairs = {'Industrials': ['PEP', 'KO'], 'Tech': ['TWTR', 'GOOG', 'AMZN', 'FB'], 'Retail': ['WMT', 'TGT', 'LOW', 'HD']}
pf = PairFinder(db, 2004, 2019, 0.1, pairs)
cointegrated_pairs = pf.find_pairs()

# Backtest configuration.
backtest_config = BackTestConfig('backtestconfig.yaml')

backtest = BackTest(db, cointegrated_pairs, backtest_config)
backtest.run()