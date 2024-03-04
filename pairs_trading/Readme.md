# Final year project - Pairs trading with cointegration
## Introduction
## Requirements

1. Python 3
2. PostgreSQL 11
3. Python Virtual environments
4. Python packages in requirements.txt

## Installation

1. Install Python 3
2. Install PostgreSQL
    1. Windows installation - https://www.postgresqltutorial.com/install-postgresql/
    2. Linux Installation - https://www.postgresqltutorial.com/install-postgresql-linux/
3. Use psql to create a new database and user on Postgres, with following commands
```buildoutcfg
$ sudo -u postgres psql
postgres=# create db securities_master;
postgres=# create user backtest with password '12345';
postgres=# GRANT ALL PRIVILEGES ON DATABASE securities_master to backtest;
```
4. Update `config/dbinfo.yaml` file with username and password.
5. Install requirements `pip3 install -r requirements.txt`


## Running the program

1. Run the main program with `python3 main.py`
2. Program would try to find cointegrated pairs from the given list in main. By default, the list is - 
   `{'Industrials': ['PEP', 'KO'], 'Tech': ['TWTR', 'GOOG', 'AMZN', 'FB'], 'Retail': ['WMT', 'TGT', 'LOW', 'HD']}`
3. The program would show the data for each of the sector, the heatmaps for stock's correlation and stock charts 
for each stock in the sector. The program pause here to allow viewing of the charts. Please close the charts 
after viewing to allow the program to go forward
4. The program then calculates the cointegrated pairs and start doing backtest. The backtest trading results
are written under a directory `PairsResults_5_30`.
5. Finally an analyzer is run on the result of the trading, and provides average and daily trading data as well
   as a chart showing the daily profit and loss with pairs trading strategy.