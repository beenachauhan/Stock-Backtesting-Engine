import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from db.DBConfig import DBConfig
import datetime
import bs4
import requests
import yfinance as yf
import pandas as pd
import os


class db:
    def __init__(self, config: DBConfig):
        self.config = config
        self.MASTER_LIST_FAILED_SYMBOLS = []

        current_path = os.getcwd()
        self.db_created_file = current_path + '/' + config.db_file

    def create_db(self):
        if not self.db_exists():
            print('Creating new database.')
            conn = psycopg2.connect(
                host=self.config.db_host,
                database='postgres',
                user=self.config.db_user,
                password=self.config.db_password
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            cur.execute("CREATE DATABASE %s  ;" % self.config.db_name)
            cur.close()

        if os.path.isfile(self.db_created_file):
            print("Database already created and populated, moving to backtesting")
        else:
            print("Creating tables and populating database with s&p 500 data")
            try:
                self.create_tables()
                self.insert_snp_500_data_into_database()
            except:
                pass
            finally:
                with open(self.db_created_file, 'w') as fp:
                    pass

    def db_exists(self):
        try:
            print("try to connect")
            conn = self.get_connection()
            cur = conn.cursor()
            cur.close()
            print('Database exists.')
            return True
        except:
            print("Database does not exist.")
            return False

    def get_connection(self):
        conn = psycopg2.connect(
            host=self.config.db_host,
            database=self.config.db_name,
            user=self.config.db_user,
            password=self.config.db_password
        )
        return conn

    def create_tables(self):
        if self.db_exists():
            commands = (
                """
                CREATE TABLE exchange (
                    id SERIAL PRIMARY KEY,
                    abbrev TEXT NOT NULL,
                    name TEXT NOT NULL,
                    currency VARCHAR(64) NULL,
                    created_date TIMESTAMP NOT NULL,
                    last_updated_date TIMESTAMP NOT NULL
                    )
                """,
                """
                CREATE TABLE data_vendor (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL,
                    website_url VARCHAR(255) NULL,
                    created_date TIMESTAMP NOT NULL,
                    last_updated_date TIMESTAMP NOT NULL
                    )
                """,
                """
                CREATE TABLE symbol (
                    id SERIAL PRIMARY KEY,
                    exchange_id integer NULL,
                    ticker TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    name TEXT NOT NULL,
                    sector TEXT NOT NULL,
                    currency VARCHAR(64) NULL,
                    created_date TIMESTAMP NOT NULL,
                    last_updated_date TIMESTAMP NOT NULL,
                    FOREIGN KEY (exchange_id) REFERENCES exchange(id)
                    )
                """,
                """
                CREATE TABLE daily_data (
                    id SERIAL PRIMARY KEY,
                    data_vendor_id INTEGER NOT NULL,
                    stock_id INTEGER NOT NULL,
                    created_date TIMESTAMP NOT NULL,
                    last_updated_date TIMESTAMP NOT NULL,
                    date_price DATE,
                    open_price NUMERIC,
                    high_price NUMERIC,
                    low_price NUMERIC,
                    close_price NUMERIC,
                    adj_close_price NUMERIC,
                    volume BIGINT,
                    FOREIGN KEY (data_vendor_id) REFERENCES data_vendor(id),
                    FOREIGN KEY (stock_id) REFERENCES symbol(id)
                    )                    
                """)
            try:
                for command in commands:
                    print('Building tables.')
                    conn = self.get_connection()
                    cur = conn.cursor()
                    cur.execute(command)
                    conn.commit()
                    cur.close()
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
            finally:
                if conn:
                    cur.close()
                    conn.close()

    def _get_tickers_from_wiki(self):
        now = datetime.datetime.utcnow()
        response = requests.get("http://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
        response_text = response.text.encode('utf-8').decode('ascii', 'ignore')
        soup = bs4.BeautifulSoup(response_text)

        tickers_list = soup.select('table')[0].select('tr')[1:]
        tickers = []
        for i, symbol in enumerate(tickers_list):
            tds = symbol.select('td')
            tickers.append(
                (tds[0].select('a')[0].text, 'equity',
                 tds[1].select('a')[0].text,
                 tds[3].text, 'USD', now, now)
            )
        return tickers

    def _insert_tickers_to_database(self, tickers):
        conn = self.get_connection()

        column_str = """
                     ticker, instrument, name, sector, currency, created_date, last_updated_date
                     """
        insert_str = ("%s, " * 7)[:-2]
        final_str = "INSERT INTO symbol (%s) VALUES (%s)" % (column_str, insert_str)
        with conn:
            cur = conn.cursor()
            cur.executemany(final_str, tickers)
            conn.commit()

    def _load_sp500_stocks(self):
        self._insert_tickers_to_database(self._get_tickers_from_wiki())

    def _load_data_from_yahoo_finance(self, symbol, symbol_id, vendor_id):
        start_dt = datetime.datetime(2004, 12, 30)
        end_dt = datetime.datetime(2021, 5, 1)

        try:
            data = yf.download(symbol, start=start_dt, end=end_dt)
            print("download data for " + symbol)
        except:
            self.MASTER_LIST_FAILED_SYMBOLS.append(symbol)
            raise Exception('Failed to load {}'.format(symbol))

        data['Date'] = data.index
        columns_table_order = ['data_vendor_id', 'stock_id', 'created_date',
                               'last_updated_date', 'date_price', 'open_price',
                               'high_price', 'low_price', 'close_price',
                               'adj_close_price', 'volume']

        newDF = pd.DataFrame()
        newDF['date_price'] = data['Date']
        newDF['open_price'] = data['Open']
        newDF['high_price'] = data['High']
        newDF['low_price'] = data['Low']
        newDF['close_price'] = data['Close']
        newDF['adj_close_price'] = data['Adj Close']
        newDF['volume'] = data['Volume']
        newDF['stock_id'] = symbol_id
        newDF['data_vendor_id'] = vendor_id
        newDF['created_date'] = datetime.datetime.utcnow()
        newDF['last_updated_date'] = datetime.datetime.utcnow()
        newDF = newDF[columns_table_order]

        # ensure our data is sorted by date
        newDF = newDF.sort_values(by=['date_price'], ascending=True)

        # convert our dataframe to a list
        list_of_lists = newDF.values.tolist()
        # convert our list to a list of tuples
        tuples_mkt_data = [tuple(x) for x in list_of_lists]

        # WRITE DATA TO DB
        insert_query = """
                            INSERT INTO daily_data (data_vendor_id, stock_id, created_date,
                            last_updated_date, date_price, open_price, high_price, low_price, close_price, 
                            adj_close_price, volume) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """

        conn = self.get_connection()
        with conn:
            cur = conn.cursor()
            cur.executemany(insert_query, tuples_mkt_data)
            conn.commit()
            print('{} complete!'.format(symbol))

    def _fetch_vendor_id(self, vendor_name):
        conn = self.get_connection()
        with conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM data_vendor WHERE name = %s", (vendor_name,))
            # will return a list of tuples
            vendor_id = cur.fetchall()
        # index to our first tuple and our first value
        vendor_id = vendor_id[0][0]
        return vendor_id

    def _insert_new_vendor(self, vendor):
        todays_date = datetime.datetime.utcnow()
        conn = self.get_connection()
        with conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO data_vendor(name, created_date, last_updated_date) VALUES (%s, %s, %s)",
                (vendor, todays_date, todays_date)
            )
            conn.commit()

    def _get_all_tickers_from_db(self):
        conn = self.get_connection()
        with conn:
            cur = conn.cursor()
            cur.execute("SELECT id, ticker FROM symbol")
            data = cur.fetchall()
            return [(d[0], d[1]) for d in data]

    def _retrieve_prices(self):
        stock_data = self._get_all_tickers_from_db()
        vendor = 'Yahoo Finance'
        self._insert_new_vendor(vendor)
        vendor_id = self._fetch_vendor_id(vendor)

        for stock in stock_data:
            # download stock data and dump into daily_data table in our Postgres DB
            symbol_id = stock[0]
            symbol = stock[1]
            print('Currently loading {}'.format(symbol))
            try:
                self._load_data_from_yahoo_finance(symbol, symbol_id, vendor_id)
            except:
                continue
        file_to_write = open('failed_symbols.txt', 'w')
        for symbol in self.MASTER_LIST_FAILED_SYMBOLS:
            file_to_write.write("%s\n" % symbol)

    def insert_snp_500_data_into_database(self):
        self._load_sp500_stocks()
        self._retrieve_prices()

    def fetch_last_day(self, year):
        conn = self.get_connection()
        with conn:
            cur = conn.cursor()
            SQL = """
                        SELECT MAX(date_part('day', date_price)) FROM daily_data
                        WHERE date_price BETWEEN '%s-12-01' AND '%s-12-31'
                        """
            cur.execute(SQL, [year, year])
            data = cur.fetchall()
            cur.close()
            last_day = int(data[0][0])
            return last_day

    def fetch_last_day_for_month(self, year, month):
        conn = self.get_connection()
        with conn:
            cur = conn.cursor()
            SQL = """
                        SELECT MAX(date_part('day', date_price)) FROM daily_data
                        WHERE date_price BETWEEN '%s-%s-01' AND '%s-%s-30'
                        """
            cur.execute(SQL, [year, month, year, month])
            data = cur.fetchall()
            cur.close()
            last_day = int(data[0][0])
            return last_day

    def load_db_tickers_sectors(self, start_date):
        # convert start_date to string for our SQL query
        date_string = start_date.strftime("%Y-%m-%d")
        conn = self.get_connection()
        with conn:
            cur = conn.cursor()
            SQL = """
                    SELECT ticker, sector FROM symbol
                    WHERE id IN
                      (SELECT DISTINCT(stock_id) 
                       FROM daily_data
                       WHERE date_price = %s)
                    """
            cur.execute(SQL, (date_string,))
            data = cur.fetchall()
            return data

    def load_df_stock_data_array(self, stocks, start_date, end_date):
        array_pd_dfs = []
        conn = self.get_connection()
        with conn:
            cur = conn.cursor()
            SQL = """
                  SELECT date_price, adj_close_price 
                  FROM daily_data 
                  INNER JOIN symbol ON symbol.id = daily_data.stock_id 
                  WHERE symbol.ticker LIKE %s
                  """
            # for each ticker in our pair
            for ticker in stocks:
                # fetch our stock data from our Postgres DB
                cur.execute(SQL, (ticker,))
                results = cur.fetchall()
                # print(ticker, "********************************")
                # input("press key to continue")
                # print(results)
                # print("***************************************")
                # input("press key to continue ")
                # create a pandas dataframe of our results
                stock_data = pd.DataFrame(results, columns=['Date', ticker])
                # ensure our data is in order of date
                stock_data = stock_data.sort_values(by=['Date'], ascending=True)
                # convert our column to float
                stock_data[ticker] = stock_data[ticker].astype(float)
                # filter our column based on a date range
                mask = (stock_data['Date'] > start_date) & (stock_data['Date'] <= end_date)
                # rebuild our dataframe
                stock_data = stock_data.loc[mask]
                # re-index the data
                stock_data = stock_data.reset_index(drop=True)
                # append our df to our array
                array_pd_dfs.append(stock_data)

        return array_pd_dfs
