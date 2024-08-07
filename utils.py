import pandas as pd
import yfinance as yf
from yahoo_fin import stock_info as si

# Self created packages
from database import Database

def pull_stock_data(news = False, earnings = False, financials = False):

	period_options   = ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max']

	interval_options = ['1m', '2m', '5m', '15m', '30m', '60m', '90m', '1h', '1d', '5d', '1wk', '1mo', '3mo']

	period_intervals = {'Day': ('1d', '15m'), 'Month':('1mo', '1d'), 'Year':('1y', '1d'),
						'5 Years':('5y', '1wk'), 'Max':('max', '1mo')}
	

	# Scrape the list of S&P 500 companies from Wikipedia
	url         = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
	sp500_table = pd.read_html(url, header=0)[0]

	# Extract the ticker symbols
	tickers     = sp500_table['Symbol'].tolist()

	def _clean_news(news):
		'''
		Turns news into dataframes and removes unnecessary columns
		'''
		for key in news.keys():
			news[key] = pd.DataFrame(news[key])
			news[key] = news[key].drop(columns = ['thumbnail', 'uuid'])

		return news
	
	def _clean_earnings(earnings):
		'''
		Restructure earnings for database entry
		'''

		for key in earnings.keys():
			earnings[key]         = earnings[key].transpose().reset_index()
			earnings[key].columns = [col if col!= 'index' else 'Date' for col in earnings[key].columns]
			
		return earnings
	
	def _clean_financials(financials):
		'''
		Clean financial records
		'''

		for key in financials.keys():
			financials[key]         = financials[key].transpose().reset_index()
			financials[key].columns = [col if col!= 'index' else 'Date' for col in financials[key].columns]

		return financials
	
	# Function to get stock data for multiple companies
	def get_multiple_stock_data(tickers, period='1y', interval='1d'):
		''' 
		Parameters:
			tickers: Capitalized Stock Code (AAPL, REGN)
			period: The length of time to pull the stocks
			interval: What frequency of data to pull

		Returns:
			data: Stock Price Data (Open, Close, Etc.)
			news: Stock News Data
			earnings: Earnings Table for a Stock 
			financials: Financial ratios of a company 
			
		'''
		data       = {}
		news       = {}
		earnings   = {}
		financials = {}
		for ticker in tickers:
			stock        = yf.Ticker(ticker)
			stock_news   = stock.news
			
			data[ticker] = stock.history(period=period, interval=interval)
			earning      = stock.balance_sheet
			financial    = stock.financials
			data[ticker] = data[ticker].reset_index()
			data[ticker].columns = [col if col != 'Datetime' else 'Date' for col in data[ticker].columns]

			news[ticker]       = stock_news
			earnings[ticker]   = earning
			financials[ticker] = financial
		
		return data, news, earnings, financials

	data_dictionary  = {}
	for key, pair_option in period_intervals.items():
		period_option        = pair_option[0]
		interval_option      = pair_option[1]
		data, news, earnings, financials = get_multiple_stock_data(tickers[:20], period = period_option, interval = interval_option)

		news                 = _clean_news(news)
		earnings             = _clean_earnings(earnings)
		financials           = _clean_financials(financials)
		data_dictionary[key] = data

	
        # self.news           = pd.DataFrame(news[list(earnings.keys())[0]])
        # self.data           = data_dict[list(data_dict.keys())[0]]
        # self.earnings       = earnings[list(earnings.keys())[0]]
        # self.financials     = financials[list(financials.keys())[0]]
        # self.default_col    = list(self.data.keys())[0]
        # self.periods        = list(self.data_dict.keys())

	host     = 'localhost'
	username = 'root'
	password = 'PASSWORD'
	database = 'plotly_stocks'
	socket   = None #'/tmp/mysql.sock'
	data     = None 

	db       = Database(host = host, username = username, password = password, database = database, socket = socket)
	db.create_stock_data(data_dictionary, news, earnings, financials)

	if news:
		pass

	if earnings:
		pass

	if financials:
		pass

	return


# pull_stock_data()