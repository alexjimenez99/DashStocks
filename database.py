import mysql.connector
from datetime import datetime
from sqlalchemy import create_engine, text, Column, Integer, String,ForeignKey, UniqueConstraint, Table, MetaData, DateTime, Float, Index

from sqlalchemy.orm import declarative_base, sessionmaker
# from sqlalchemy.sql import text

# Create the database if it doesn't exist
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.exc import NoResultFound

import warnings
import pandas as pd

from variables import earnings_columns

# Create a declarative base class
Base   = declarative_base()

# Define a table using a class
class Stocks(Base):
    __tablename__  = 'companies'
    id             = Column(Integer, primary_key=True)  # Common to use 'id' as the primary key name
    company_name   = Column(String(50), unique=True, nullable=False)
    stocks         = relationship("StockData", back_populates="company")

class Interval(Base):
    __tablename__ = 'intervals'
    id            = Column(Integer, primary_key=True)  # Using 'id' consistently as primary key
    duration      = Column(String(20), nullable=False, unique=True)
    stocks   = relationship("StockData", back_populates="interval")

class StockData(Base):
    __tablename__ = 'stock_data'
    id          = Column(Integer, primary_key=True)
    company_id  = Column(Integer, ForeignKey('companies.id'))
    interval_id = Column(Integer, ForeignKey('intervals.id'))
    time_stamp  = Column(DateTime, nullable=False)
    open_price  = Column(Float, nullable=False)
    high_price  = Column(Float, nullable=False)
    low_price   = Column(Float, nullable=False)
    close_price = Column(Float, nullable=False)
    volume      = Column(Float, nullable=False)
    company     = relationship("Stocks", back_populates="stocks")
    interval    = relationship("Interval", back_populates="stocks")

class News(Base):
	__tablename__ = 'stock_news'
	id              = Column(Integer, primary_key = True, autoincrement = True)
	company_name    = Column(String(15), nullable = False)
	title           = Column(String(200), nullable = False)
	publisher       = Column(String(50), nullable = False)
	link            = Column(String(200), nullable = True)
	publish_time    = Column(Integer, nullable = True)
	story_type      = Column(String(20), nullable = True)
	related_tickers = Column(String(100), nullable = True)
	


# class Financials(Base):
# 	__tablename__ = 'financials'



	# relationships point to one another in different tables
	# Foreignkeys contain table_name.field_name

class Database:

	def __init__(self,
			    host     = 'finance-dash.csufivv7vfmp.us-east-1.rds.amazonaws.com',
				username = 'admin',
				password = 'finance-dash',
				database = 'finance-dash',
				socket   = None):

		self.host     = host
		self.username = username
		self.password = password
		self.database = database
		self.socket   = socket
		
		# Create Tables
		Base.metadata.create_all(self._create_engine())

	def _create_connection(self, host, username, password):
		
		conn = mysql.connector.connect(
            host=host,
            user=username,
            password=password
        	)
	
		cursor = conn.cursor()

		return conn, cursor
	
	def _create_engine(self):
		if self.socket is not None:
			database_url = f'mysql+mysqldb://{self.username}:{self.password}@{self.host}/{self.database}?unix_socket={self.socket}'

		else:
			database_url = f'mysql+mysqldb://{self.username}:{self.password}@{self.host}/{self.database}'

		return create_engine(database_url, echo = False)
		# return create_engine(f'mysql+mysqlconnector://{self.username}:{self.password}@{self.host}/{self.database}')
	
	def _create_session(self, engine):
		return sessionmaker(bind=engine)()

	def create_database(self, database):
		
		conn, cursor = self._create_connection(self.host, self.username, self.password, self.database, self.socket)

		try:
			cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database}`")
			cursor.close()
			conn.close()
		except mysql.connector.Error as err:
			print(f"Error: {err}")

		return 

# engine = create_database(host, username, password, database)

	def _add_stock_prices(self, data, engine):

		company_names = data[list(data.keys())[0]].keys()
		intervals     = data.keys()
		for company_name in company_names:
			# Pull Any interval to get company names
			session    = self._create_session(engine)
			# necessary to commit so company has an id attribute
			company    = Stocks(company_name = company_name)
			session.add(company)  
			session.commit() 

			for interval in intervals:
				try:
					interval_id = session.query(Interval).filter_by(duration=interval).one()
				except NoResultFound:
					interval_id = Interval(duration = interval)
					session.add(interval_id)
					session.commit()

				company_data  = data[interval][company_name]
				company_cols  = company_data.columns

				time_stamps   = company_data[company_cols[0]].values
				time_stamps   = [pd.to_datetime(ts).to_pydatetime() for ts in time_stamps]
				time_stamps   = [dt.strftime('%Y-%m-%d %H:%M:%S') for dt in time_stamps]

				opening_prices = company_data[company_cols[1]].values
				high_prices    = company_data[company_cols[2]].values
				low_prices     = company_data[company_cols[3]].values
				close_prices   = company_data[company_cols[4]].values
				volumes        = company_data[company_cols[5]].values

				for index in range(len(time_stamps)):
					time_stamp    = time_stamps[index]
					opening_price = opening_prices[index]
					high_price    = high_prices[index]
					low_price     = low_prices[index]
					close_price   = close_prices[index]

					volume        = volumes[index]

					stock_data    = StockData(company_id = company.id,
											interval_id = interval_id.id,
											time_stamp  = time_stamp,
											open_price  = opening_price, 
											high_price  = high_price, 
											low_price   = low_price, 
											close_price = close_price, 
											volume      = volume)
				
					session.add(stock_data)
			session.commit()
			session.close()

	def _dynamic_earnings(self, data):
		'''
		Earnings has a lot of columns so create it in a loop
		'''
		
		attributes = {'__tablename__': 'earnings', 'id': Column(Integer, primary_key=True, autoincrement = True),'company': Column(String(10))}
		new_columns = []
		for key in data.keys():
			for column in data[key].columns:
				try:
					column_name = column.replace(' ', '_').replace('&', 'and').lower()
				except AttributeError:
					print(column)
					column_name = str(column)
				if column == 'Date' and column_name not in attributes:
					attributes[column_name] = Column(DateTime, nullable = False)
					new_columns.append(column_name)
				elif column != 'Date' and column_name not in attributes:
					attributes[column_name] = Column(Float, nullable = True)  # 
					new_columns.append(column_name)
	

		return type('Earnings', (Base,), attributes)
	
	def _harmonize_columns(self, columns):
		new_columns = []
		for column in columns:
			try:
				column_name = column.replace(' ', '_').replace('&', 'and').lower()
			except AttributeError:
				print(column)
				column_name  = str(column)
			new_columns.append(column_name)

		return new_columns
		
	
	def _add_other_data(self, news, earnings, financials, engine):
		'''
		Adds all data besides stockdata since there are no intervals for this data
		'''

		Earnings      = self._dynamic_earnings(earnings)
		Base.metadata.create_all(engine)
		for key in news.keys():
			session   = self._create_session(engine)
			news_item = news[key]
			news_cols = news_item.columns
			earning   = earnings[key]
			financial = financials[key]

			# print(news_item)
			for index in news_item.index:
				
				# print(news_item)
				# print(news_item[news_cols[0]])
				tickers    = ','.join(news_item[news_cols[5]][index])
			
				news_story = News(company_name = key,
							title           = news_item[news_cols[0]][index],
							publisher       = news_item[news_cols[1]][index],
							link            = news_item[news_cols[2]][index],
							publish_time    = news_item[news_cols[3]][index],
							story_type      = news_item[news_cols[4]][index],
							related_tickers = tickers)
				
				session.add(news_story)

			session.commit()
			
			# get lowercased and SQLf friendly columns
		
			

			for key in earnings:			
				earnings[key].columns = self._harmonize_columns(earnings[key].columns)
				# earnings[key]   = earnings[key].transpose()
				earnings[key]   = earnings[key].where(pd.notnull(earnings[key]), None)
				earnings[key]['company'] = [key] * len(earnings[key].index)
				
				for index in earnings[key].index:
					row    = earnings[key].loc[index]
					try:
						earnings_record     = Earnings(**row.to_dict())
					except TypeError:
						print(key, row.to_dict())

					session.add(earnings_record)
				
			session.commit()
			session.close()

			


	def create_stock_data(self, data, news, earnings, financials):
		# Create a configured "Session" class
		engine      = self._create_engine()
		self._add_stock_prices(data, engine)
		self._add_other_data(news, earnings, financials, engine)

		return 

	def insert_stock_prices(self, data):
		'''
		Parameters:
		- data

		Returns:
		- 

		'''
		engine  = self._create_engine()
		# Session = sessionmaker(bind=engine)
		session = self._create_session(engine)
		# # Create a session
		# session = Session()

		# Left off here making sure my table is compatible
		# With the table schema for my database 
		if isinstance(data, dict):
			for key in data.keys():
				company_data = data[key]
				company_data.to_sql('lifetime_stocks', con=engine, if_exists='append', index=False)

		return 

	def show_tables(self, show_rows = False):
		engine  = self._create_engine()
		with engine.connect() as connection:
			result = connection.execute(text("SHOW TABLES"))
			for row in result:
				table_name = row[0]
				if show_rows:
					rows = connection.execute(text(f'SELECT * FROM {table_name}'))

					for r in rows:
						print(r)