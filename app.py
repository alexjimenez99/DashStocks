import pandas as pd
import numpy  as np
import plotly 
import plotly.io as pio

import dash
from dash import Dash, html, dash_table, dcc, callback, Output, Input
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template

import plotly.express as px
import pandas as pd
import os

from utils import pull_stock_data
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import declarative_base, sessionmaker

from database import StockData, Interval, News

class App:
    
    # Styling Tips: https://towardsdatascience.com/3-easy-ways-to-make-your-dash-application-look-better-3e4cfefaf772
    def __init__(self, data_dict, news, earnings, financials, company_names):

        # Cyborg, Slate
        self.app            = dash.Dash(__name__, external_stylesheets = [dbc.themes.SLATE])
        load_figure_template('SLATE')
                             
        self.data_dict      = data_dict
        self.news_dict      = news
        self.earnings_dict  = earnings
        self.financial_dict = financials
        self.news_cols      = ['title', 'publisher']
        self.company_names  = company_names
        
        # Dictionaries for Companies
        self.company_dictionary = self.company_names.set_index('id')['company_name'].to_dict()
        self.id_dictionary      = self.company_names.set_index('company_name')['id'].to_dict()

        # self.data           = data_dict[list(data_dict.keys())[0]]
       
        
        if financials is not None:
            self.financials = financials[list(financials.keys())[0]]
    
        self.default_col    = 'interval_id'
        self.periods        = self.data_dict[self.default_col].unique()
        self.period_dict    = self._period_dict(self.periods)

        stock_columns       = self._stock_columns()
        stock_ids           = list(self.company_dictionary.keys())
        self.display_data   = self.data_dict[self.data_dict['company_id'] == 1]

        self.earnings       = earnings[earnings['company'] == 'A']
        self.news           = self.news_dict[self.news_dict['company_name'] == 'A']

        # self.news           = self.convert_type(self.news, ['thumbnail', 'link', 'related_tickers'])[self.news_cols]

        self.app_layout(self.display_data, stock_columns, company_names['company_name'])
        self.set_callbacks()
        self.run_server()
        

    def _period_dict(self, periods):
        period_dictionary = {'Daily': 1,
                             'Weekly': 2,
                             'Yearly': 3,
                             '5 Years': 4,
                             'All Time': 5}
        return period_dictionary
    
    def _stock_columns(self):
        stock_columns = ['open_price', 'high_price', 'low_price', 'close_price', 'volume']
        return stock_columns


    def update_stock_data(self):
        pull_stock_data(news = False, earnings = False, financials = False)

    def update_periods(self, data_dict):
        '''
        Update the default stock data periods
        '''
        time_periods = list(self.data_dict.keys())
        if time_periods != self.periods:
            periods = time_periods

        return periods

    def update_period(self, period):
        ''' 
        Update the data to have a different time period
        '''
        period = self.display_data[self.display_data['interval_id'] == period]
        return period

    def update_company(self, company):
       
        company = self.id_dictionary[company]
        self.data_dict[self.data_dict['company_id'] == company]
        return self.data_dict[self.data_dict['company_id'] == company]

    def get_companies(self, data):
        return list(data.keys())
    
    def run_server(self):
        self.app.run_server(debug=True)  

    def convert_type(self, x, col):
        if isinstance(x, list):
            for c in col:
                x[c] = x[c].astype(str)
        else:
            x[col] = x[col].astype(str)

        return x # Convert thumbnail to string if necessary
    
    def side_bar(self):
        # Define the sidebar
        sidebar = html.Div(
            [
                html.H4("Menu", className="display-4"),
                html.Hr(),
                dbc.Nav(
                    [
                        dbc.NavLink("Home", href="/", active="exact"),
                        dbc.NavLink("Stock Forecasting", href="/page-1", active="exact"),
                        dbc.NavLink("Stock Basics", href="/page-2", active="exact"),
                    ],
                    vertical=True,
                    pills=True,
                ),
            ],
            style={
                'gridArea': 'side-layout', 
                'position': 'fixed',
                'top': 0,
                'left': 0,
                'bottom': 0,
                'width': '250px',
                'padding': '2rem 1rem',
                'background-color': '#f8f9fa',
            },
        )

        return sidebar
    
    def main_layout(self, display_data, stock_columns, companies):

        #Header Element
        layout = html.Div([
        html.Div(children = [
            html.H1(children = 'Stock Monitoring Dashboard')
        ],  style={'gridArea': 'header', 'gridColumn': 'span 2', 'display': 'flex', 'justifyContent': 'center', 'alignItems': 'center'}),
        
        html.Br(),
        html.Div(children = [
            
            html.Label('Select Company'),
            dcc.Dropdown(
                id     ='company-dropdown',
                options=[{'label': company, 'value': company} for company in companies],
                value=companies[0] # Default Value Set
            ),

            html.Br(),
            html.Label('Stock Period'),
            dcc.Dropdown(
                id='period',
                options=[{'label': label, 'value': period} for label, period in self.period_dict.items()],
                value=self.periods[0] # Default Value Set
            ),

            html.Br(),
            html.Label('Select Data'),
            dcc.Dropdown(
                id='data-dropdown',
                options=[
                    {'label': category, 'value': category} for category in stock_columns[1:4] 
                ],
                value=stock_columns[1] # Default Value Set
            ),

            dcc.Graph(id='interactive-graph')
            
        ],  style={'gridArea': 'main-plot', 'gridColumn': 'span 2','justifyContent': 'center', 'alignItems': 'center'}),

        html.Br(),
        # Left Div Object
        html.Div(children=[
            html.H1(children='Stock Volume'),   
            dcc.Graph(id = 'stock-volume')

        ], style={'gridArea': 'first'}),

        
        # Second Div Object
        html.Div(([
            html.H1(children='Earnings'),
            dcc.Interval(id='interval-component', interval=1*1000, n_intervals=0),
            dash_table.DataTable(
                id           = 'earnings',
                columns      = [{"name": str(i), "id": str(i)} for i in self.earnings.columns],
                data         = self.earnings.to_dict('records'),
                # style_table  = {'overflowX': 'auto'},
                style_table={'height': '450px', 'width': '500px', 'overflowY': 'auto', 'overflowX': 'auto'},
                style_cell={
                    'textAlign': 'left',
                    'minWidth': '150px',
                    'width': '150px',
                    'maxWidth': '150px',
                    'overflow': 'hidden',
                    'textOverflow': 'ellipsis',
                    'whiteSpace': 'nowrap',
                    'backgroundColor': '#f0f0f0'  # Cell background color
        
        },
        style_header = {
                    'backgroundColor': 'rgb(22, 61, 96)',
                    'fontWeight': 'bold',
                    'color': '#f0f0f0'
                    },
        style_data={
            'backgroundColor': '#000000',  # Data background color
            'color': '#FFFFFF',  # Data text color
            
        },
            style_as_list_view=True,
            ),
            
            ]), style={'gridArea': 'second'}
            ),

            # html.A('Link to external site', href='https://www.example.com', target='_blank')
            html.Div([
                html.H1(children = 'Company News'),
                dash_table.DataTable(
                    id           = 'news-table',
                    columns      = [{'name': i, 'id': i} for i in self.news.columns],
                    data         = self.news.to_dict('records'),
                    style_table={'height': 'auto', 'width': 'auto', 'overflowY': 'auto', 'overflowX': 'auto'},
                    style_cell={
                        'textAlign': 'left',
                        'minWidth': '150px',
                        'width': '150px',
                        'maxWidth': '150px',
                        'overflow': 'hidden',
                        'textOverflow': 'ellipsis',
                        'whiteSpace': 'nowrap',
                        'backgroundColor': '#f0f0f0',  # Cell background color

                    },
                    style_header = {
                        'backgroundColor': 'rgb(22, 61, 96)',
                        'fontWeight': 'bold',
                        'color': '#f0f0f0'
                        },
                    style_as_list_view=True,
                    style_data={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'lineHeight': '15px',
                    'backgroundColor': '#000000',  # Data background color
                    'color': '#FFFFFF'  # Data text color
                    },
                )], style={'gridArea': 'news-grid', 'gridColumn': 'span 2','justifyContent': 'center', 'alignItems': 'center'})
                
            ], # Main Div Styling
            style={
                'display': 'grid',
                'gridArea': 'main-layout',
                'gridTemplateAreas': '''
                    "header header"
                    "main-plot main-plot"
                    "first second"
                    "news-grid news-grid"
                ''',
                'gridTemplateRows': 'auto auto 1fr',
                'gridTemplateColumns': '1fr 1fr',
                'gap': '10px',
                'padding': '10px'
                # 'backgroundColor': '#f0f0f0'
        })

        return layout

    
    # def main_layout(self, display_data, stock_columns, companies):
        

    def app_layout(self, display_data, stock_columns, companies):
        
        side_bar         = self.side_bar()
        main_layout      = self.main_layout(display_data, stock_columns, companies)
        self.app.layout  = html.Div([
            side_bar,
            main_layout
        ],
        style = {'display': 'grid',
                'gridTemplateAreas': '''
                    "side-layout main-layout"
                ''',
                'gridTemplateRows': 'auto',
                'gridTemplateColumns': '250px 1fr',
                'gap': '0px'}
                )
        
    
        # Callback to update the available data columns based on the selected company
    def set_callbacks(self):
         # Callback to update the graph based on the selected company and data column
        @self.app.callback(
            Output('interactive-graph', 'figure'),
            Input('company-dropdown', 'value'),
            Input('data-dropdown', 'value'),
            Input('period', 'value')
        )

        def update_graph(selected_company, selected_data, period):
       
            # company_key = next(key for key in self.data.keys() if selected_company in key)
            display_data = self.update_company(selected_company)
            display_data = display_data[display_data['interval_id'] == period]
            fig = px.line(display_data, x='time_stamp', y=selected_data, title=f'{selected_company} - {selected_data}', template='plotly_dark')
            return fig

        @self.app.callback(
            Output('stock-volume', 'figure'),
            Input('company-dropdown', 'value'),
            Input('period', 'value')
        )

        def update_volume(selected_company, period):

            # self.data    = self.update_period(period)
            display_data = self.update_company(selected_company)
            display_data = display_data[display_data['interval_id'] == period]
            stock_volume = display_data['volume']
            fig          = px.line(display_data, x= 'time_stamp', y = stock_volume, title = f'{selected_company} - Volume', template='plotly_dark')

            return fig

        @self.app.callback(
            Output('earnings', 'columns'),
            Output('earnings', 'data'),
            Input('company-dropdown', 'value'),
            # Input('interval-component', 'n_intervals')
        )

        def update_earnings(selected_company):
            earnings = self.earnings_dict[self.earnings_dict['company'] == selected_company]
            columns      = [{"name": str(i), "id": str(i)} for i in self.earnings.columns]
            earnings = earnings.to_dict('records')
      
            return columns, earnings
       

def _create_engine():
    username        = 'root'
    password        = 'Y5d7fp32!%40'
    host            = 'localhost'
    database        = 'plotly_stocks'
    return create_engine(f'mysql+mysqldb://{username}:{password}@{host}/{database}')

if __name__ == '__main__':
    engine          = _create_engine()
    session         = sessionmaker(bind=engine)()
    interval_id     = session.query(Interval)

    # SQL database commands
    data_dictionary = session.query(StockData)
    news            = session.query(News)
    earnings        = None #session.query(Earnings)

    metadata = MetaData()
    metadata.reflect(bind=engine)

    data       = {}
    for table_name in metadata.tables:
        
        # Get the table object
        table = metadata.tables[table_name]
        
        # Query the table and load the data into a DataFrame
        with engine.connect() as conn:
            df = pd.read_sql(table.select(), conn)
        # Query the table and load the data into a DataFrame

        data[table_name] = df
      
        

    # financials      = session.query(Financials)
    financials      = None
    App(data['stock_data'], data['stock_news'], data['earnings'], None, data['companies'])
    # App(data_dictionary, news, earnings, financials)
