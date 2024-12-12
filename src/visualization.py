import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import pandas as pd

class WeatherDashboard:
    def __init__(self):
        self.app = Dash(__name__)
        self.setup_layout()

    def setup_layout(self):
        self.app.layout = html.Div([
            html.H1("Real-Time Weather Monitoring Dashboard"),
            
            html.Div([
                html.Div([
                    html.H3("Temperature Trends"),
                    dcc.Graph(id='temperature-graph')
                ], className='graph-container'),
                
                html.Div([
                    html.H3("Weather Parameters"),
                    dcc.Graph(id='weather-params-graph')
                ], className='graph-container'),
                
                html.Div([
                    html.H3("Wind Speed Analysis"),
                    dcc.Graph(id='wind-graph')
                ], className='graph-container')
            ]),
            
            dcc.Interval(
                id='interval-component',
                interval=300000,  # update every 5 minutes
                n_intervals=0
            )
        ])

    def create_temperature_plot(self, df):
        """
        Create temperature trend plot
        """
        fig = px.line(df, x='timestamp', y='temperature', color='city',
                     title='Temperature Trends Over Time')
        return fig

    def create_weather_parameters_plot(self, df):
        """
        Create weather parameters plot
        """
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(x=df['timestamp'], y=df['humidity'],
                                mode='lines', name='Humidity'))
        fig.add_trace(go.Scatter(x=df['timestamp'], y=df['pressure'],
                                mode='lines', name='Pressure'))
        
        fig.update_layout(title='Weather Parameters Over Time',
                         xaxis_title='Time',
                         yaxis_title='Value')
        return fig

    def create_wind_plot(self, df):
        """
        Create wind speed analysis plot
        """
        fig = px.scatter(df, x='timestamp', y='wind_speed', color='city',
                        size='wind_speed', title='Wind Speed Analysis')
        return fig

    def update_dashboard(self, df):
        """
        Update all dashboard components with new data
        """
        @self.app.callback(
            [Output('temperature-graph', 'figure'),
             Output('weather-params-graph', 'figure'),
             Output('wind-graph', 'figure')],
            [Input('interval-component', 'n_intervals')]
        )
        def update_graphs(_):
            temp_fig = self.create_temperature_plot(df)
            params_fig = self.create_weather_parameters_plot(df)
            wind_fig = self.create_wind_plot(df)
            return temp_fig, params_fig, wind_fig

    def run_server(self, debug=True):
        """
        Run the Dash server
        """
        self.app.run_server(debug=debug)
