# sys
import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import pyspark 
from pyspark.sql import SparkSession 
import plotly.graph_objs as go
# importing"read_table" function from data.py file
from data import read_table


#####################################################################
# PYSPARK SQL CONNECTION
#####################################################################

# collecting the data
rdd = read_table("scenarios") 
# Use 'rdd' as a pandas dataframe  


#####################################################################
# PLOTLY DASH APP
#####################################################################
app_name = 'dash-sparksqlExample'
app = dash.Dash(__name__)
app.title = 'TAESB Dash with Spark'

# Generating any plot
# TODO: fill the following 'x' and 'y' parameters with the desired columns of the dataframe
trace = go.Bar(x=<rdd.column>, y=<rdd.column>, name='Some plot')
 

# Creating basic definition of the app layout
# The app layout is highly customizable
app.layout = html.Div(children=[html.H1("TAESB Dash with Spark", style={'textAlign': 'center'}),
    dcc.Graph(
    id='example-graph',
    figure={
    'data': [trace],
    'layout':
    go.Layout(title='Spark Data', barmode='stack')
    })
    ], className="container")
 
if __name__ == '__main__':
    app.run_server(debug=True)