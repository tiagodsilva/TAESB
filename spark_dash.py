# sys
import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import pyspark 
from pyspark.sql import SparkSession 
import plotly.graph_objs as go
# importing"read_table" function from data.py file
from app import read_table


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
app = dash.Dash(__name__, external_stylesheets=["https://fonts.googleapis.com/css?family=Source+Sans+Pro|Roboto+Slab"])
app.title = 'TAESB Dash with Spark'

# Generating any plot
# TODO: fill the following 'x' and 'y' parameters with the desired columns of the dataframe
data_frame = rdd.toPandas() 
trace = go.Bar(x=data_frame["scenario_id"], y=data_frame["execution_time"], name='Some plot')
 

# Here we are creating the first plot, which will be located at left 
plot_1 = html.Div( className="first-plot",
    children=[
        dcc.Graph(
        id='example-graph',
        figure={
        'data': [trace],
        'layout':
        go.Layout(title='Spark Data', barmode='stack')
        })
    ])

# Here we are creating the second 'plot', which will be located at right
# TODO: create the second visualization :)
plot_2 = html.Div(className='second-plot',
        children=html.H1("Second plot here"))

# Preparing the app layout
layout = html.Div(className="layout",
    children=[
        plot_1,
        plot_2
    ])

# Adding to the app the layout previously created
# The app layout is highly customizable 
app.layout = html.Div(
    id="main-div",
    children= [
        html.H1("TAESB Dash with Spark", className = "page-header", style={'textAlign': 'center'}),# page header
        layout],

)
 
if __name__ == '__main__':
    app.run_server(debug=True)
