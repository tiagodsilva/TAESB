# sys
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output 
import pandas as pd

import warnings 
with warnings.catch_warnings(): 
    warnings.filterwarnings("ignore") 
    import pyspark 
    from pyspark.sql import SparkSession 
    from pyspark.sql import functions as F 

import plotly.graph_objs as go
# importing"read_table" function from data.py file
from app import read_table

#####################################################################
# PYSPARK SQL CONNECTION
#####################################################################

#####################################################################
# PLOTLY DASH APP
#####################################################################
app_name = 'dash-sparksqlExample'
app = dash.Dash(__name__, external_stylesheets=["https://fonts.googleapis.com/css?family=Source+Sans+Pro|Roboto+Slab"])
app.title = 'TAESB Dash with Spark'



# Here we are creating the first plot, which will be located at left 
plot_1 = html.Div(className="first-plot",
    children=[
        dcc.Graph(
            id="antsFood",
        ), 
        dcc.Interval( 
            id="intervalComponent", 
            interval=1e3, # milliseocnds, 
            n_intervals=0
        ) 
    ])

# Here we are creating the second 'plot', which will be located at right
# TODO: create the second visualization :)
plot_2 = html.Div(className='second-plot',
        children=html.H1("Second plot here", 
            id="antsStats"))

# Container for the buttons 
buttons = html.Div([ 
    html.Div( 
        id="containerButtons", 
    ), 
    dcc.Interval( 
        id="intervalButtons", 
        interval=5e3, 
        n_intervals=0 
    ) 
]) 

# Preparing the app layout
layout = html.Div(className="layout",
    children=[
        plot_1,
        plot_2, 
    ])

# Adding to the app the layout previously created
# The app layout is highly customizable 
app.layout = html.Div(
    id="main-div",
    children= [
        html.H1("TAESB Dash with Spark", className = "page-header", style={'textAlign': 'center'}),# page header
        html.Div( 
            children=[
                layout, 
                buttons
            ]
        ) 
        ],
    
)

@app.callback(Output("antsFood", "figure"), 
    Input("intervalComponent", "n_intervals")) 
def update_graph_live(n: int): 
    """ 
    Update the visualization lively. 
    """ 
    # collecting the data
    rdd = read_table("ants") 
    # Use 'rdd' as a pandas dataframe  

    # Generating any plot
    # TODO: fill the following 'x' and 'y' parameters with the desired columns of the dataframe
    data_frame = rdd.orderBy(F.desc("captured_food")).toPandas().iloc[:9, :] 
    trace = go.Bar(x=data_frame["ant_id"], y=data_frame["captured_food"], name='Some plot')
    
    # Return the figures' layout 
    return {
            'data': [trace],
            'layout':
            go.Layout(title='Spark Data', barmode='stack')
        }

@app.callback(Output("containerButtons", "children"), 
        Input("intervalButtons", "n_intervals")) 
def update_buttons(n_intervals: int): 
    """ 
    Update the buttons. 
    """ 
    # Capture the anthills 
    anthills = read_table("anthills") \
            .select("anthill_id") \
            .distinct() \
            .collect() 

    # Return the buttons 
    return [ 
            html.Button(identifier, 
                id=identifier[0] + "button") \
                    for identifier in anthills 
    ] 

if __name__ == '__main__':
    app.run_server(debug=True)
