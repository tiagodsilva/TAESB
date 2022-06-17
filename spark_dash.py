# sys
import dash
from dash import dcc, html 
from dash.dependencies import Input, Output 
import pandas as pd

import plotly.graph_objs as go

import psycopg2 

# Capture credentials to access the data base 
from taesb.SparkConf import POSTGRESQL_HOST, \
        POSTGRESQL_USER, \
        POSTGRESQL_PASSWORD, \
        POSTGRESQL_DATABASE 
#####################################################################
# POSTGRESQL SQL CONNECTION
#####################################################################
db_conn = psycopg2.connect( 
        host=POSTGRESQL_HOST, 
        user=POSTGRESQL_USER, 
        password=POSTGRESQL_PASSWORD, 
        database=POSTGRESQL_DATABASE 
) 

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
        children=[ 
            html.Div("Second plot here", id="antsStats"), 
            dcc.Interval( 
                id="antsStatsInterval", 
                interval=1e3, 
                n_intervals=0 
            ) 
        ])

# Container for the buttons 

magnifier = html.Div(
    children=[ 
        html.Div( 
            id="scenarios", 
            children=[ 
            # Update the options periodically 
            html.Div([ 
                dcc.Dropdown( 
                    ["NULL"], 
                    "NULL", 
                    id="availableScenarios"
            ), 
                dcc.Interval( 
                    id="intervalSelect", 
                    interval=5e3, 
                    n_intervals=0 
            ) 
        ]), 
        html.Div( 
            id="magnify", 
        )
    ]) 
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
                magnifier
            ]
        ) 
        ],
    
)

def execute_query(query: str): 
    """ 
    Execute a query in the database. 
    """ 
    global db_conn 
    # Instantiate cursor 
    cursor = db_conn.cursor() 
    # Execute query 
    cursor.execute(query) 
    # Fetch instances 
    values = cursor.fetchall() 
    # Update cursor 
    cursor.close() 
    return values 

@app.callback(Output("availableScenarios", "options"), 
        [Input("intervalSelect", "n_intervals")]) 
def update_buttons(n_intervals: int): 
    """ 
    Update the buttons. 
    """ 
    # Capture the scnearios  
    query = "SELECT scenario_id FROM scenarios;" 
    scenarios = execute_query(query) 
    # Return the buttons 
    return ["NULL"] + [identifier[0] for identifier in scenarios] 

if __name__ == '__main__':
    app.run_server(debug=True)
