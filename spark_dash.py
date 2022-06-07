# sys
import dash
from dash import dcc, html 
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

@app.callback(Output("antsStats", "children"), 
        Input("antsStatsInterval", "n_intervals")) 
def update_dash_stats(n_intervals: int): 
    """ 
    Update the displayed stats in the Dash application; it is a text. 
    """ 
    # Identify the data in the data base 
    try: 
        stats = read_table("stats").toPandas().iloc[-1, :]
    except IndexError:
        # There are no instances in the data set currently 
        return list() 

    # Instantiate a string to display the appropriate quantities 
    info = """The current application contemplates 
    + {scenarios} scenario(s), 
    + {anthills} anthill(s), 
    + {ants} ants, with {antssf} searching foods, 
    + {foods_in_anthills} foods in anthills, 
    + {foods_in_deposit} foods in deposits, 
    + {foods_in_transit} foods in transit. 

Also, {avg_execution_time} was the average execution time, 
while {fst_scenario_id} was the 
shortest scenario, with 
{fst_scenario_time} iterations; 
{slw_scenario_id}, however, 
was the longest, with {slw_scenario_time} 
iterations needed. Each ant brings, 
in this sense, {avg_ant_food} units in average 
to their anthills, but the most 
voracious ant provided {max_ant_food} 
units to its own anthill.""".format( 
            scenarios=stats["scenarios"], 
            anthills=stats["anthills"], 
            ants=stats["ants"], 
            antssf=stats["ants_searching_food"], 
            foods_in_anthills=stats["foods_in_anthills"], 
            foods_in_deposit=stats["foods_in_deposit"], 
            foods_in_transit=stats["ants"] - stats["ants_searching_food"], 
            avg_execution_time=stats["avg_execution_time"], 
            fst_scenario_id=stats["fst_scenario_id"], 
            fst_scenario_time=stats["fst_scenario_time"], 
            slw_scenario_id=stats["slw_scenario_id"], 
            slw_scenario_time=stats["slw_scenario_time"], 
            avg_ant_food=stats["avg_ant_food"], 
            max_ant_food=stats["max_ant_food"] 
    ) 
    
    return html.Pre(info) 

@app.callback(Output("availableScenarios", "options"), 
        [Input("intervalSelect", "n_intervals")]) 
def update_buttons(n_intervals: int): 
    """ 
    Update the buttons. 
    """ 
    # Capture the anthills 
    scenarios = read_table("scenarios") \
            .select("scenario_id") \
            .distinct() \
            .collect() 

    # Return the buttons 
    return ["NULL"] + [identifier[0] for identifier in scenarios] 

@app.callback( 
        Output("magnify", "children"), 
        Input("availableScenarios", "value") 
) 
def update_scenario(value: str): 
    """ 
    Update the display of the current scenario. 
    """ 
    # Check if a scenario was chosen  
    if value == "NULL": 
        return list() 

    # If it was chosen, compute its data 
    data = read_table("scenarios") 
    curr_scenario = data \
        .filter(data["scenario_id"] == value) \
        .collect()[0] 
   
    # Identify the anthills in the current scenario 
    anthills = read_table("anthills") 
    anthills = anthills \
            .filter(anthills.scenario_id == value) 
    
    # Compute the quantity of anthills 
    quantity_anthills = anthills.count() 
    # Execution time 
    execution_time = curr_scenario[1] 

    # Identify the ants in this scenario 
    ants = read_table("ants") 
    
    ants = ants \
            .join(anthills, anthills.anthill_id == ants.anthill_id) 
           
    # Scenario's information 
    info = """This scenario contains 
    + {anthills} anthills, 
    + {ants} ants, 
and it was executed for {execution_time} iterations.
    """.format( 
            anthills=quantity_anthills,
            ants=ants.count(),
            execution_time=execution_time) 

    # Return the scenario's information 
    return html.Pre(info)  

if __name__ == '__main__':
    app.run_server(debug=True)
