""" 
Write visualizations for the benchmarks. 
""" 
# Sys 
import os 
import sys 

# Visualizations 
import seaborn as sns 
import pandas as pd 
import matplotlib.pyplot as plt 

# Docs 
from typing import List, Dict, Tuple  

sns.set_theme(style="whitegrid", palette="pastel") 

def line_plot(data: List[Dict], 
        x: str, 
        y: str, 
        x_label: str, 
        y_label: str, 
        filename: str=None): 
    """ 
    Draw a line plot and write it to the file `filename`. 
    """
    # Instantiate a data frame 
    dataframe = pd.DataFrame(data) 

    # and draw a figure 
    sns.lineplot(x=x, y=y, data=dataframe) 

    # Write visualization's labels
    plt.xlabel(x_label) 
    plt.ylabel(y_label) 

    if filename: 
        plt.savefig(filename) 

    # Display the visualization 
    plt.show() 


