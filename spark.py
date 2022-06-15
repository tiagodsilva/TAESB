@app.task(priority=8) 
def update_stats(): 
    """ 
    Update the appropriate data in the analytical database. 
    """ 
    # print("Update the database, Luke!") 
    # Read the tables 
    tables_names = ["ants", "anthills", "foods", "scenarios"] 
        
    # And instantiate a container for the data frames 
    tables = { 
            dbtable:None for dbtable in tables_names 
    } 
    
    # Iterate across the tables 
    for table in tables: 
        tables[table] = app.read_table(table) 

    # print(tables) 
    
    # Compute the desired statistics 
    try: 
        scenarios = tables["scenarios"].count() 
        anthills = tables["anthills"].count() 

        # Quantity of foods available at the food deposits 
        foods_in_deposit = tables["foods"] \
                .agg(F.sum("current_volume")) \
                .collect()[0][0]

        # Quantity of ants alive 
        ants = tables["ants"].count()
        # Quantity of ants searching for food 
        ants_searching_food = tables["ants"] \
                .agg(F.sum("searching_food")) \
                .collect()[0][0]

        # Quantity of foods in transit 
        foods_in_transit = ants - ants_searching_food 
        # Quantity of foods in the anthills 
        foods_in_anthills = tables["anthills"] \
                .agg(F.sum("food_storage")) \
                .collect()[0][0]

        # Quantity of foods in total 
        total_foods = foods_in_deposit + foods_in_transit + foods_in_anthills

        # Active scenarios 
        active_scenarios = tables["scenarios"] \
            .filter(tables["scenarios"].active != 1) 
    
        avg_execution_time = active_scenarios.agg( 
                F.mean("execution_time")  
        ) \
        .collect()[0][0] 
        
        # Execution time in the scenarios 
        ord_scenarios = active_scenarios \
                .orderBy(F.desc("execution_time")) 
        
        fst_scenario = ord_scenarios \
                .take(1)[0] \
                .asDict() 

        slw_scenario = ord_scenarios \
                .tail(1)[0] \
                .asDict()  
        
        # print(fst_scenario, slw_scenario) 

        fst_scenario_id, fst_scenario_time = fst_scenario["scenario_id"], \
                fst_scenario["execution_time"] 
        slw_scenario_id, slw_scenario_time = slw_scenario["scenario_id"], \
                slw_scenario["execution_time"]  
        
        # Ant's food gathering 
        ants_foods = tables["ants"] \
                .agg(F.avg("captured_food"), F.max("captured_food")) \
                .collect()[0] 
        
        avg_ant_food, max_ant_food = ants_foods[0], ants_foods[1] 

        # Update the data base 
        app.update_stats(
                scenarios=scenarios, 
                anthills=anthills,
                ants_searching_food=ants_searching_food, 
                ants=ants, 
                foods_in_anthills=foods_in_anthills,
                foods_in_deposit=foods_in_deposit,
                avg_execution_time=avg_execution_time,
                fst_scenario_id=fst_scenario_id,
                fst_scenario_time=fst_scenario_time,
                slw_scenario_id=slw_scenario_id,
                slw_scenario_time=slw_scenario_time,
                avg_ant_food=avg_ant_food,
                max_ant_food=max_ant_food 
        )

        # print(total_foods) 
    except TypeError as err: 
        # There are no instances in the table 
        # print(err) 
        pass 
    except IndexError as err: 
        # There are no instances in the anthill tables 
        # print(err) 
        pass 
    

