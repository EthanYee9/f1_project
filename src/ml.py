import pandas as pd
from src.csv_etl_script import create_connection

def f1_prediction(driver, circuit, grid, team, year):
    conn = create_connection()
    round = finding_race_round(conn, circuit, year)
    creating_df(conn, driver, circuit, round, grid, team, year)
    conn.close()

def finding_race_round(conn, circuit, year):
    round_number = conn.run(f"""
        SELECT dim_races.round FROM dim_races
        JOIN dim_circuits ON dim_races.circuit_id = dim_circuits.circuit_id
        WHERE dim_circuits.circuit_name = '{circuit}' and dim_races.year = '{year}';
    """)
    return round_number[0][0]

def creating_df(conn, driver, circuit, round, grid, team, year):
    driver_history_data = extract_result_data(conn, driver, year, round)
    driver_history_df = pd.DataFrame(driver_history_data, columns=["Name", "Year", "Circuit", "Starting position", "Finishing position"])
    driver_history_df.to_csv("driver_history_data.csv", sep=',', index=False, na_rep='\\N')
    print(driver_history_df)

    constructor_data = extract_car_data(conn, round, team, year)
    constructor_df = pd.DataFrame(constructor_data, columns=["Team", "Circuit", "Year", "Points", "Constructor's position", "Wins", "finish position"])
    print(constructor_df)

    return driver_history_df

def extract_result_data(conn, driver, year, round):
    race_results = conn.run(f"""
        SELECT dim_drivers.full_name, dim_races.year, dim_circuits.circuit_name, fact_race_results.starting_position, fact_race_results.finish_position
        FROM fact_race_results
        JOIN dim_drivers ON fact_race_results.driver_id = dim_drivers.driver_id
        JOIN dim_races ON fact_race_results.race_id = dim_races.race_id  
        JOIN dim_circuits ON dim_races.circuit_id = dim_circuits.circuit_id        
        WHERE dim_drivers.full_name = '{driver}' AND (dim_races.year < '{year}' OR (dim_races.year = '{year}' AND dim_races.round < '{round}')); 
    """) 
    return race_results

def extract_car_data(conn, round, team, year):
    
    # need to add finishing positions to this dataframe as it is my Y data points 
    print(round -1)
    constructor_standings = conn.run(f"""
        SELECT dim_constructors.constructor_name, dim_circuits.circuit_name ,dim_races.year, fact_constructor_standings.points, fact_constructor_standings.position, fact_constructor_standings.wins, fact_race_results.finish_position
        FROM fact_constructor_standings
        JOIN dim_races ON fact_constructor_standings.race_id = dim_races.race_id 
        JOIN dim_constructors ON fact_constructor_standings.constructor_id = dim_constructors.constructor_id
        JOIN dim_circuits ON dim_races.circuit_id = dim_circuits.circuit_id 
        JOIN fact_race_results ON fact_race_results.race_id = dim_races.race_id AND fact_race_results.constructor_id = dim_constructors.constructor_id
        GROUP BY dim_constructors.constructor_name, dim_circuits.circuit_name, dim_races.year, fact_constructor_standings.points, fact_constructor_standings.position, fact_constructor_standings.wins, fact_race_results.finish_position;
    """) 
    return constructor_standings


f1_prediction("Lewis Hamilton", "Autódromo José Carlos Pace", None, "McLaren", 2022)