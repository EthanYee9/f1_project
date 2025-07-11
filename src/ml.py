import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from src.csv_etl_script import create_connection

def f1_prediction(driver, circuit, grid, team, year):
    conn = create_connection()
    round = round_lookup(conn, circuit, year)
    driver_history_df, constructor_df, drivers_season_df = creating_df(conn, driver, circuit, round, grid, team, year)
    conn.close()
    ml_driver_results(driver_history_df,  constructor_df, drivers_season_df, {"Driver id": 1, "Year":2020, "Circuit id": 1, "Starting position":1})

def round_lookup(conn, circuit, year):
    round_number = conn.run(f"""
        SELECT dim_races.round FROM dim_races
        JOIN dim_circuits ON dim_races.circuit_id = dim_circuits.circuit_id
        WHERE dim_circuits.circuit_name = '{circuit}' and dim_races.year = '{year}';
    """)
    return round_number[0][0]

def creating_df(conn, driver, circuit, round, grid, team, year):
    driver_history_data = extract_result_data(conn, driver, year, round)
    driver_history_df = pd.DataFrame(driver_history_data, columns=["Name", "Driver id", "Year", "Circuit", "Circuit id","Starting position", "Finishing position"])
    print(driver_history_df)
    driver_history_df.to_csv("driver_history_data.csv", sep=',', index=False, na_rep='\\N')

    constructor_data = extract_car_data(conn, round, team, year)
    constructor_df = pd.DataFrame(constructor_data, columns=["Team", "Team id", "Race id", "Year", "Points", "Constructor's position", "Wins", "finish position"])
    print(constructor_df)
    constructor_df.to_csv("constructor_season_data.csv", sep=',', index=False, na_rep='\\N')

    drivers_season_data = extract_driver_season_data(conn, round, year)
    drivers_season_df = pd.DataFrame(drivers_season_data, columns=["Name", "Driver id", "Race id", "Year", "Points", "Position", "Wins", "Finishing position"])
    print(drivers_season_df)
    driver_history_df.to_csv("driver_season_data.csv", sep=',', index=False, na_rep='\\N')

    return driver_history_df, constructor_df, drivers_season_df

def extract_result_data(conn, driver, year, round):
    race_results = conn.run(f"""
        SELECT dim_drivers.full_name, fact_race_results.driver_id, dim_races.year, dim_circuits.circuit_name, dim_circuits.circuit_id, fact_race_results.starting_position, fact_race_results.finish_position
        FROM fact_race_results
        JOIN dim_drivers ON fact_race_results.driver_id = dim_drivers.driver_id
        JOIN dim_races ON fact_race_results.race_id = dim_races.race_id  
        JOIN dim_circuits ON dim_races.circuit_id = dim_circuits.circuit_id        
        WHERE dim_races.year < '{year}' AND fact_race_results.finish_position > 0 OR (dim_races.year = '{year}' AND dim_races.round < '{round}'); 
    """) 
    return race_results

def extract_car_data(conn, round, team, year):
    # constructor standings before the race 
    constructor_standings = conn.run(f"""
        WITH previous_standings AS (
            SELECT dim_constructors.constructor_name, fact_constructor_standings.constructor_id, dim_races.race_id, dim_races.year, dim_races.round, fact_constructor_standings.points, fact_constructor_standings.position, fact_constructor_standings.wins
            FROM fact_constructor_standings
            JOIN dim_races ON fact_constructor_standings.race_id = dim_races.race_id 
            JOIN dim_constructors ON fact_constructor_standings.constructor_id = dim_constructors.constructor_id
        ),
        
        current_results AS (
            SELECT fact_race_results.constructor_id, dim_races.race_id, dim_races.year, dim_races.round, fact_race_results.finish_position
            FROM fact_race_results
            JOIN dim_races ON fact_race_results.race_id = dim_races.race_id   
        )
                        
        SELECT ps.constructor_name, ps.constructor_id, cr.race_id, ps.year, ps.points, ps.position, ps.wins, cr.finish_position
        FROM previous_standings ps
        JOIN current_results cr ON ps.constructor_id = cr.constructor_id AND ps.year = cr.year AND ps.round + 1 = cr.round
        WHERE cr.finish_position > 0 AND (ps.year < '{year}' OR (ps.year = '{year}' AND ps.round < '{round}'));
        """) 
    return constructor_standings

def extract_driver_season_data(conn, round, year):
     # driver standings before the race 
    driver_data = conn.run(f"""
        WITH previous_standings AS (
            SELECT dim_drivers.full_name, ds.driver_id, dim_races.race_id, dim_races.year, dim_races.round, ds.points, ds.position, ds.wins
            FROM fact_driver_standings ds
            JOIN dim_races ON ds.race_id = dim_races.race_id
            JOIN dim_drivers ON ds.driver_id = dim_drivers.driver_id
        ), 
        current_results AS (
            SELECT rr.driver_id, dim_races.race_id, dim_races.year, dim_races.round, rr.finish_position
            FROM fact_race_results rr
            JOIN dim_races ON rr.race_id = dim_races.race_id   
        )                                 
                           
        SELECT ps.full_name, ps.driver_id, cr.race_id, ps.year, ps.points, ps.position, ps.wins, cr.finish_position
        FROM previous_standings ps
        JOIN current_results cr ON ps.driver_id = cr.driver_id AND ps.year = cr.year AND ps.round + 1 = cr.round
        WHERE cr.finish_position > 0 AND (ps.year < '{year}' OR (ps.year = '{year}' AND ps.round < '{round}'));
    """)
    return driver_data

def ml_driver_results(driver_history_df, df_constructor_season, df_driver_season, custom_input_dict=None):
    X = driver_history_df.drop(columns=["Name","Circuit", "Finishing position"])
    y = driver_history_df["Finishing position"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=9, test_size=0.3)
    model_driver_result = GradientBoostingRegressor()
    model_driver_result.fit(X_train, y_train)
    pred_1 = model_driver_result.predict(X_test)
    score = model_driver_result.score(X_test, y_test)
    print(score)

    # displaying test predictions 
    results_df = X_test.copy()
    results_df["Actual"] = y_test
    results_df["Predicted"] = pred_1.round()
    print(results_df.head(10))

    if custom_input_dict:
        input_df = pd.DataFrame([custom_input_dict])[X.columns]
        custom_prediction = model_driver_result.predict(input_df)
        print("\nPredicted finishing position:", round(custom_prediction[0]))

    X2 = df_constructor_season.drop(columns=["Team", "finish position"])
    y2 = df_constructor_season["finish position"]
    X2_train, X2_test, y2_train, y2_test = train_test_split(X2, y2, test_size=0.3, random_state=9)
    model_constructor = GradientBoostingRegressor()
    model_constructor.fit(X2_train, y2_train)
    pred_2 = model_constructor.predict(X2_test)
    score = model_constructor.score(X2_test, y2_test)
    print(score)
    # displaying test predictions 
    results_df = X2_test.copy()
    results_df["Actual"] = y2_test
    results_df["Predicted"] = pred_2.round()
    print(results_df.head(10))

    # Driver season model
    X3 = df_driver_season.drop(columns=["Name", "Finishing position"])
    y3 = df_driver_season["Finishing position"]
    X3_train, X3_test, y3_train, y3_test = train_test_split(X3, y3, test_size=0.3, random_state=9)
    model_driver_season = GradientBoostingRegressor()
    model_driver_season.fit(X3_train, y3_train)
    pred_3 = model_driver_season.predict(X3_test)
    score = model_driver_season.score(X3_test, y3_test)
    print(score)
    # displaying test predictions 
    results_df = X3_test.copy()
    results_df["Actual"] = y3_test
    results_df["Predicted"] = pred_3.round()
    print(results_df.head(10))

f1_prediction("Lewis Hamilton", "Autódromo José Carlos Pace", None, "McLaren", 2022)