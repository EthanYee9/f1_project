import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor, StackingRegressor
from sklearn.linear_model import LinearRegression
from src.csv_etl_script import create_connection

def f1_prediction(driver, circuit, grid, team, year):
    conn = create_connection()
    round = round_lookup(conn, circuit, year)
    driver_history_df, constructor_df, drivers_season_df = creating_df(conn, driver, circuit, round, grid, team, year)
    conn.close()
    ml_driver_results(driver_history_df, {"Driver id": 1, "Year":2020, "Circuit id": 1, "Starting position":1})

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
    constructor_df = pd.DataFrame(constructor_data, columns=["Team", "Team id","Year", "Points", "Constructor's position", "Wins", "finish position"])
    print(constructor_df)

    drivers_season_data = extract_driver_season_data(conn, round, year)
    drivers_season_df = pd.DataFrame(drivers_season_data, columns=["Name", "Driver id", "Year", "Points", "Position", "Wins", "Finishing position"])
    print(drivers_season_df)

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
    constructor_standings = conn.run(f"""
        SELECT dim_constructors.constructor_name, fact_constructor_standings.constructor_id, dim_races.year, fact_constructor_standings.points, fact_constructor_standings.position, fact_constructor_standings.wins, fact_race_results.finish_position
        FROM fact_constructor_standings
        JOIN dim_races ON fact_constructor_standings.race_id = dim_races.race_id 
        JOIN dim_constructors ON fact_constructor_standings.constructor_id = dim_constructors.constructor_id
        JOIN dim_circuits ON dim_races.circuit_id = dim_circuits.circuit_id 
        JOIN fact_race_results ON fact_race_results.race_id = dim_races.race_id AND fact_race_results.constructor_id = dim_constructors.constructor_id
        WHERE dim_races.year < '{year}' OR (dim_races.year = '{year}' AND dim_races.round < '{round}')
        GROUP BY dim_constructors.constructor_name, fact_constructor_standings.constructor_id, dim_races.year, fact_constructor_standings.points, fact_constructor_standings.position, fact_constructor_standings.wins, fact_race_results.finish_position;
    """) 
    return constructor_standings

def extract_driver_season_data(conn, round, year):
    driver_data = conn.run(f"""
        SELECT dim_drivers.full_name, fact_driver_standings.driver_id, dim_races.year, fact_driver_standings.points, fact_driver_standings.position, fact_driver_standings.wins, fact_race_results.finish_position
        FROM fact_driver_standings 
        JOIN dim_races ON fact_driver_standings.race_id = dim_races.race_id
        JOIN dim_drivers ON fact_driver_standings.driver_id = dim_drivers.driver_id
        JOIN fact_race_results ON fact_driver_standings.driver_id = dim_drivers.driver_id AND fact_race_results.race_id = dim_races.race_id
        WHERE dim_races.year < '{year}' OR (dim_races.year = '{year}' AND dim_races.round < '{round}')
        GROUP BY dim_drivers.full_name, fact_driver_standings.driver_id, dim_races.year, fact_driver_standings.points, fact_driver_standings.position, fact_driver_standings.wins, fact_race_results.finish_position;
    """)
    return driver_data

def ml_driver_results(driver_history_df, custom_input_dict=None):
    X = driver_history_df.drop(columns=["Name","Circuit", "Finishing position"])
    y = driver_history_df["Finishing position"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=9, test_size=0.3)
    model = RandomForestRegressor()
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)
    score = model.score(X_test, y_test)
    print(score)

    # displaying test predictions 
    results_df = X_test.copy()
    results_df["Actual"] = y_test
    results_df["Predicted"] = predictions.round()
    print(results_df.head(10))

    if custom_input_dict:
        input_df = pd.DataFrame([custom_input_dict])[X.columns]
        custom_prediction = model.predict(input_df)
        print("\nPredicted finishing position:", round(custom_prediction[0]))




f1_prediction("Lewis Hamilton", "Autódromo José Carlos Pace", None, "McLaren", 2022)