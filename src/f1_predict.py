import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from src.csv_etl_script import create_connection

def f1_prediction(predict_dict):
    conn = create_connection()
    round = round_lookup(conn, predict_dict["Circuit id"], predict_dict["Year"])
    df = creating_df(conn, round, predict_dict["Year"])
    conn.close()
    outcome_prediction = ml_driver_results(df, predict_dict)
    return outcome_prediction

def round_lookup(conn, circuit_id, year):
    round_number = conn.run(f"""
        SELECT dim_races.round FROM dim_races
        JOIN dim_circuits ON dim_races.circuit_id = dim_circuits.circuit_id
        WHERE dim_circuits.circuit_id = '{circuit_id}' and dim_races.year = '{year}';
    """)
    return round_number[0][0]

def creating_df(conn, round, year):
    driver_history_data = extract_result_data(conn, year, round)
    driver_history_df = pd.DataFrame(driver_history_data, columns=["Name", "Driver id", "Race id", "Year", "Circuit", "Circuit id", "Constructor id", "Starting position", "Finishing position"])
    driver_history_df.to_csv("driver_history_data.csv", sep=',', index=False, na_rep='\\N')

    constructor_data = extract_car_data(conn, round, year)
    constructor_df = pd.DataFrame(constructor_data, columns=["Team", "Team id", "Race id", "Year", "Team points", "Team ranking", "Team wins", "Finishing position"])
    constructor_df.to_csv("constructor_season_data.csv", sep=',', index=False, na_rep='\\N')
    constructor_df =  constructor_df.drop(["Team", "Year", "Finishing position"], axis=1)

    drivers_season_data = extract_driver_season_data(conn, round, year)
    drivers_season_df = pd.DataFrame(drivers_season_data, columns=["Name", "Driver id", "Race id", "Year", "Driver points", "Driver ranking", "Driver wins", "Finishing position"])
    drivers_season_df.to_csv("driver_season_data.csv", sep=',', index=False, na_rep='\\N')
    drivers_season_df =  drivers_season_df.drop(["Name", "Year", "Finishing position"], axis=1)

    df = pd.merge(
        driver_history_df,
        drivers_season_df, 
        how = "left", 
        left_on=["Race id", "Driver id"],
        right_on=["Race id", "Driver id"]
    )
    df = pd.merge(
        df, 
        constructor_df, 
        how = "left", 
        left_on=["Race id", "Constructor id"],
        right_on=["Race id", "Team id"]
    )
    df =  df.drop("Team id", axis=1)
    # df.to_csv("merged_data.csv", sep=',', index=False, na_rep='\\N')

    return df

def extract_result_data(conn, year, round):
    race_results = conn.run(f"""
        SELECT dim_drivers.full_name, rr.driver_id, rr.race_id, dim_races.year, dim_circuits.circuit_name, dim_circuits.circuit_id, rr.constructor_id, rr.starting_position, rr.finish_position
        FROM fact_race_results rr
        JOIN dim_drivers ON rr.driver_id = dim_drivers.driver_id
        JOIN dim_races ON rr.race_id = dim_races.race_id  
        JOIN dim_circuits ON dim_races.circuit_id = dim_circuits.circuit_id        
        WHERE dim_races.year < '{year}' AND rr.finish_position > 0 OR (dim_races.year = '{year}' AND dim_races.round < '{round}'); 
    """) 
    return race_results

def extract_car_data(conn, round, year):
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

def ml_driver_results(df, custom_input_dict=None):
    X = df.drop(columns=["Name", "Race id", "Circuit", "Finishing position"])
    y = df["Finishing position"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=9, test_size=0.3)
    model_driver_result = RandomForestRegressor()
    model_driver_result.fit(X_train, y_train)
    pred_1 = model_driver_result.predict(X_test)
    score = model_driver_result.score(X_test, y_test)
    print(score)

    # # displaying test predictions 
    # results_df = X_test.copy()
    # results_df["Actual"] = y_test
    # results_df["Predicted"] = pred_1.round()
    # print(results_df.head(10))

    if custom_input_dict:
        input_df = pd.DataFrame([custom_input_dict])
        custom_prediction = model_driver_result.predict(input_df)
        # print("\nPredicted finishing position:", round(custom_prediction[0]))
        return round(custom_prediction[0])
    
def driver_lookup(conn, driver_name):
    driver_id = conn.run(f"""
        SELECT driver_id 
        FROM dim_drivers 
        WHERE full_name = '{driver_name}';
    """)
    return driver_id[0][0]

def circuit_lookup(conn, circuit_name):
    circuit_id = conn.run(f"""
        SELECT circuit_id 
        FROM dim_circuits 
        WHERE circuit_name = '{circuit_name}';
    """)
    return circuit_id[0][0]

def constructor_lookup(conn, team_name):
    constructor_id = conn.run(f"""
        SELECT constructor_id 
        FROM dim_constructors 
        WHERE constructor_name = '{team_name}';
    """)
    return constructor_id[0][0]
