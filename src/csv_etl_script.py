import pandas as pd
import time
from decimal import Decimal
from pg8000.native import Connection 
import os
print("Working directory:", os.getcwd())

def wait_for_connection(max_retries=5, delay=5):
    retries = 0 
    while retries < max_retries:
        try:
            conn = create_connection()
            conn.run("SELECT 1;")
            conn.close()
            print("connection established")
            return 
        except Exception as e:
            retries += 1
            print(f"error connecting to postgres: {e}")
            print(f"retrying in {delay} seconds ... (Attempt number {retries})")
            time.sleep(delay)
    print("Max retries reached. Exiting")
    exit(1)

def etl_csv():
    table_names = [
        "dim_circuits", "dim_constructors", "dim_races", "dim_drivers", 
        "fact_race_results", "fact_constructor_standings", "fact_driver_standings",
    ]
    
    create_tables()
    for table_name in table_names:
        transformed_df = transform_df(table_name)
        insert_into_warehouse(transformed_df, table_name)
    
  
def csv_to_df(file_path):
    df = pd.read_csv(file_path, na_values="\\N")
    return df

def transform_df(table_name):
    match table_name:
        case "dim_constructors":
            constructors_df = csv_to_df("data/corrected_data/corrected_constructors.csv")
            constructors_df = constructors_df.loc[:,["name", "nationality"]]
            constructors_df.rename(columns={"name": "constructor_name"}, inplace=True)
            return constructors_df
        case "dim_drivers":
            drivers_df = csv_to_df("data/corrected_data/corrected_driver.csv")
            drivers_df = drivers_df.loc[:, ["forename", "surname", "number", "nationality", "dob"]]
            drivers_df.rename(columns={"number": "driver_number"}, inplace=True)
            drivers_df["full_name"] = drivers_df["forename"] + " " + drivers_df["surname"]
            drivers_df = drivers_df[["forename", "surname", "full_name", "driver_number", "nationality", "dob"]]
            return drivers_df
        case "dim_races":
            races_df = csv_to_df("data/corrected_data/corrected_races.csv")
            races_df = races_df.loc[:, ["circuitId", "year", "round", "date"]]
            races_df.rename(columns={"circuitId": "circuit_id"}, inplace=True)
            print(races_df)
            return races_df
        case "dim_circuits":
            circuits_df = csv_to_df("data/src_data/circuits.csv")
            circuits_df = circuits_df.loc[:, ["name", "location", "country"]]
            circuits_df.rename(columns={"name": "circuit_name"}, inplace=True)
            return circuits_df
        case "fact_race_results":
            race_results_df = csv_to_df("data/corrected_data/corrected_results.csv")
            race_results_df = race_results_df.loc[:, ["raceId", "driverId", "position", "grid", "points", "fastestLapTime", "constructorId"]]
            race_results_df.rename(columns={"raceId": "race_id", "driverId": "driver_id", "position": "finish_position", "grid": "starting_position", "fastestLapTime": "fastest_lap_time", "constructorId": "constructor_id"}, inplace=True)

            # converting fastest lap time into seconds 
            fastest_lap_times = []
            for row, value in enumerate(race_results_df.loc[:,"fastest_lap_time"]):
                if pd.isna(value):
                    fastest_lap_times.append(0)
                    continue
                fastest_lap_in_seconds = Decimal(value[0]) * 60 + Decimal(value[2:])
                fastest_lap_times.append(float(fastest_lap_in_seconds))
            race_results_df["fastest_lap_time"] = fastest_lap_times

            finish_positions = []
            for index, value in enumerate(race_results_df.loc[:,"finish_position"]):
                if pd.isna(value):
                    finish_positions.append(0)
                    continue
                finish_positions.append(value)
            race_results_df["finish_position"] = finish_positions

            return race_results_df
        case "fact_driver_standings":
            driver_standings_df = csv_to_df("data/corrected_data/corrected_driver_standings.csv")
            driver_standings_df = driver_standings_df.loc[:, ["raceId", "driverId", "points", "position", "wins"]]
            driver_standings_df.rename(columns={"raceId": "race_id", "driverId": "driver_id"}, inplace=True)
            return driver_standings_df
        case "fact_constructor_standings":
            constructor_standings_df = csv_to_df("data/corrected_data/corrected_constructor_standings.csv")
            constructor_standings_df = constructor_standings_df.loc[:, ["raceId", "constructorId", "points", "position", "wins"]]
            constructor_standings_df.rename(columns={"raceId": "race_id", "constructorId": "constructor_id"}, inplace=True)
            return constructor_standings_df

def create_connection():
    database = "f1_database"
    host = "postgres"
    user = "postgres"
    dbport = 5432
    password = "secret"
    return Connection(
        database=database, host=host, user=user, password=password, port=dbport
    )

def create_tables():
    conn = create_connection()
    conn.run("""
        DROP TABLE IF EXISTS fact_race_results CASCADE;
        DROP TABLE IF EXISTS fact_constructor_standings CASCADE;
        DROP TABLE IF EXISTS fact_driver_standings CASCADE;
        DROP TABLE IF EXISTS dim_races CASCADE;
        DROP TABLE IF EXISTS dim_circuits CASCADE;
        DROP TABLE IF EXISTS dim_constructors CASCADE;
        DROP TABLE IF EXISTS dim_drivers CASCADE;       
        
        CREATE TABLE dim_circuits (
            circuit_id SERIAL PRIMARY KEY, 
            circuit_name VARCHAR, 
            location VARCHAR, 
            country VARCHAR     
        );
        CREATE TABLE dim_races (
            race_id SERIAL PRIMARY KEY, 
            circuit_id INT REFERENCES dim_circuits(circuit_id), 
            year INT, 
            date DATE,
            round INT   
        );
        
        CREATE TABLE dim_constructors (
            constructor_id SERIAL PRIMARY KEY,
            constructor_name VARCHAR, 
            nationality VARCHAR
        );
        
        CREATE TABLE dim_drivers (
            driver_id SERIAL PRIMARY KEY, 
            forename VARCHAR, 
            surname VARCHAR, 
            full_name VARCHAR, 
            driver_number INT, 
            nationality VARCHAR, 
            dob DATE
        );
        CREATE TABLE fact_race_results (
            result_id SERIAL PRIMARY KEY, 
            race_id INT REFERENCES dim_races(race_id),
            driver_id INT REFERENCES dim_drivers(driver_id), 
            finish_position INT, 
            starting_position INT, 
            points INT, 
            fastest_lap_time DOUBLE PRECISION, 
            constructor_id INT
        );
        CREATE TABLE fact_constructor_standings (
            constructor_standings_id SERIAL PRIMARY KEY,
            race_id INT REFERENCES dim_races(race_id), 
            constructor_id INT REFERENCES dim_constructors(constructor_id), 
            points INT, 
            position INT, 
            wins INT
        );
       
        CREATE TABLE fact_driver_standings (
            driver_standings_id SERIAL PRIMARY KEY,
            race_id INT REFERENCES dim_races(race_id),
            driver_id INT REFERENCES dim_drivers(driver_id), 
            points INT, 
            position INT, 
            wins INT
        );
    """) 
    conn.close()

def insert_into_warehouse(df, table_name):
    query = f"INSERT INTO {table_name} \n ("
    column_string = ', '.join(df.columns)
    query += column_string
    query += ") \n VALUES \n"  
    
    # if table_name == "fact_race_results":
    for row in range(df.shape[0]):
        row_value_list = []
        for value in df.loc[row, :]:
            if isinstance(value,str):
                value = value.replace("'", "''")
                row_value_list.append(f"'{value}'")
            if pd.isna(value):
                row_value_list.append("NULL")
            elif not isinstance(value, str):
                row_value_list.append(str(value))
        row_value = ', '.join(row_value_list)
        query += f"({row_value}),\n"
    query = query[:-2] + ";"
    # print(query)

    conn = create_connection()
    conn.run(query) 
    print("running")
    conn.close()

if __name__ == "__main__":
    wait_for_connection()
    etl_csv()
