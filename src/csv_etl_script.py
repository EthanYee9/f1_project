import pandas as pd
from pg8000.native import Connection 

def etl_csv():
    table_names = [
        "fact_race_results", "fact_constructor_standings", "fact_driver_standings",
        "dim_constructors", "dim_races", "dim_drivers", "dim_circuits"
    ]
    create_database()
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
            constructors_df = csv_to_df("data/constructors.csv")
            constructors_df = constructors_df.loc[:,["name", "nationality"]]
            constructors_df.rename(columns={"name": "constructor_name"}, inplace=True)
            return constructors_df
        case "dim_drivers":
            drivers_df = csv_to_df("data/drivers.csv")
            drivers_df = drivers_df.loc[:, ["forename", "surname", "number", "nationality", "dob"]]
            drivers_df.rename(columns={"number": "driver_number"}, inplace=True)
            drivers_df["full_name"] = drivers_df["forename"] + " " + drivers_df["surname"]
            drivers_df = drivers_df[["forename", "surname", "full_name", "driver_number", "nationality", "dob"]]
            return drivers_df
        case "dim_races":
            races_df = csv_to_df("data/races.csv")
            races_df = races_df.loc[:, ["circuitId", "year", "round", "date"]]
            races_df.rename(columns={"circuitId": "circuit_id"}, inplace=True)
            return races_df
        case "dim_circuits":
            circuits_df = csv_to_df("data/circuits.csv")
            circuits_df = circuits_df.loc[:, ["name", "location", "country"]]
            circuits_df.rename(columns={"name": "circuit_name"}, inplace=True)
            return circuits_df
        case "fact_race_results":
            race_results_df = csv_to_df("data/results.csv")
            race_results_df = race_results_df.loc[:, ["raceId", "driverId", "position", "grid", "points", "fastestLapTime", "constructorId"]]
            race_results_df.rename(columns={"raceId": "race_id", "driverId": "driver_id", "position": "finish_position", "grid": "starting_position", "fastestLapTime": "fastest_lap_time", "constructorId": "constructor_id"}, inplace=True)
            return race_results_df
        case "fact_driver_standings":
            driver_standings_df = csv_to_df("data/driver_standings.csv")
            driver_standings_df = driver_standings_df.loc[:, ["raceId", "driverId", "points", "position", "wins"]]
            driver_standings_df.rename(columns={"raceId": "race_id", "driverId": "driver_id"}, inplace=True)
            return driver_standings_df
        case "fact_constructor_standings":
            constructor_standings_df = csv_to_df("data/constructor_standings.csv")
            constructor_standings_df = constructor_standings_df.loc[:, ["raceId", "constructorId", "points", "position", "wins"]]
            constructor_standings_df.rename(columns={"raceId": "race_id", "constructorId": "constructor_id"}, inplace=True)
            return constructor_standings_df

def create_connection():
    user = "postgres"
    database = "destination_db"
    dbport = 5432
    password = "secret"
    return Connection(
        database=database, user=user, password=password, port=dbport
    )

def create_database():
    conn = create_connection()
    conn.run("""
        DROP DATABASE IF EXISTS f1_database;
        CREATE DATABASE f1_database;
    """) 
    conn.close()

def create_tables():
    conn = create_connection()
    conn.run("""
        CREATE TABLE dim_circuits (
            circuit_id INT SERIAL PRIMARY KEY, 
            circuit_name VARCHAR, 
            location VARCHAR, 
            country VARCHAR     
        );
        CREATE TABLE dim_constructors (
            constructor_id INT SERIAL PRIMARY KEY,
            name VARCHAR, 
            nationality VARCHAR
        );
        CREATE TABLE dim_drivers (
            driver_id INT PRIMARY KEY, 
            first_name VARCHAR, 
            last_name VARCHAR, 
            full_name VARCHAR, 
            driver_number INT, 
            nationality VARCHAR, 
            dob DATE
        );
        CREATE TABLE dim_races (
            race_id SERIAL PRIMARY KEY, 
            circuit_id INT REFERENCES dim_circuits(circuit_id), 
            year INT, 
            date DATE,
            round INT   
        );
        CREATE TABLE fact_race_results (
            result_id SERIAL PRIMARY KEY, 
            race_id INT REFERENCES dim_races(race_id),
            driver_id INT REFERENCES dim_drivers(driver_id), 
            finish_position INT, 
            starting_position INT, 
            points INT, 
            fastest_lap INT, 
            constructor_id INT
        );
        CREATE TABLE fact_constructors_standings (
            race_id INT REFERENCE dim_races(race_id), 
            constructor_id INT REFERENCES dim_constructors(constructor_id), 
            points INT, 
            position INT, 
            wins INT, 
            PRIMARY KEY (race_id, constructor_id)
        );
        CREATE TABLE fact_driver_standings (
            race_id INT REFERENCES dim_races(race_id),
            driver_id INT REFERENCES dim_drivers(driver_id), 
            points INT, 
            position INT, 
            wins INT,
            PRIMARY KEY (race_id, driver_id)
        )
    """) 
    conn.close()

def insert_into_warehouse(df, table_name):
    query = f"INSERT INTO {table_name} \n ("
    column_string = ', '.join(df.columns)
    query += column_string
    query += ") \n VALUES \n ("  
    for row in range(df.shape[0]):
        str_row_values = [str(value) for value in df.loc[row, :]]
        row_value = ','.join(str_row_values)
        query += f"{row_value},\n"
    query = query[:-2] + ");"

    conn = create_connection()
    conn.run(
        f"""{query}"""
    ) 

    conn.close()

# etl_csv()