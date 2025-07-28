import pandas as pd 
from src.f1_predict import driver_lookup
from src.csv_etl_script import create_connection

conn = create_connection()
def race_results_chart(conn, driver, date):
    driver_id = driver_lookup(conn, driver)
    start_date, end_date = date 
    data = conn.run(f"""
        SELECT rr.finish_position, dim_races.date
        FROM fact_race_results rr 
        JOIN dim_races ON rr.race_id = dim_races.race_id
        WHERE rr.driver_id = '{driver_id}' AND (dim_races.date BETWEEN '{start_date}' And '{end_date}');
    """)
    df = pd.DataFrame(data, columns=["Finish position", "Date"])
    return df

def qualifying_chart(conn, driver, date):
    driver_id = driver_lookup(conn, driver)
    start_date, end_date = date 
    data = conn.run(f"""
        SELECT rr.starting_position, dim_races.date
        FROM fact_race_results rr 
        JOIN dim_races ON rr.race_id = dim_races.race_id
        WHERE rr.driver_id = '{driver_id}' AND (dim_races.date BETWEEN '{start_date}' And '{end_date}');
    """)
    df = pd.DataFrame(data, columns=["Starting position", "Date"])
    return df
