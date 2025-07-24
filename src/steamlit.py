import streamlit as st
import requests
from src.csv_etl_script import create_connection

conn = create_connection()

st.title("F1 Predictor")
col_1, col_2 = st.columns(2)

with col_1:
    list_of_drivers = [driver[0] for driver in conn.run("SELECT full_name FROM dim_drivers;")]
    driver = st.selectbox("Select a driver", list_of_drivers)

    list_of_years = [row[0] for row in conn.run("SELECT DISTINCT year FROM dim_races ORDER BY year DESC;")]
    year = st.selectbox("Select the Year", list_of_years)

with col_2:
    list_of_circuits = [circuit[0] for circuit in conn.run("SELECT circuit_name FROM dim_circuits;")]
    circuit = st.selectbox("Select a circuit", list_of_circuits)

    list_of_teams = [row[0] for row in conn.run("SELECT constructor_name FROM dim_constructors;")]
    team = st.selectbox("Select a team", list_of_teams)

starting_position = st.number_input("Starting Grid Position", min_value=1)

col_3, col_4 = st.columns(2)


with col_3:
    driver_points = st.number_input("Driver Points", min_value=0)
    driver_ranking = st.number_input("Driver Ranking", min_value=1)
    driver_wins = st.number_input("Driver Wins", min_value=0)

with col_4:
    team_points = st.number_input("Team Points", min_value=0)
    team_ranking = st.number_input("Team Ranking", min_value=1)
    team_wins = st.number_input("Team Wins", min_value=0)

conn.close()

if st.button("Predict"):
    predict_dict = {
        "driver_name": driver,
        "circuit_name": circuit,
        "year": year,
        "team_name": team,
        "starting_position": starting_position,
        "driver_points": driver_points,
        "driver_ranking": driver_ranking,
        "driver_wins": driver_wins,
        "team_points": team_points,
        "team_ranking": team_ranking,
        "team_wins": team_wins
    }

    url = 'http://127.0.0.1:8000/predict'
    response = requests.post(url, json=predict_dict)
    print(response.status_code)
    outcome = response.json()

    st.header(f"Predicted finishing position: {outcome}")