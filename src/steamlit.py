import streamlit as st
from src.csv_etl_script import create_connection

st.title("F1 Predictor")

conn = create_connection()

list_of_drivers = [driver[0] for driver in conn.run("SELECT full_name FROM dim_drivers;")]
driver = st.selectbox("Select a driver", list_of_drivers)

list_of_circuits = [circuit[0] for circuit in conn.run("SELECT circuit_name FROM dim_circuits;")]
circuit = st.selectbox("Select a circuit", list_of_circuits)

list_of_years = [row[0] for row in conn.run("SELECT DISTINCT year FROM dim_races ORDER BY year DESC;")]
year = st.selectbox("Select the Year", list_of_years)

list_of_teams = [row[0] for row in conn.run("SELECT constructor_name FROM dim_constructors;")]
team = st.selectbox("Select a team", list_of_teams)

starting_position = st.number_input("Starting Grid Position", min_value=1)
driver_points = st.number_input("Driver Points", min_value=0)
driver_ranking = st.number_input("Driver Ranking", min_value=1)
driver_wins = st.number_input("Driver Wins", min_value=0)
team_points = st.number_input("Team Points", min_value=0)
team_ranking = st.number_input("Team Ranking", min_value=1)
team_wins = st.number_input("Team Wins", min_value=0)