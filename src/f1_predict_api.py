from fastapi import FastAPI
from pydantic import BaseModel
from src.f1_predict import *

app = FastAPI()

class PredictionRequest(BaseModel):
    driver_name: str
    circuit_name: str
    year: int 
    team_name: str
    starting_position: int 
    driver_points: int
    driver_ranking: int
    driver_wins: int
    team_points: int
    team_ranking: int
    team_wins: int

@app.post("/predict")
def predict_f1_outcome(request: PredictionRequest):
    conn = create_connection()
    
    predict_dict = {
        "Driver id": driver_lookup(conn, request.driver_name),
        "Year": request.year,
        "Circuit id": circuit_lookup(conn, request.circuit_name),
        "Constructor id": constructor_lookup(conn, request.team_name),
        "Starting position": request.starting_position,
        "Driver points": request.driver_points,
        "Driver ranking": request.driver_ranking,
        "Driver wins": request.driver_wins,
        "Team points": request.team_points,
        "Team ranking": request.team_ranking,
        "Team wins": request.team_wins
    }

    outcome_prediction = f1_prediction(predict_dict)
    conn.close()
    return {"Predicted finishing position": outcome_prediction}