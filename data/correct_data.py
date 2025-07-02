import csv 
import pandas as pd
from src.csv_etl_script import csv_to_df

def correct_race_data():
    with open("data/src_data/races.csv", 'r') as file:
        reader = csv.DictReader(file, delimiter=",")
        
        corrected_data = []
        for index, row_dict in enumerate(reader):
            row_dict["raceId"] = index + 1 
            corrected_data.append(row_dict)
        
        keys = corrected_data[0].keys()

    with open("data/corrected_data/corrected_races.csv", "w", newline="") as file:
        writer = csv.DictWriter(file, keys)
        writer.writeheader()
        writer.writerows(corrected_data)

def correct_driver_data():
    with open("data/src_data/drivers.csv", 'r') as file:
        reader = csv.DictReader(file, delimiter=",")
        
        corrected_data = []
        for index, row_dict in enumerate(reader):
            row_dict["driverId"] = index + 1 
            corrected_data.append(row_dict)
        
        keys = corrected_data[0].keys()

    with open("data/corrected_data/corrected_driver.csv", "w", newline="") as file:
        writer = csv.DictWriter(file, keys)
        writer.writeheader()
        writer.writerows(corrected_data)

def correct_constructors_data():
    with open("data/src_data/constructors.csv", 'r') as file:
        reader = csv.DictReader(file, delimiter=",")
        
        corrected_data = []
        for index, row_dict in enumerate(reader):
            row_dict["constructorId"] = index + 1 
            corrected_data.append(row_dict)
        
        keys = corrected_data[0].keys()

    with open("data/corrected_data/corrected_constructors.csv", "w", newline="") as file:
        writer = csv.DictWriter(file, keys)
        writer.writeheader()
        writer.writerows(corrected_data)

def correct_results_data():
    with open("data/src_data/results.csv", 'r') as file:
        reader = csv.DictReader(file, delimiter=",")
        
        corrected_data = []
        for index, row_dict in enumerate(reader):
            row_dict["resultId"] = index + 1
            if int(row_dict["driverId"]) > 809:
                row_dict["driverId"] = int(row_dict["driverId"]) - 1
            corrected_data.append(row_dict)
        
        keys = corrected_data[0].keys()
    
    with open("data/src_data/altered_results.csv", "w", newline="") as file:
        writer = csv.DictWriter(file, keys)
        writer.writeheader()
        writer.writerows(corrected_data)
    

    race_df = csv_to_df("data/src_data/races.csv")
    corrected_race_df = csv_to_df("data/corrected_data/corrected_races.csv")
    corrected_results_df = csv_to_df("data/src_data/altered_results.csv")
    corrected_constructors_df = csv_to_df("data/corrected_data/corrected_constructors.csv")
    constructors_df = csv_to_df("data/src_data/constructors.csv")
    
    # merge old race with corrected race
    race_df = race_df.loc[:, ["raceId", "date"]]
    corrected_race_df = corrected_race_df.rename(columns={"raceId": "corrected_race_id"})
    corrected_race_df = corrected_race_df.join(race_df.set_index('date'), on='date')
    corrected_race_df = corrected_race_df.loc[:, ["corrected_race_id", "raceId"]]

    # merge df w/ old and corrected race data with race results 
    corrected_results_df = corrected_results_df.join(corrected_race_df.set_index('raceId'), on='raceId')
    
    # delete old raceId
    corrected_results_df = corrected_results_df.drop("raceId", axis=1)
    corrected_results_df = corrected_results_df.rename(columns={"corrected_race_id":"raceId"})
    col = corrected_results_df.pop("raceId")
    corrected_results_df.insert(1,col.name, col)

    # merge old constructor with corrected constructor
    corrected_constructors_df = corrected_constructors_df.loc[:, ["constructorId", "name"]]
    corrected_constructors_df = corrected_constructors_df.rename(columns={"constructorId": "new_constructor_id"})
    constructors_df = constructors_df.join(corrected_constructors_df.set_index("name"), on="name")
    constructors_df = constructors_df.loc[:, ["constructorId", "new_constructor_id"]]

    # merge df w/ old and corrected constructor data with constructor results 
    corrected_results_df = corrected_results_df.join(constructors_df.set_index('constructorId'), on='constructorId')
    
    # delete old constructorId
    corrected_results_df = corrected_results_df.drop("constructorId", axis=1)
    corrected_results_df = corrected_results_df.rename(columns={"new_constructor_id": "constructorId"})
    col = corrected_results_df.pop("constructorId")
    corrected_results_df.insert(3,col.name, col)
    corrected_results_df = corrected_results_df.astype({
        "raceId": "Int64",
        "constructorId": "Int64",
        "driverId": "Int64",
        "position": "Int64",
        "points": "int64",
        "rank": "Int64",
        "milliseconds": "Int64",
        "fastestLap": "Int64",
        "number": "Int64"
    })
    corrected_results_df.to_csv("data/corrected_data/corrected_results.csv", sep=',', index=False, na_rep='\\N')
   
    
def correct_constructor_standings_data():
    with open("data/src_data/constructor_standings.csv", 'r') as file:
        reader = csv.DictReader(file, delimiter=",")
        
        corrected_data = []
        for index, row_dict in enumerate(reader):
            row_dict["constructorStandingsId"] = index + 1 
            corrected_data.append(row_dict)
        
        keys = corrected_data[0].keys()

    with open("data/src_data/altered_constructor_standings.csv", "w", newline="") as file:
        writer = csv.DictWriter(file, keys)
        writer.writeheader()
        writer.writerows(corrected_data)

    race_df = csv_to_df("data/src_data/races.csv")
    corrected_race_df = csv_to_df("data/corrected_data/corrected_races.csv")
    corrected_constructor_standings_df = csv_to_df("data/src_data/altered_constructor_standings.csv")
    corrected_constructors_df = csv_to_df("data/corrected_data/corrected_constructors.csv")
    constructors_df = csv_to_df("data/src_data/constructors.csv")

    # correcting raceId
    race_df = race_df.loc[:, ["raceId", "date"]]
    corrected_race_df = corrected_race_df.rename(columns={"raceId": "corrected_race_id"})
    corrected_race_df = corrected_race_df.join(race_df.set_index('date'), on='date')
    corrected_race_df = corrected_race_df.loc[:, ["corrected_race_id", "raceId"]]
    corrected_constructor_standings_df = corrected_constructor_standings_df.join(corrected_race_df.set_index('raceId'), on='raceId')
    corrected_constructor_standings_df = corrected_constructor_standings_df.drop("raceId", axis=1)
    corrected_constructor_standings_df = corrected_constructor_standings_df.rename(columns={"corrected_race_id":"raceId"})
    col = corrected_constructor_standings_df.pop("raceId")
    corrected_constructor_standings_df.insert(1,col.name, col)

    # correcting constructorId
    corrected_constructors_df = corrected_constructors_df.loc[:, ["constructorId", "name"]]
    corrected_constructors_df = corrected_constructors_df.rename(columns={"constructorId": "new_constructor_id"})
    constructors_df = constructors_df.join(corrected_constructors_df.set_index("name"), on="name")
    constructors_df = constructors_df.loc[:, ["constructorId", "new_constructor_id"]]
    corrected_constructor_standings_df = corrected_constructor_standings_df.join(constructors_df.set_index('constructorId'), on='constructorId')
    corrected_constructor_standings_df = corrected_constructor_standings_df.drop("constructorId", axis=1)
    corrected_constructor_standings_df = corrected_constructor_standings_df.rename(columns={"new_constructor_id": "constructorId"})
    col = corrected_constructor_standings_df.pop("constructorId")
    corrected_constructor_standings_df.insert(2,col.name, col)
    corrected_constructor_standings_df = corrected_constructor_standings_df.astype({
        "raceId": "Int64",
        "constructorId": "Int64",
        "points": "int64"
    })
    corrected_constructor_standings_df.to_csv("data/corrected_data/corrected_constructor_standings.csv", sep=',', index=False, na_rep='\\N')

def correct_driver_standings_data():
    with open("data/src_data/driver_standings.csv", 'r') as file:
        reader = csv.DictReader(file, delimiter=",")
        
        corrected_data = []
        for index, row_dict in enumerate(reader):
            row_dict["driverStandingsId"] = index + 1
            if int(row_dict["driverId"]) > 809:
                row_dict["driverId"] = int(row_dict["driverId"]) - 1
            corrected_data.append(row_dict)
        
        keys = corrected_data[0].keys()
    
    with open("data/src_data/altered_driver_standings.csv", "w", newline="") as file:
        writer = csv.DictWriter(file, keys)
        writer.writeheader()
        writer.writerows(corrected_data)

    race_df = csv_to_df("data/src_data/races.csv")
    corrected_race_df = csv_to_df("data/corrected_data/corrected_races.csv")
    corrected_driver_standings_df = csv_to_df("data/src_data/altered_driver_standings.csv")
    
    # correcting driver standings
    race_df = race_df.loc[:, ["raceId", "date"]]
    corrected_race_df = corrected_race_df.rename(columns={"raceId": "corrected_race_id"})
    corrected_race_df = corrected_race_df.join(race_df.set_index('date'), on='date')
    corrected_race_df = corrected_race_df.loc[:, ["corrected_race_id", "raceId"]]
    corrected_driver_standings_df = corrected_driver_standings_df.join(corrected_race_df.set_index('raceId'), on='raceId')
    corrected_driver_standings_df = corrected_driver_standings_df.drop("raceId", axis=1)
    corrected_driver_standings_df = corrected_driver_standings_df.rename(columns={"corrected_race_id":"raceId"})
    col = corrected_driver_standings_df.pop("raceId")
    corrected_driver_standings_df.insert(1,col.name, col)
    corrected_driver_standings_df.to_csv("data/corrected_data/corrected_driver_standings.csv", sep=',', index=False, na_rep='\\N')


correct_results_data()
correct_race_data()
correct_driver_data()
correct_constructors_data()
correct_constructor_standings_data()
correct_driver_standings_data()