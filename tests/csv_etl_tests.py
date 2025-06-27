import pandas as pd
from pandas.testing import assert_frame_equal
from unittest.mock import patch
from src.csv_etl_script import csv_to_df, transform_df

class TestExtract_csv:
    def test_csv_to_df_returns_df(self, tmp_path):
        test_file = tmp_path/"test.csv"
        test_file.write_text("a,b\n1,2\n3,4")
        assert isinstance(csv_to_df(test_file), pd.DataFrame)
    
    def test_csv_returns_correct_df_values(self, tmp_path):
        test_file = tmp_path/"test.csv"
        test_file.write_text("a,b\n 1,2\n 3,4")
        
        data = {
            "a":[1, 3],
            "b":[2, 4]
        }
        assert_frame_equal(csv_to_df(test_file), pd.DataFrame(data))

class TestTransform_df:
    def test_transform_dim_constructors(self):
        data = {
            "constructorId": [1, 2, 3],
            "constructorRef": ["mclaren","bmw_sauber", "williams"],
            "name": ["McLaren", "BMW Sauber", "Williams"],
            "nationality": ["British", "German", "British"],
            "url": ["http://en.wikipedia.org/wiki/McLaren", "http://en.wikipedia.org/wiki/BMW_Sauber", "http://en.wikipedia.org/wiki/Williams_Grand_Prix_Engineering"]
        }

        expected_data = {
            "constructor_name": ["McLaren", "BMW Sauber", "Williams"],
            "nationality": ["British", "German", "British"],
        }
        
        with patch("src.csv_etl_script.csv_to_df") as mock_csv_to_df:
            mock_csv_to_df.return_value = pd.DataFrame(data)
            result = transform_df("dim_constructors")
            expected = pd.DataFrame(expected_data)
            assert isinstance(result, pd.DataFrame)
            assert_frame_equal(result, expected, check_like=True)
    

    def test_transform_dim_drivers(self):
        data = {
           "driverId": [1, 2, 3], 
           "driverRef": ["bob", "andy", "brandy"],
           "number": [9, 7, 3],
           "code": ["BOB", "AND", "BRA"],
           "forename": ["Bob", "Andy", "Brandy"],
           "surname": ["Builder", "Lee", "Man"],
           "dob": ["1976-08-27", "1991-04-22", "2002-01-04"],
           "nationality": ["Chinese", "Japan", "American"], 
           "url": ["http://en.wikipedia.org/wiki/Bob_Builder", "http://en.wikipedia.org/wiki/Andy_Lee", "http://en.wikipedia.org/wiki/Brandy_Man"]
        }

        expected_data = {
            "forename": ["Bob", "Andy", "Brandy"],
            "surname": ["Builder", "Lee", "Man"],
            "full_name": ["Bob Builder","Andy Lee", "Brandy Man"],
            "driver_number": [9, 7, 3],
            "nationality": ["Chinese", "Japan", "American"], 
            "dob": ["1976-08-27", "1991-04-22", "2002-01-04"],
        }
        
        with patch("src.csv_etl_script.csv_to_df") as mock_csv_to_df:
            mock_csv_to_df.return_value = pd.DataFrame(data)
            result = transform_df("dim_drivers")
            expected = pd.DataFrame(expected_data)
            assert isinstance(result, pd.DataFrame)
            assert_frame_equal(result, expected, check_like=True)
            
    def test_transform_dim_races(self):
        data = {
            "raceId": [2, 4, 8],
            "year":[2007, 2012, 2022],
            "round":[2, 4, 8],
            "circuitId": [1, 5, 19],
            "name":["Chinese Grand Prix", "Spanish Grand Prix", "German Grand Prix"],
            "date":["2007-03-01", "2012-08-11", "2022-12-05"], 
        }

        expected_data = {
            "circuit_id": [1, 5, 19],
            "year":[2007, 2012, 2022],
            "round":[2, 4, 8],
            "date":["2007-03-01", "2012-08-11", "2022-12-05"], 
        }
        
        with patch("src.csv_etl_script.csv_to_df") as mock_csv_to_df:
            mock_csv_to_df.return_value = pd.DataFrame(data)
            result = transform_df("dim_races")
            expected = pd.DataFrame(expected_data)
            assert isinstance(result, pd.DataFrame)
            assert_frame_equal(result, expected, check_like=True)

    def test_transform_dim_circuits(self):
        data = {
            "circuitId": [1, 4, 7],
            "circuitRef": ["moo_moo_meadow", "rainbow_road", "yoshi_falls"],
            "name": ["moo moo meadow", "rainbow road", "yoshi falls"],
            "location": ["mario", "space", "sky"],
            "country": ["Spain", "moon", "england"],
            "lat": [-37.8497, 5.6156, -23.7036],
            "lng": [101.738, 54.6031, -6.03417],
            "alt": [642, 0, 2],
            "url": ["http://en.wikipedia.org/wiki/Moo_Grand_Prix_Circuit", "http://en.wikipedia.org/wiki/Rainbow_road]", "http://en.wikipedia.org/wiki/yoshi"]

        }

        expected_data = {
            "circuit_name": ["moo moo meadow", "rainbow road", "yoshi falls"],
            "location": ["mario", "space", "sky"],
            "country": ["Spain", "moon", "england"]
        }

        with patch("src.csv_etl_script.csv_to_df") as mock_csv_to_df:
            mock_csv_to_df.return_value = pd.DataFrame(data)
            result = transform_df("dim_circuits")
            expected = pd.DataFrame(expected_data)
            assert isinstance(result, pd.DataFrame)
            assert_frame_equal(result, expected, check_like=True)

    def test_transform_fact_race_results(self):
        data = {
            "resultId":[2, 4, 7],
            "raceId":[12, 34, 66],
            "driverId":[6, 88, 21],
            "constructorId":[4, 6, 8],
            "fastestLapTime": ["1:26.452", "1:27.142", "1:27.892"],
            "number":[2, 6, 71],
            "grid":[1, 5, 7],
            "position":[1, 4, 9],
            "points":[30, 25, 3],
        }

        expected_data = {
            "race_id":[12, 34, 66],
            "driver_id":[6, 88, 21],
            "constructor_id":[4, 6, 8],
            "fastest_lap_time": [86.452, 87.142, 87.892],
            "starting_position":[1, 5, 7],
            "finish_position":[1, 4, 9],
            "points":[30, 25, 3],
        }

        with patch("src.csv_etl_script.csv_to_df") as mock_csv_to_df:
            mock_csv_to_df.return_value = pd.DataFrame(data)
            result = transform_df("fact_race_results")
            print(result)
            expected = pd.DataFrame(expected_data)
            assert isinstance(result, pd.DataFrame)
            assert_frame_equal(result, expected, check_like=True)
        
    def test_transform_fact_driver_standings(self):
        data = {
           "driverStandingsId": [1, 13, 3],
           "raceId": [4, 6, 8],
           "driverId": [44, 56, 9],
           "points": [30, 12, 3],
           "position": [1, 3, 9],
           "wins": [2, 7, 0],
        }

        expected_data = {
            "race_id": [4, 6, 8],
            "driver_id": [44, 56, 9],
            "points": [30, 12, 3],
            "position": [1, 3, 9],
            "wins": [2, 7, 0],
        }

        with patch("src.csv_etl_script.csv_to_df") as mock_csv_to_df:
            mock_csv_to_df.return_value = pd.DataFrame(data)
            result = transform_df("fact_driver_standings")
            expected = pd.DataFrame(expected_data)
            assert isinstance(result, pd.DataFrame)
            assert_frame_equal(result, expected, check_like=True)
    
    def test_transform_fact_constructor_standings(self):
        data = {
            "constructorStandingsId": [1, 4, 5],
            "raceId": [3, 5, 7], 
            "constructorId": [12, 5, 9], 
            "points": [1, 6, 7], 
            "position": [6, 3, 8],  
            "wins": [4, 2, 5],   
        }

        expected_data = {
            "race_id": [3, 5, 7], 
            "constructor_id": [12, 5, 9], 
            "points": [1, 6, 7], 
            "position": [6, 3, 8],  
            "wins": [4, 2, 5],   
        }

        with patch("src.csv_etl_script.csv_to_df") as mock_csv_to_df:
            mock_csv_to_df.return_value = pd.DataFrame(data)
            result = transform_df("fact_constructor_standings")
            expected = pd.DataFrame(expected_data)
            assert isinstance(result, pd.DataFrame)
            assert_frame_equal(result, expected, check_like=True)