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
            "constructor_id": [1, 2, 3],
            "constructor_name": ["McLaren", "BMW Sauber", "Williams"],
            "nationality": ["British", "German", "British"],
        }
        
        with patch("src.csv_etl_script.csv_to_df") as mock_csv_to_df:
            mock_csv_to_df.return_value = pd.DataFrame(data)
            result = transform_df("dim_constructors")
            assert isinstance(result, pd.DataFrame)
            assert_frame_equal(result, pd.DataFrame(expected_data))

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
            "driver_id": [1, 2, 3],
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
            assert isinstance(result, pd.DataFrame)
            assert_frame_equal(result, pd.DataFrame(expected_data))

    def test_transform_dim_races(self):
        data = {
            "raceId": [2, 4, 8],
            "year":[2007, 2012, 2022],
            "round":[2, 4, 8],
            "circuitId": [1, 5, 19],
            "name":["Chinese Grand Prix", "Spanish Grand Prix", "German Grand Prix"],
            "date":["2007-03-01", "2012-08-11", "2022-12-05"], 
            "time":["06:00:00", "12:00:00", "12:00:00"],
            "url":["http://en.wikipedia.org/wiki/2009_Chinese_Grand_Prix", "http://en.wikipedia.org/wiki/2009_Spanish_Grand_Prix", "http://en.wikipedia.org/wiki/2009_German_Grand_Prix"],
            "fp1_date":[None, None, None],
            "fp1_time":[None, None, None],
            "fp2_date":[None, None, None],
            "fp2_time":[None, None, None],
            "fp3_date":[None, None, None],
            "fp3_time":[None, None, None],
            "quali_date":[None, None, None],
            "quali_time":[None, None, None],
            "sprint_date":[None, None, None],
            "sprint_time":[None, None, None],
        }

        expected_data = {
            "race_id": [2, 4, 8],
            "circuit_id": [1, 5, 19],
            "year":[2007, 2012, 2022],
            "round":[2, 4, 8],
            "date":["2007-03-01", "2012-08-11", "2022-12-05"], 
        }
        
        with patch("src.csv_etl_script.csv_to_df") as mock_csv_to_df:
            mock_csv_to_df.return_value = pd.DataFrame(data)
            result = transform_df("dim_races")
            assert isinstance(result, pd.DataFrame)
            assert_frame_equal(result, pd.DataFrame(expected_data))