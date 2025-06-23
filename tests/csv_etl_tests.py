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
            assert isinstance(transform_df("dim_constructors"), pd.DataFrame)
            assert_frame_equal(transform_df("dim_constructors"), pd.DataFrame(expected_data))

    def test_transform_dim_drivers(self):
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
            assert isinstance(transform_df("dim_constructors"), pd.DataFrame)
            assert_frame_equal(transform_df("dim_constructors"), pd.DataFrame(expected_data))
