from pandas.testing import assert_frame_equal
from src.csv_etl_script import csv_to_df, transform_df

class TestExtractCsv:
    def test_csv_to_df_returns_df(self):
        input = "results"
        expected = 
        assert type(csv_to_df(input)) == expected