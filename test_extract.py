import unittest
import pandas as pd
from extract import extract_data


class TextExtraction(unittest.TestCase):
    
    def test_extract__data(self):
        df = extract_data("data/Test_Data.csv")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertFalse(df.empty)

if __name__ == '__main__':
    unittest.main()