import unittest
prog_path = Path(os.path.abspath(__file__))
from es-oracle-para import read_tables_from_csv

class TestReadTablesFromCSV(unittest.TestCase):

    def test_read_tables_from_csv_normal(self):
        # Test with valid CSV input
        file_path = 'test_data.csv'
        expected = [{'schema_table': 'TABLE1', 'filter_using': 'COL1', 'date_column':'DATE1'}]
        
        actual = read_tables_from_csv(file_path)

        self.assertEqual(expected, actual)

    def test_read_tables_from_csv_missing_file(self):
        # Test with missing CSV file
        file_path = 'missing.csv'
        expected = []
        
        actual = read_tables_from_csv(file_path)

        self.assertEqual(expected, actual)

    def test_read_tables_from_csv_empty(self):
        # Test with empty CSV file
        file_path = 'empty.csv'
        expected = []

        actual = read_tables_from_csv(file_path)

        self.assertEqual(expected, actual)