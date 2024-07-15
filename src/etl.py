import pandas as pd
import utils.config as settings
from utils.utils import Connections
import hashlib

log = settings.log


class ETL(Connections):
    """
    This class performs the ETL process: reading CSV files, loading data to a PostgreSQL table,
    and exporting data to a JSON file. It inherits some functions from the Connections class in utils.
    """

    def __init__(self):
        super().__init__()
        self.script_path = settings.base_dir
        self.csv_files = ['people', 'places']

    def _read_csv_to_dataframe(self, full_path: str) -> pd.DataFrame:
        """
        Reads a CSV file and returns a pandas DataFrame.

        Args:
            full_path (str): The full path of the CSV file.

        Returns:
            pd.DataFrame: DataFrame containing the CSV data.
        """
        try:
            log.info("=====> Reading CSV ====>")
            df = pd.read_csv(full_path)
            log.info("==> CSV Read")
            return df
        except Exception as e:
            log.error("==> Error reading CSV")
            raise e

    def _generate_id(self, row, table_name):
        """
        Generates a unique ID for a row based on its content.

        Args:
            row (pd.Series): A row of data.
            table_name (str): The name of the table.

        Returns:
            str: A SHA-256 hashed ID.
        """
        if table_name == 'people':
            combined_string = f"{row['given_name']}{row['family_name']}{row['date_of_birth']}{row['place_of_birth']}"
        elif table_name == 'places':
            combined_string = f"{row['city']}{row['country']}"
        else:
            combined_string = ''.join(str(row[col]) for col in row.columns)

        return hashlib.sha256(combined_string.encode()).hexdigest()

    def _load_to_postgres(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Loads data from a pandas DataFrame to a PostgreSQL table.

        Args:
            df (pd.DataFrame): The DataFrame containing the data.
            table_name (str): The name of the PostgreSQL table.
        """
        try:
            log.info("=====> Loading to postgres ====>")
            df['id'] = df.apply(
                lambda row: self._generate_id(row, table_name), axis=1)

            conditions = 'append' if table_name == 'people' else 'replace'

            if table_name == 'people':
                df = df[['id', 'given_name', 'family_name',
                         'date_of_birth', 'place_of_birth']]
                df['date_of_birth'] = pd.to_datetime(
                    df['date_of_birth'], format='%Y-%m-%d')
            elif table_name == 'places':
                df = df[['id', 'city', 'country', 'county']]

            df.to_sql(name=table_name, con=self.connect_to_postgres(),
                      if_exists=conditions, index=False)
            log.info("==> Loaded to postgres")
        except Exception as e:
            log.error("==> Error loading to postgres")
            raise e

    def _export_json_from_postgres(self, table_name: str) -> None:
        """
        Exports data from a PostgreSQL table to a JSON file.

        Args:
            table_name (str): The name of the PostgreSQL table.
        """
        try:
            path = settings.get_csv_file_path('target_output')
            log.info("=====> Exporting JSON ====>")

            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(sql=query, con=self.connect_to_postgres())

            if table_name == 'people':
                df['date_of_birth'] = df['date_of_birth'].astype(str)

            df.to_json(f"{path}/{table_name}.json", orient='records')
            log.info("==> Exported JSON")
        except Exception as e:
            log.error("==> Error exporting JSON")
            raise e

    def run_etl(self) -> None:
        """
        Runs the ETL process: reads CSV files, loads data to PostgreSQL tables, and exports data to JSON files.
        """
        path = settings.get_csv_file_path('source')
        for file in self.csv_files:

            df = self._read_csv_to_dataframe(f"{path}/{file}.csv")
            self._load_to_postgres(df, file)
            self._export_json_from_postgres(file)

    def load_to_postgres(self) -> None:
        """
        Loads data from CSV files to PostgreSQL tables.
        """
        for file in self.csv_files:
            path = settings.get_csv_file_path('source')
            df = self._read_csv_to_dataframe(f"{path}/{file}.csv")
            self._load_to_postgres(df, file)

    def export_json_from_postgres(self) -> None:
        """
        Exports data from PostgreSQL tables to JSON files.
        """
        for file in self.csv_files:
            self._export_json_from_postgres(file)

# Example call to the ETL process
# etl = ETL()
# etl.run_etl()
