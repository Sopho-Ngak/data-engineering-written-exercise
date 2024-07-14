import pandas as pd
import utils.config as settings
from utils.utils import Connections
import hashlib

csv_files = [
    'people',
    'places'
]
log = settings.log


class ETL(Connections):
    '''
    This class is the main class used to perform the ETL process. It inherits some function from the Connections class in utils. 
    It reads the csv files, loads the data to the postgres table and exports the data to a JSON file.
    '''

    def __init__(self):
        super().__init__()
        self.scrip_path = settings.base_dir

    def _read_csv_to_dataframe(self, full_path: str) -> pd.DataFrame:
        '''
        This function reads the csv file and returns a pandas dataframe. It takes the full path of the csv file as an argument.
        args: full_path: str
        return: pd.DataFrame
        '''
        try:
            log.info("=====> Reading CSV ====>")
            df = pd.read_csv(full_path)
            log.info("==> CSV Read")
            return df
        except Exception as e:
            log.error("==> Error reading CSV")
            raise e

    def _load_to_postgres(self, df: pd.DataFrame, table_name: str) -> None:
        '''
        This function loads the data from the pandas dataframe to the postgres table. It takes the dataframe and the table name as arguments.
        args: df: pd.DataFrame, table_name: str
        return: None
        '''

        def generate_id(row):
            if table_name == 'people':
                combined_string = row['given_name'] + row['family_name'] + \
                    row['date_of_birth'] + row['place_of_birth']
            elif table_name == 'places':
                combined_string = row['city'] + row['country']
            else:
                for col in df.columns:
                    combined_string += str(row[col])

            hashed_string = hashlib.sha256(
                combined_string.encode()).hexdigest()
            return hashed_string

        try:
            log.info("=====> Loading to postgres ====>")
            df['id'] = df.apply(generate_id, axis=1)

            if table_name == 'people':
                df = df[['id', 'given_name', 'family_name',
                         'date_of_birth', 'place_of_birth']]

            elif table_name == 'places':
                df = df[['id', 'city', 'country', 'county']]

            df.to_sql(name=table_name, con=self.connect_to_postgres(),
                      if_exists='append', index=False)
            log.info("==> Loaded to postgres")
        except Exception as e:
            log.error("==> Error loading to postgres")
            raise e

    def _export_json_from_postgres(self, table_name: str) -> None:
        '''
        This function exports the data from the postgres table to a JSON file. It takes the table name as an argument.
        args: table_name: str
        return: None
        '''
        try:
            path = settings.get_csv_file_path('target_output')
            log.info("=====> Exporting JSON ====>")
            df = pd.read_sql(
                f"SELECT * FROM {table_name}", con=self.connect_to_postgres())
            df.to_json(
                f"{path}/{table_name}.json", orient='records')
            log.info("==> Exported JSON")
        except Exception as e:
            log.error("==> Error exporting JSON")
            raise e

    def run_etl(self) -> None:
        '''
        This function runs the ETL process. It reads the csv files, loads the data to the postgres table and exports the data to a JSON file.
        args: None
        return: None
        '''
        for file in csv_files:
            path = settings.get_csv_file_path('source')
            df = self._read_csv_to_dataframe(f"{path}/{file}.csv")
            self._load_to_postgres(df, file)
            self._export_json_from_postgres(file)

    def load_to_postgres(self) -> None:
        '''
        This function loads the data from the csv files to the postgres table.
        args: None
        return: None
        '''
        for file in csv_files:
            path = settings.get_csv_file_path('source')
            df = self._read_csv_to_dataframe(f"{path}/{file}.csv")
            self._load_to_postgres(df, file)

    def export_json_from_postgres(self) -> None:
        '''
        This function exports the data from the postgres table to a JSON file.
        args: None
        return: None
        '''

        for file in csv_files:
            self._export_json_from_postgres(file)
