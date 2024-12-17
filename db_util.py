from sqlalchemy import create_engine
import pandas as pd

def import_credentials(file_path):
    import yaml
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    return data

class RDSDatabaseConnector:
    def __init__(self, data):
        self.data = data
        self.engine= None

    def initialize_engine(self):
        HOST = self.data['RDS_HOST']
        USER = self.data['RDS_USER']
        PASSWORD = self.data['RDS_PASSWORD']
        DATABASE = self.data['RDS_DATABASE']
        PORT = self.data['RDS_PORT']
        engine = create_engine(f"{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        
        # Create the connection URL for SQLAlchemy
        connection_url = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
        
        # Create SQLAlchemy engine
        self.engine = create_engine(connection_url)
        print("Engine initialized successfully.")
    
    def extract_data(self, table_name="loan_payments"):
        if self.engine is None:
            raise ValueError("Engine not initialized. Please call 'initialize_engine' first.")
        
        # Query the table and load data into a Pandas DataFrame
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, self.engine)
        return df

    def save_data_to_csv(self, data, file_name="loan_payments_data.csv"):
        data.to_csv(file_name, index=False)
        print(f"Data saved to {file_name}.")
data = import_credentials('credentials.yaml')
db_connector = RDSDatabaseConnector(data)
db_connector.initialize_engine()
data_extract = db_connector.extract_data()
db_connector.save_data_to_csv(data_extract, 'loan_payments_data.csv')

def load_data_from_csv(file_path):
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        print(f"Data successfully loaded from {file_path}.")
        return df
    except FileNotFoundError:
        print(f"Error: The file at {file_path} does not exist.")
        return None
    except pd.errors.EmptyDataError:
        print(f"Error: The file at {file_path} is empty.")
        return None
    except pd.errors.ParserError:
        print(f"Error: There was an issue parsing the file at {file_path}.")
        return None
