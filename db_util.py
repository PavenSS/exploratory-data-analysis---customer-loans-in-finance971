def import_credentials():
    import yaml
    with open('credentials.yaml', 'r') as f:
        data = yaml.load(f, Loader=yaml.SafeLoader)
    return data

class RDSDatabase3Connector:
    def __init__(self, data):
        data = data

    def extract_data():
        from sqlalchemy import create_engine
        import pandas as pd
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = #'password'
        DATABASE = 'pagila'
        PORT = 5432
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")