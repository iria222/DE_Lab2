import pandas as pd
from sqlalchemy import create_engine
from data_preparation import *
import json



def load_db_config(config_file='config.json'):
    with open(config_file, 'r') as file:
        return json.load(file)
    
def get_connection(db_config):
    return create_engine(
        f'mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{'pit_stops_db'}'
    )


def load_driver_data(engine, driver_data):
    try:
        driver_data.to_sql('driver', con=engine, if_exists='append', index=False)
    except Exception as ex:
        print("Error while loading drivers dimension table: \n", ex)
    else:
        print("Driver dimension table loaded successfully.")


def load_race_data(engine, race_data):
    try:
        race_data.to_sql('race', con=engine, if_exists='append', index=False)
    except Exception as ex:
        print("Error while loading race dimension table: \n", ex)
    else:
        print("Race dimension table loaded successfully.")


def load_pit_stops_data(engine, pit_stops_data):
    try:
        pit_stops_data.to_sql('pit_stops', con=engine, if_exists='append', index=False, chunksize=50000, method='multi')
    except Exception as ex:
        print("Error while loading pit_stops fact table: \n", ex)
    else:
        print("Pit_stops fact table loaded successfully.")



if __name__ == '__main__':
    
    driver_data = pd.read_csv("Data/drivers.csv")
    pit_stops_data = pd.read_csv("Data/pit_stops.csv")
    race_data = pd.read_csv("Data/races.csv")


    try:
        # Get the connection with the database
        db_config = load_db_config()
        engine = get_connection(db_config)
        print(f"Connection to the {db_config['host']} for user {db_config['user']} created successfully.")
        
    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)




    race_data = transform_date(race_data, date_col='date')

    load_driver_data(engine, prepare_driver_data(driver_data))
    load_race_data(engine, prepare_race_data(race_data))


    # Read the dimension tables to get the IDs and add them to the fact tables

    race_db =  pd.read_sql('SELECT * FROM race', con=engine)
    driver_db = pd.read_sql('SELECT * FROM driver', con=engine)



    
    fact_pit_stops_data = prepare_pit_stops_data(
        pit_stops_data, driver_data, race_data,
        race_db, driver_db
    )
    

    load_pit_stops_data(engine, fact_pit_stops_data)
