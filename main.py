import pandas as pd
from sqlalchemy import create_engine
from data_preparation import *
import json
import gc
import time
import math
import numpy as np



def load_db_config(config_file='config.json'):
    with open(config_file, 'r') as file:
        return json.load(file)
    
def get_connection(db_config):
    return create_engine(
        f'mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}'
    )


def load_driver_data(engine, driver_data):
    try:
        driver_data.to_sql('driver', con=engine, if_exists='append', index=False)
    except Exception as ex:
        print("Error while loading drivers dimension table: \n", ex)
    else:
        print("Driver dimension table loaded successfully.")


def load_constructor_data(engine, constructor_data):
    try:
        constructor_data.to_sql('constructor', con=engine, if_exists='append', index=False)
    except Exception as ex:
        print("Error while loading constructor dimension table: \n", ex)
    else:
        print("Constructor dimension table loaded successfully.")


def load_race_data(engine, race_data):
    try:
        race_data.to_sql('race', con=engine, if_exists='append', index=False)
    except Exception as ex:
        print("Error while loading race dimension table: \n", ex)
    else:
        print("Race dimension table loaded successfully.")


def load_circuit_data(engine, circuit_data):
    try:
        circuit_data.to_sql('circuit', con=engine, if_exists='append', index=False)
    except Exception as ex:
        print("Error while loading circuit dimension table: \n", ex)
    else:
        print("Circuit dimension table loaded successfully.")


def load_qualifying_data(engine, qualifying_data):
    try:
        qualifying_data.to_sql('qualifying', con=engine, if_exists='append', index=False, chunksize=50000, method='multi')
    except Exception as ex:
        print("Error while loading qualifying fact table: \n", ex)
    else:
        print("Qualifying fact table loaded successfully.")

def load_pit_stops_data(engine, pit_stops_data):
    try:
        pit_stops_data.to_sql('pit_stops', con=engine, if_exists='append', index=False, chunksize=50000, method='multi')
    except Exception as ex:
        print("Error while loading pit_stops fact table: \n", ex)
    else:
        print("Pit_stops fact table loaded successfully.")

def load_status_data(engine, status_data):
    try:
        status_data.to_sql('status', con=engine, if_exists='append', index=False, chunksize=50000, method='multi')
    except Exception as ex:
        print("Error while loading status table: \n", ex)
    else:
        print("Status table loaded successfully.")

def load_results_data(engine, results_data):
    try:
        results_data.to_sql('results', con=engine, if_exists='append', index=False, chunksize=50000, method='multi')
    except Exception as ex:
        print("Error while loading results fact table: \n", ex)
    else:
        print("Results fact table loaded successfully.")



if __name__ == '__main__':
    
    circuit_data = pd.read_csv("Data/circuits.csv")
    constructor_data = pd.read_csv("Data/constructors.csv")
    driver_data = pd.read_csv("Data/drivers.csv")
    pit_stops_data = pd.read_csv("Data/pit_stops.csv")
    qualifying_data = pd.read_csv("Data/qualifying.csv")
    race_data = pd.read_csv("Data/races.csv")
    status_data = pd.read_csv("Data/status.csv")
    result_data = pd.read_csv("Data/results.csv")


    try:
        # Get the connection with the database
        db_config = load_db_config()
        engine = get_connection(db_config)
        print(f"Connection to the {db_config['host']} for user {db_config['user']} created successfully.")
        
    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)




    race_data = transform_date(race_data, date_col='date')

    load_driver_data(engine, prepare_driver_data(driver_data))
    load_constructor_data(engine, prepare_constructor_data(constructor_data))
    load_race_data(engine, prepare_race_data(race_data))
    load_circuit_data(engine, prepare_circuit_data(circuit_data))
    load_status_data(engine, prepare_status_data(status_data))

    # Read the dimension tables to get the IDs and add them to the fact tables

    race_db =  pd.read_sql('SELECT * FROM race', con=engine)
    driver_db = pd.read_sql('SELECT * FROM driver', con=engine)
    constructor_db = pd.read_sql('SELECT * FROM constructor', con=engine)
    status_db = pd.read_sql('SELECT * FROM status', con=engine)
    circuit_db = pd.read_sql('SELECT * FROM circuit', con=engine)


    facts_qualifying_data = prepare_qualifying_data(
        qualifying_data, driver_data, constructor_data, race_data, circuit_data,
        circuit_db, constructor_db, race_db, driver_db
    )
    
    fact_pit_stops_data = prepare_pit_stops_data(
        pit_stops_data, driver_data, constructor_data, race_data,
        constructor_db, race_db, driver_db
    )
    
    facts_results_data = prepare_results_data(
        result_data, driver_data, constructor_data, race_data, circuit_data, status_data,
        circuit_db, constructor_db, race_db, driver_db, status_db
    )

    load_qualifying_data(engine, facts_qualifying_data)
    load_pit_stops_data(engine, fact_pit_stops_data)
    load_results_data(engine, facts_results_data)
