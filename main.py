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






# def _to_native_scalar(v):
   
#     if pd.isna(v):
#         return None
#     if isinstance(v, (np.generic,)):
#         try:
#             return v.item()
#         except Exception:
#             return v
#     return v

# def load_flight_data(engine, flight_data, chunksize=2000, disable_fk=False):
 
#     conn = engine.raw_connection()
#     cursor = conn.cursor()
#     try:
#         if disable_fk:
#             cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
#             conn.commit()

#         cols = list(flight_data.columns)
#         placeholders = ",".join(["%s"] * len(cols))
#         col_list_sql = ",".join([f"`{c}`" for c in cols])
#         insert_sql = f"INSERT INTO fact_flights ({col_list_sql}) VALUES ({placeholders})"

#         total = len(flight_data)
#         batches = math.ceil(total / chunksize)
#         # print(f"Inserting {total} rows in {batches} batches of {chunksize}...")

#         for i in range(batches):
#             start = i * chunksize
#             end = min(start + chunksize, total)
#             chunk = flight_data.iloc[start:end]

#             to_list = []
#             for row in chunk.itertuples(index=False, name=None):
#                 new_row = tuple(_to_native_scalar(v) for v in row)
#                 to_list.append(new_row)

#             t0 = time.time()
#             try:
#                 cursor.executemany(insert_sql, to_list)
#                 conn.commit()
#                 print(f"  -> Batch {i+1}/{batches} inserted rows {start+1}-{end} in {time.time()-t0:.1f}s")
#             except Exception as ex:
#                 conn.rollback()
#                 print(f"Error inserting batch {i+1}: {ex}")

#                 # print("Sample converted rows (first 5):")
#                 for r in to_list[:5]:
#                     print(r)
#                 raise
#             finally:
#                 # liberar memoria
#                 del chunk
#                 del to_list
#                 gc.collect()

#         if disable_fk:
#             cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
#             conn.commit()

#         print("All batches inserted successfully.")
#     finally:
#         try:
#             cursor.close()
#             conn.close()
#         except Exception:
#             pass


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




    # HAY QUE AÑADIR ISTO
    race_data = transform_date(race_data, date_col='date')

    load_driver_data(engine, prepare_driver_data(driver_data))
    load_constructor_data(engine, prepare_constructor_data(constructor_data))
    load_race_data(engine, prepare_race_data(race_data))
    load_circuit_data(engine, prepare_circuit_data(circuit_data))
    load_qualifying_data(engine, prepare_qualifying_data(qualifying_data))
    load_pit_stops_data(engine, prepare_pit_stops_data(pit_stops_data))
    load_status_data(engine, prepare_status_data(status_data))
    load_results_data(engine, prepare_results_data(result_data))




    # # Read the dimension tables to get the IDs and add them to the fact table
    # airport_db = pd.read_sql('SELECT airport_id, iata_code FROM airport', con=engine)
    # airline_db = pd.read_sql('SELECT airline_id, airline_iata FROM airline', con=engine)
    # date_db = pd.read_sql('SELECT date_id, year, month, day FROM date', con=engine)
    # cancellation_db = pd.read_sql('SELECT cancellation_id, cancellation_type FROM cancellation_reason', con=engine)

    # facts_flight_data = prepare_flight_data(flights_data, cancellation_db, date_db, airport_db, airline_db)

    # # print("facts_flight_data shape:", facts_flight_data.shape)

    # print("Memoria aproximada (MB):", facts_flight_data.memory_usage(deep=True).sum() / 1024**2)


    # DEBUG: inspección rápida del DataFrame de hechos antes de insertar
    # print(">>> DEBUG: facts_flight_data tipo:", type(facts_flight_data))
    # print(">>> DEBUG: facts_flight_data shape:", getattr(facts_flight_data, "shape", None))
    # try:
    #     print(">>> DEBUG: primeras filas:\n", facts_flight_data.head(5))
    #     print(">>> DEBUG: info():")
    #     facts_flight_data.info()
    #     print(">>> DEBUG: nulos por columna:\n", facts_flight_data.isnull().sum().sort_values(ascending=False).head(20))
    # except Exception as e:
    #     print(">>> DEBUG: error mostrando facts_flight_data:", e)

    # # Chequeos de mapeo (muy útiles si prepare_flight_data hace merges/mappings)
    # # (usa flights_data original para comparar valores sin mapear)
    # print("Distinct ORIGIN_AIRPORT sample (first 20):", flights_data['ORIGIN_AIRPORT'].dropna().astype(str).unique()[:20])
    # print("Distinct DESTINATION_AIRPORT sample (first 20):", flights_data['DESTINATION_AIRPORT'].dropna().astype(str).unique()[:20])

    # # Si existen columnas origin_airport_id / destination_airport_id, contar nulos
    # if 'origin_airport_id' in facts_flight_data.columns:
    #     print("origin_airport_id nulls:", facts_flight_data['origin_airport_id'].isnull().sum())
    # if 'destination_airport_id' in facts_flight_data.columns:
    #     print("destination_airport_id nulls:", facts_flight_data['destination_airport_id'].isnull().sum())
    # if 'airline_id' in facts_flight_data.columns:
    #     print("airline_id nulls:", facts_flight_data['airline_id'].isnull().sum())
    # if 'date_id' in facts_flight_data.columns:
    #     print("date_id nulls:", facts_flight_data['date_id'].isnull().sum())

    # load_flight_data(engine,facts_flight_data )

    # load_flight_data(engine, facts_flight_data, chunksize=2000, disable_fk=True)



