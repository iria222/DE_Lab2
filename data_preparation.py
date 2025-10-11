import pandas as pd
import datetime
import time as _time
import numpy as np


def _log_q(msg):
    print(f"[prepare_qualifying_data] {msg}")

def _log_r(msg):
    print(f"[prepare_results_data] {msg}")

def _log_p(msg):
    print(f"[prepare_pit_stops_data] {msg}")

def handle_nulls(df, columns_to_clean=None, null_markers=None):
    """
    Versión simplificada: solo reemplaza marcadores de null por None/NaN.
    
    Parámetros:
    -----------
    df : pd.DataFrame
        DataFrame a limpiar
    columns_to_clean : list, opcional
        Lista de columnas específicas a limpiar. Si None, limpia todas.
    null_markers : list, opcional
        Lista de valores a considerar como null. Por defecto: ['\\N', 'null', '', 'N/A']
    
    Retorna:
    --------
    pd.DataFrame con nulls estandarizados
    """
    
    df = df.copy()
    
    if null_markers is None:
        null_markers = ['\\N', 'null', 'NULL', 'None', '', 'N/A', 'n/a', 'NA']
    
    if columns_to_clean is None:
        columns_to_clean = df.columns
    
    for col in columns_to_clean:
        if col in df.columns:
            df[col] = df[col].replace(null_markers, None)
    
    return df

def remove_rows_with_missing_keys(df, key_columns, logger_func=None):
    """
    Elimina las filas de un DataFrame donde cualquiera de las columnas clave 
    especificadas es nula.

    Parámetros:
    -----------
    df : pd.DataFrame
        El DataFrame de entrada.
    key_columns : list
        Una lista de nombres de columnas a comprobar en busca de nulos.
    logger_func : function, opcional
        Una función de logging (como _log_q, _log_r) para imprimir mensajes.
        Si es None, usará print().

    Retorna:
    --------
    pd.DataFrame
        Un nuevo DataFrame sin las filas que tenían nulos en las columnas clave.
    """
    initial_rows = len(df)
    
    # Nos aseguramos de que las columnas existen antes de intentar eliminar nulos
    existing_key_columns = [col for col in key_columns if col in df.columns]
    
    if not existing_key_columns:
        msg = f"WARNING: Ninguna de las columnas clave {key_columns} existe en el DataFrame."
        if logger_func:
            logger_func(msg)
        else:
            print(msg)
        return df

    cleaned_df = df.dropna(subset=existing_key_columns).copy()
    
    rows_dropped = initial_rows - len(cleaned_df)
    
    if rows_dropped > 0:
        msg = f"INFO: Se eliminaron {rows_dropped} filas por tener valores nulos en: {existing_key_columns}."
        if logger_func:
            logger_func(msg)
        else:
            print(msg)
            
    return cleaned_df


def prepare_driver_data(driver_data):
    
    driver_data = driver_data.rename(
        columns={
            'forename' : 'driver_name',
            'surname' : 'driver_surname',
            'dob' : 'date_of_birth',
            'nationality' : 'driver_nationality',

        }
    )

    driver_data['date_of_birth'] = pd.to_datetime(driver_data['date_of_birth'], errors='coerce')

    driver_data = handle_nulls(driver_data)


    return driver_data[['driver_name', 'driver_surname', 'date_of_birth', 'driver_nationality']].drop_duplicates()

def prepare_constructor_data(constructor_data):
    constructor_data = constructor_data.rename(
        columns={
            'name' : 'constructor_name',
            'nationality' : 'constructor_nationality'
        }
    )
    return constructor_data[['constructor_name', 'constructor_nationality']].drop_duplicates()


#   ** MIRAR O DAS FECHAS **
def prepare_race_data(race_data):
    """
    Asume que race_data ya tiene YEAR, MONTH, DAY creadas por transform_date()
    """
    race_data = race_data.rename(
        columns={
            'name': 'race_name'
        }
    )
    
    # Crear DataFrame limpio con las columnas necesarias
    race_clean = race_data[['YEAR', 'MONTH', 'DAY', 'race_name']].copy()
    
    # Renombrar a minúsculas para la BD
    race_clean = race_clean.rename(
        columns={
            'YEAR': 'year',
            'MONTH': 'month',
            'DAY': 'day'
        }
    )

    race_data = handle_nulls(race_data)

    
    return race_clean.drop_duplicates()

def transform_date(race_df, date_col='date', overwrite=False):


    df = race_df.copy()

    if date_col not in df.columns:
        raise KeyError(f"No existe la columna '{date_col}' en el DataFrame")

    df[date_col] = df[date_col].replace({'\\N': None, 'null': None, '': None})
    
    parsed = pd.to_datetime(df[date_col], format='%Y-%m-%d', errors='coerce')

    df['RACE_DATE'] = parsed

    # Añadir YEAR/MONTH/DAY en mayúsculas (compatibles con tu prepare_race_data)
    if overwrite or 'YEAR' not in df.columns:
        df['YEAR'] = parsed.dt.year.astype('Int64')   # Int64 para permitir nulos
    if overwrite or 'MONTH' not in df.columns:
        df['MONTH'] = parsed.dt.month.astype('Int64')
    if overwrite or 'DAY' not in df.columns:
        df['DAY'] = parsed.dt.day.astype('Int64')

    # info rápida de validación
    n_total = len(df)
    n_invalid = parsed.isna().sum()
    if n_invalid:
        print(f"[add_date_columns_from_date] {n_invalid}/{n_total} fechas NO parseadas (NaT).")
    else:
        print(f"[add_date_columns_from_date] Todas las fechas parseadas correctamente ({n_total}).")

    return df

def prepare_circuit_data(circuit_data):
    circuit_data = circuit_data.rename(
        columns={
            'name' : 'circuit_name',
            'location' : 'circuit_location',
            'country' : 'circuit_country',
            'lat' : 'latitude',
            'lng' : 'longitude',
            'alt' : 'altitude'
        }
    )

    circuit_data = handle_nulls(circuit_data)

    return circuit_data[['circuit_name', 'circuit_location', 'circuit_country', 'latitude', 'longitude', 'altitude']].drop_duplicates()


def prepare_status_data(status_data):
    status_data = status_data.rename(
        columns={
            'STATUS' : 'status'
        }
    )

    status_data = handle_nulls(status_data)

    return status_data[['status']].drop_duplicates()




# def prepare_qualifying_data(qualifying_data, circuit_db, constructor_db, race_db, driver_db):
#     qualifying_data = qualifying_data.rename(
#         columns={
#             'Q1' : 'q1',
#             'Q2' : 'q2',
#             'Q3' : 'q3'
#         }
#     )

#     circuit_map = {}
#     if {'circuit_id'}.issubset(circuit_db.columns):
#         circuit_map = dict(zip(circuit_db['circuit_id'].astype(int)))
#     else:
#         _log_q("WARNING: circuit_db does not contain circuit_id")

#     constructor_map = {}
#     if {'constructor_id'}.issubset(constructor_db.columns):
#         constructor_map = dict(zip(constructor_db['constructor_id'].astype(int)))
#     else:
#         _log_q("WARNING: constructor_db does not contain constructor_id")

#     race_map = {}
#     if {'race_id'}.issubset(race_db.columns):
#         race_map = dict(zip(race_db['race_id'].astype(int)))
#     else:
#         _log_q("WARNING: race_db does not contain race_id")

#     driver_map = {}
#     if {'driver_id'}.issubset(driver_db.columns):
#         driver_map = dict(zip(driver_db['driver_id'].astype(int)))
#     else:
#         _log_q("WARNING: driver_db does not contain driver_id")

#     out_cols = ['circuit_id', 'constructor_id', 'race_id', 'driver_id', 'q1', 'q2', 'q3']
  
    
#     return out_cols.drop_duplicates()

#     fact_pit_stops_data = prepare_pit_stops_data(pit_stops_data, constructor_db, race_db, driver_db)

def prepare_qualifying_data(qualifying_data, drivers_csv, constructors_csv, races_csv, circuits_csv,
                            circuit_db, constructor_db, race_db, driver_db):
    """
    Prepara datos de qualifying mapeando Driver, Constructor, Race y Circuit 
    mediante claves de negocio.
    """
    _log_q("Preparando datos de qualifying (con circuit_id por nombre de circuito)...")
    
    df = qualifying_data.copy()
    
    # 1. PREPARAR CLAVES DE NEGOCIO ... (sin cambios aquí)
    drivers_csv_temp = drivers_csv[['driverId', 'forename', 'surname', 'dob']].rename(columns={
        'forename': 'driver_name', 'surname': 'driver_surname', 'dob': 'date_of_birth'
    })
    drivers_csv_temp['date_of_birth'] = pd.to_datetime(drivers_csv_temp['date_of_birth'], errors='coerce')
    constructors_csv_temp = constructors_csv[['constructorId', 'name']].rename(columns={'name': 'constructor_name'})
    races_csv_temp = races_csv[['raceId', 'year', 'name']].rename(columns={'name': 'race_name'})
    race_circuit_names = races_csv[['raceId', 'circuitId']].merge(
        circuits_csv[['circuitId', 'name']], on='circuitId', how='left'
    ).rename(columns={'name': 'circuit_name'}).drop(columns=['circuitId'])

    
    # 2. MERGE DE CLAVES DE NEGOCIO ... (sin cambios aquí)
    df = df.merge(drivers_csv_temp, on='driverId', how='left')
    df = df.merge(constructors_csv_temp, on='constructorId', how='left')
    df = df.merge(races_csv_temp, on='raceId', how='left')
    df = df.merge(race_circuit_names, on='raceId', how='left')
    df.drop(columns=['driverId', 'constructorId', 'raceId'], inplace=True, errors='ignore')
    
    # Eliminar filas donde la clave de negocio (circuit_name) es nula
    df = remove_rows_with_missing_keys(df, key_columns=['circuit_name'], logger_func=_log_q)
    # 3. MERGE CON LAS TABLAS DE DIMENSIÓN ... (el resto del código sigue igual)
    _log_q("Mapeando nuevos IDs...")
    
    driver_db['date_of_birth'] = pd.to_datetime(driver_db['date_of_birth'], errors='coerce')
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
    df = df.merge(driver_db[['driver_id', 'driver_name', 'driver_surname', 'date_of_birth']],
                  on=['driver_name', 'driver_surname', 'date_of_birth'], how='left')
    df.drop(columns=['driver_name', 'driver_surname', 'date_of_birth'], inplace=True, errors='ignore')

    df = df.merge(constructor_db[['constructor_id', 'constructor_name']],
                  on='constructor_name', how='left')
    df.drop(columns=['constructor_name'], inplace=True, errors='ignore')

    df = df.merge(race_db[['race_id', 'year', 'race_name']],
                  on=['year', 'race_name'], how='left')
    df.drop(columns=['year', 'race_name'], inplace=True, errors='ignore')
                  
    circuit_db['circuit_name_clean'] = circuit_db['circuit_name'].astype(str).str.strip().str.lower()
    df['circuit_name_clean'] = df['circuit_name'].astype(str).str.strip().str.lower()
    df = df.merge(circuit_db[['circuit_id', 'circuit_name_clean']],
                  on='circuit_name_clean', how='left')
    df.drop(columns=['circuit_name', 'circuit_name_clean'], inplace=True, errors='ignore')

    # ... (el resto de la función no cambia)
    
    # 4. LIMPIEZA Y SELECCIÓN FINAL
    rename_map = {'q1': 'q1', 'q2': 'q2', 'q3': 'q3'}
    df = df.rename(columns=rename_map)
    needed_columns = ['circuit_id', 'constructor_id', 'race_id', 'driver_id', 'q1', 'q2', 'q3']
    existing_cols = [c for c in needed_columns if c in df.columns]
    result = df[existing_cols].copy()
    for col in ['circuit_id', 'constructor_id', 'race_id', 'driver_id']:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors='coerce').astype('Int64')
    null_counts = result[['driver_id', 'constructor_id', 'race_id', 'circuit_id']].isna().sum()
    if null_counts.any():
        _log_q(f"WARNING: IDs no mapeados:\n{null_counts[null_counts > 0]}")
    else:
        _log_q("Todos los IDs mapeados correctamente.")
    return result.drop_duplicates()


# def prepare_pit_stops_data(pit_stops_data, constructor_db, race_db, driver_db):
#     pit_stops_data = pit_stops_data.rename(
#         columns={
#             'STOP' : 'stop_number',
#             'LAP' : 'lap_number',
#             'TIME' : 'stop_time',
#             'MILISECONDS' : 'stop_duration'
#         }
#     )

#     constructor_map = {}
#     if {'constructor_id'}.issubset(constructor_db.columns):
#         constructor_map = dict(zip(constructor_db['constructor_id'].astype(int)))
#     else:
#         _log_p("WARNING: constructor_db does not contain constructor_id")

#     race_map = {}
#     if {'race_id'}.issubset(race_db.columns):
#         race_map = dict(zip(race_db['race_id'].astype(int)))
#     else:
#         _log_p("WARNING: race_db does not contain race_id")

#     driver_map = {}
#     if {'driver_id'}.issubset(driver_db.columns):
#         driver_map = dict(zip(driver_db['driver_id'].astype(int)))
#     else:
#         _log_p("WARNING: driver_db does not contain driver_id")

#     return pit_stops_data[['constructor_id', 'race_id', 'driver_id', 'stop_number', 'lap_number', 'stop_time', 'stop_duration']].drop_duplicates()


# def prepare_results_data(results_data,  circuit_db, constructor_db, race_db, driver_db, status_db):
#     results_data = results_data.rename(
#         columns={
#             'NUMBER' : 'car_number',
#             'GRID' : 'starting_position',
#             'POSITION' : 'final_position',
#             'POSITIONORDER' : 'position_order',
#             'POINTS' : 'points',
#             'LAPS' : 'laps',



#         }
#     )

#     circuit_map = {}
#     if {'circuit_id'}.issubset(circuit_db.columns):
#         circuit_map = dict(zip(circuit_db['circuit_id'].astype(int)))
#     else:
#         _log_r("WARNING: circuit_db does not contain circuit_id")

#     constructor_map = {}
#     if {'constructor_id'}.issubset(constructor_db.columns):
#         constructor_map = dict(zip(constructor_db['constructor_id'].astype(int)))
#     else:
#         _log_r("WARNING: constructor_db does not contain constructor_id")

#     race_map = {}
#     if {'race_id'}.issubset(race_db.columns):
#         race_map = dict(zip(race_db['race_id'].astype(int)))
#     else:
#         _log_r("WARNING: race_db does not contain race_id")

#     driver_map = {}
#     if {'driver_id'}.issubset(driver_db.columns):
#         driver_map = dict(zip(driver_db['driver_id'].astype(int)))
#     else:
#         _log_r("WARNING: driver_db does not contain driver_id")
    
#     status_map = {}
#     if {'status_id'}.issubset(status_db.columns):
#         status_map = dict(zip(driver_db['status_id'].astype(int)))
#     else:
#         _log_r("WARNING: status_db does not contain status_id")

    

#     return results_data[['constructor_id', 'race_id', 'driver_id', 'circuit_id', 'status_id', 'car_number', 'starting_position', 'final_position', 'position_order', 'points', 'laps']].drop_duplicates()

def prepare_pit_stops_data(pit_stops_data, drivers_csv, constructors_csv, races_csv,
                           constructor_db, race_db, driver_db):
    """
    Prepara datos de pit stops mapeando a nuevos IDs *SOLO* usando claves de negocio.
    Obtiene el constructor a través de results.csv.
    """
    _log_p("Preparando datos de pit stops (solo claves de negocio)...")
    
    df = pit_stops_data.copy()
    
    # 1. PREPARAR CLAVES DE NEGOCIO Y DATOS INTERMEDIOS
    
    # Driver: forename + surname + dob
    drivers_csv_temp = drivers_csv[['driverId', 'forename', 'surname', 'dob']].rename(columns={
        'forename': 'driver_name', 'surname': 'driver_surname', 'dob': 'date_of_birth'
    })
    drivers_csv_temp['date_of_birth'] = pd.to_datetime(drivers_csv_temp['date_of_birth'], errors='coerce')
    
    # Race: year + name
    races_csv_temp = races_csv[['raceId', 'year', 'name']].rename(columns={'name': 'race_name'})
    
    # Constructor: Se necesita el constructor del results.csv (raceId+driverId)
    results_csv = pd.read_csv("Data/results.csv")
    race_driver_to_constructor = results_csv[['raceId', 'driverId', 'constructorId']].drop_duplicates()
    
    # Constructor Name (Clave de Negocio)
    constructor_name_map = constructors_csv[['constructorId', 'name']].rename(columns={'name': 'constructor_name'})
    
    
    # 2. MERGE DE CLAVES DE NEGOCIO EN EL DATAFRAME DE HECHOS (df)
    
    # Obtener claves de negocio de Driver
    df = df.merge(drivers_csv_temp, on='driverId', how='left')
    
    # Obtener clave de negocio de Race
    df = df.merge(races_csv_temp, on='raceId', how='left')
    
    # Obtener constructorId original (del results.csv)
    df = df.merge(race_driver_to_constructor, on=['raceId', 'driverId'], how='left')
    
    # Obtener constructor_name (clave de negocio)
    df = df.merge(constructor_name_map, on='constructorId', how='left')
    
    # Limpiar columnas temporales y IDs originales
    df.drop(columns=['driverId', 'forename', 'surname', 'raceId', 'constructorId'], inplace=True, errors='ignore')
    
    
    # 3. MERGE CON LAS TABLAS DE DIMENSIÓN (DB) PARA OBTENER LOS NUEVOS IDs
    
    _log_p("Mapeando nuevos IDs...")
    
    # Mapeo Driver ID
    driver_db['date_of_birth'] = pd.to_datetime(driver_db['date_of_birth'], errors='coerce')
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
    df = df.merge(driver_db[['driver_id', 'driver_name', 'driver_surname', 'date_of_birth']],
                  on=['driver_name', 'driver_surname', 'date_of_birth'], how='left')
    df.drop(columns=['driver_name', 'driver_surname', 'date_of_birth'], inplace=True, errors='ignore')

    # Mapeo Race ID
    df = df.merge(race_db[['race_id', 'year', 'race_name']],
                  on=['year', 'race_name'], how='left')
    df.drop(columns=['year', 'race_name'], inplace=True, errors='ignore')
                  
    # Mapeo Constructor ID
    df = df.merge(constructor_db[['constructor_id', 'constructor_name']],
                  on='constructor_name', how='left')
    df.drop(columns=['constructor_name'], inplace=True, errors='ignore')

    
    # 4. LIMPIEZA Y SELECCIÓN FINAL
    
    rename_map = {
        'stop': 'stop_number', 'lap': 'lap_number', 'time': 'stop_time', 'milliseconds': 'stop_duration'
    }
    df = df.rename(columns=rename_map)
    
    needed_columns = ['constructor_id', 'race_id', 'driver_id', 'stop_number', 'lap_number', 'stop_time', 'stop_duration']
    existing_cols = [c for c in needed_columns if c in df.columns]
    result = df[existing_cols].copy()
    
    null_counts = result[['driver_id', 'constructor_id', 'race_id']].isna().sum()
    if null_counts.any():
        _log_p(f"WARNING: IDs no mapeados:\n{null_counts}")
    else:
        _log_p("Todos los IDs mapeados correctamente.")
    
    return result.drop_duplicates()

def prepare_results_data(results_data, drivers_csv, constructors_csv, races_csv, circuits_csv, status_csv,
                         circuit_db, constructor_db, race_db, driver_db, status_db):
    """
    Prepara datos de results, mapeando todas las dimensiones 
    mediante claves de negocio, sin usar IDs originales.
    """
    _log_r("Preparando datos de results (con circuit_id y status_id por nombre)...")
    
    df = results_data.copy()
    
    # 1. PREPARAR CLAVES DE NEGOCIO ... (sin cambios aquí)
    drivers_csv_temp = drivers_csv[['driverId', 'forename', 'surname', 'dob']].rename(columns={
        'forename': 'driver_name', 'surname': 'driver_surname', 'dob': 'date_of_birth'
    })
    drivers_csv_temp['date_of_birth'] = pd.to_datetime(drivers_csv_temp['date_of_birth'], errors='coerce')
    constructors_csv_temp = constructors_csv[['constructorId', 'name']].rename(columns={'name': 'constructor_name'})
    races_csv_temp = races_csv[['raceId', 'year', 'name']].rename(columns={'name': 'race_name'})
    race_circuit_names = races_csv[['raceId', 'circuitId']].merge(
        circuits_csv[['circuitId', 'name']], on='circuitId', how='left'
    ).rename(columns={'name': 'circuit_name'}).drop(columns=['circuitId'])
    status_csv_temp = status_csv[['statusId', 'status']]
    
    
    # 2. MERGE DE CLAVES DE NEGOCIO ... (sin cambios aquí)
    df = df.merge(drivers_csv_temp, on='driverId', how='left')
    df = df.merge(constructors_csv_temp, on='constructorId', how='left')
    df = df.merge(races_csv_temp, on='raceId', how='left')
    df = df.merge(race_circuit_names, on='raceId', how='left')
    df = df.merge(status_csv_temp, on='statusId', how='left')
    df.drop(columns=['driverId', 'constructorId', 'raceId', 'statusId'], inplace=True, errors='ignore')
    
    # Eliminar filas donde las claves de negocio (circuit_name, status) son nulas
    df = remove_rows_with_missing_keys(df, key_columns=['circuit_name', 'status'], logger_func=_log_r)
    
    # 3. MERGE CON LAS TABLAS DE DIMENSIÓN ... (el resto del código sigue igual)
    _log_r("Mapeando nuevos IDs...")
    
    driver_db['date_of_birth'] = pd.to_datetime(driver_db['date_of_birth'], errors='coerce')
    df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce')
    df = df.merge(driver_db[['driver_id', 'driver_name', 'driver_surname', 'date_of_birth']],
                  on=['driver_name', 'driver_surname', 'date_of_birth'], how='left')
    df.drop(columns=['driver_name', 'driver_surname', 'date_of_birth'], inplace=True, errors='ignore')

    df = df.merge(constructor_db[['constructor_id', 'constructor_name']],
                  on='constructor_name', how='left')
    df.drop(columns=['constructor_name'], inplace=True, errors='ignore')
                  
    df = df.merge(race_db[['race_id', 'year', 'race_name']],
                  on=['year', 'race_name'], how='left')
    df.drop(columns=['year', 'race_name'], inplace=True, errors='ignore')
                  
    circuit_db['circuit_name_clean'] = circuit_db['circuit_name'].astype(str).str.strip().str.lower()
    df['circuit_name_clean'] = df['circuit_name'].astype(str).str.strip().str.lower()
    df = df.merge(circuit_db[['circuit_id', 'circuit_name_clean']], on='circuit_name_clean', how='left')
    df.drop(columns=['circuit_name', 'circuit_name_clean'], inplace=True, errors='ignore')
                  
    status_db['status_clean'] = status_db['status'].astype(str).str.strip().str.lower()
    df['status_clean'] = df['status'].astype(str).str.strip().str.lower()
    df = df.merge(status_db[['status_id', 'status_clean']], on='status_clean', how='left')
    df.drop(columns=['status', 'status_clean'], inplace=True, errors='ignore')


    num_cols = ['number', 'grid', 'positionOrder', 'points', 'laps']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    rename_map = {
        'number': 'car_number', 'grid': 'starting_position', 'position': 'final_position', 
        'positionOrder': 'position_order', 'points': 'points', 'laps': 'laps'
    }
    df = df.rename(columns=rename_map)
    
    needed_columns = ['circuit_id', 'constructor_id', 'race_id', 'driver_id', 'status_id',
                      'car_number', 'starting_position', 'final_position', 'position_order', 'points', 'laps']
    existing_cols = [c for c in needed_columns if c in df.columns]
    result = df[existing_cols].copy()
    
    
    # <-- INICIO DE LOS CAMBIOS CLAVE -->

    # 1. Limpiar y rellenar la columna 'final_position'
    if 'final_position' in result.columns:
        # Primero, convertimos el texto '\\N' a un nulo numérico (NaN)
        result['final_position'] = result['final_position'].replace({'\\N': np.nan})
        result['final_position'] = pd.to_numeric(result['final_position'], errors='coerce')
        
        # AHORA, rellenamos esos nulos con 0
        result['final_position'] = result['final_position'].fillna(0)
        
        # Finalmente, como ya no hay nulos, podemos convertir la columna a entero
        result['final_position'] = result['final_position'].astype(int)

    # 2. Convertir las columnas de ID a un tipo float compatible
    id_cols = ['circuit_id', 'constructor_id', 'race_id', 'driver_id', 'status_id']
    for col in id_cols:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors='coerce').astype(float)

    # <-- FIN DE LOS CAMBIOS CLAVE -->

    null_counts = result[id_cols].isna().sum()
    if null_counts.any():
        _log_r(f"WARNING: IDs no mapeados:\n{null_counts[null_counts > 0]}")
    else:
        _log_r("Todos los IDs mapeados correctamente.")
    
    return result.drop_duplicates()

# def prepare_date_data(flights_data):
#     new_date_data = flights_data[['YEAR', 'MONTH', 'DAY', 'DAY_OF_WEEK']].drop_duplicates().copy()
#     new_date_data = new_date_data.rename(
#         columns={
#             'YEAR' : 'year',
#             'MONTH' : 'month',
#             'DAY' : 'day',
#             'DAY_OF_WEEK' : 'day_of_week'
#         }
#     )
#     days = {1: 'Monday', 2: 'Tuesday', 3: 'Wednesday', 4: 'Thursday', 5: 'Friday', 6: 'Saturday', 7: 'Sunday'}
#     new_date_data['day_of_week'] = new_date_data['day_of_week'].map(days)
#     return new_date_data

# def prepare_cancellation_data(flights_data):
#     new_cancellation_data = flights_data[['CANCELLATION_REASON']].drop_duplicates().dropna().copy()
#     new_cancellation_data = new_cancellation_data.rename(
#         columns={
#             'CANCELLATION_REASON' : 'cancellation_type'
#         }
#     )

#     return new_cancellation_data

# def _log(msg):
#     print(f"[prepare_flight_data] {msg}")

# def _format_time_series(ser):

#     s = ser.astype(str).str.extract(r'(\d{1,4})', expand=False).fillna('')
#     s = s.str.zfill(4)  # '5'->'0005', '930'->'0930'
#     s = s.where(s != '', pd.NA)

#     formatted = pd.Series(pd.NA, index=ser.index, dtype="object")
#     mask = s.notna()
#     formatted.loc[mask] = s.loc[mask].str.slice(0,2) + ':' + s.loc[mask].str.slice(2,4) + ':00'
#     return formatted

# def prepare_flight_data(flight_data, cancellation_db, date_db, airport_db, airline_db):
#     t0 = _time.time()
#     _log("start (no-merge version)")

#     cols_maybe_needed = [
#         'FLIGHT_NUMBER','TAIL_NUMBER','AIRLINE','ORIGIN_AIRPORT','DESTINATION_AIRPORT',
#         'YEAR','MONTH','DAY','CANCELLATION_REASON',
#         'SCHEDULED_DEPARTURE','SCHEDULED_TIME','DEPARTURE_TIME','DEPARTURE_DELAY',
#         'TAXI_OUT','WHEELS_OFF','ELAPSED_TIME','AIR_TIME','DISTANCE','WHEELS_ON','TAXI_IN',
#         'SCHEDULED_ARRIVAL','ARRIVAL_TIME','ARRIVAL_DELAY','DIVERTED','CANCELLED'
#     ]
#     present_cols = [c for c in cols_maybe_needed if c in flight_data.columns]
#     df = flight_data[present_cols].copy()

#     time_columns = ['SCHEDULED_DEPARTURE','DEPARTURE_TIME','WHEELS_OFF','WHEELS_ON','SCHEDULED_ARRIVAL','ARRIVAL_TIME']
#     for col in time_columns:
#         if col in df.columns:
#             df[col] = _format_time_series(df[col])
#     _log(f"time cols formatted in {_time.time()-t0:.2f}s")


#     date_map = {}
#     if {'date_id','year','month','day'}.issubset(date_db.columns):

#         date_map = { f"{int(r.year)}-{int(r.month)}-{int(r.day)}": int(r.date_id)
#                      for _, r in date_db.iterrows() }
#     else:
#         _log("WARNING: date_db no contiene date_id/year/month/day")

#     airport_map = {}
#     if {'iata_code','airport_id'}.issubset(airport_db.columns):
#         airport_map = dict(zip(airport_db['iata_code'].astype(str).str.upper().str.strip(),
#                                airport_db['airport_id'].astype(int)))
#     else:
#         _log("WARNING: airport_db no contiene iata_code/airport_id")

#     airline_map = {}
#     if {'airline_iata','airline_id'}.issubset(airline_db.columns):
#         airline_map = dict(zip(airline_db['airline_iata'].astype(str).str.upper().str.strip(),
#                                airline_db['airline_id'].astype(int)))
#     else:
#         _log("WARNING: airline_db no contiene airline_iata/airline_id")

#     cancellation_map = {}
#     if {'cancellation_id','cancellation_type'}.issubset(cancellation_db.columns):
#         cancellation_map = dict(zip(cancellation_db['cancellation_type'].astype(str),
#                                     cancellation_db['cancellation_id'].astype(int)))
#     else:
#         _log("WARNING: cancellation_db no contiene cancellation_type/cancellation_id")


#     if {'YEAR','MONTH','DAY'}.issubset(df.columns) and date_map:
#         date_key = df['YEAR'].astype(str).str.strip() + '-' + df['MONTH'].astype(str).str.strip() + '-' + df['DAY'].astype(str).str.strip()
#         df['date_id'] = date_key.map(date_map)
#     else:
#         df['date_id'] = pd.NA
#     _log(f"date_id mapped in {_time.time()-t0:.2f}s")


#     if 'ORIGIN_AIRPORT' in df.columns:
#         df['origin_airport_id'] = df['ORIGIN_AIRPORT'].astype(str).str.extract(r'([A-Za-z0-9]+)', expand=False).fillna('').str.upper().map(airport_map)
#     else:
#         df['origin_airport_id'] = pd.NA

#     if 'DESTINATION_AIRPORT' in df.columns:
#         df['destination_airport_id'] = df['DESTINATION_AIRPORT'].astype(str).str.extract(r'([A-Za-z0-9]+)', expand=False).fillna('').str.upper().map(airport_map)
#     else:
#         df['destination_airport_id'] = pd.NA

#     if 'AIRLINE' in df.columns:
#         df['airline_id'] = df['AIRLINE'].astype(str).str.extract(r'([A-Za-z0-9]+)', expand=False).fillna('').str.upper().map(airline_map)
#     else:
#         df['airline_id'] = pd.NA

#     if 'CANCELLATION_REASON' in df.columns:
#         df['cancellation_id'] = df['CANCELLATION_REASON'].map(cancellation_map)
#     else:
#         df['cancellation_id'] = pd.NA

#     _log(f"airports/airline/cancellation mapped in {_time.time()-t0:.2f}s")

#     num_cols = ['SCHEDULED_TIME','DEPARTURE_DELAY','TAXI_OUT','ELAPSED_TIME','AIR_TIME','DISTANCE','TAXI_IN','ARRIVAL_DELAY','FLIGHT_NUMBER']
#     for nc in num_cols:
#         if nc in df.columns:
#             df[nc] = pd.to_numeric(df[nc], errors='coerce')

#     if 'DISTANCE' in df.columns:
#         df['DISTANCE'] = pd.to_numeric(df['DISTANCE'], errors='coerce').astype('Float32')
#     if 'AIR_TIME' in df.columns:
#         df['AIR_TIME'] = pd.to_numeric(df['AIR_TIME'], errors='coerce').astype('Float32')
#     if 'FLIGHT_NUMBER' in df.columns:
#         df['FLIGHT_NUMBER'] = pd.to_numeric(df['FLIGHT_NUMBER'], errors='coerce').astype('Int32')


#     rename_map = {
#         'FLIGHT_NUMBER':'flight_number',
#         'TAIL_NUMBER':'aircraft_id',
#         'SCHEDULED_DEPARTURE':'scheduled_departure',
#         'SCHEDULED_TIME':'scheduled_time',
#         'DEPARTURE_TIME':'departure_time',
#         'DEPARTURE_DELAY':'departure_delay',
#         'TAXI_OUT':'taxi_out',
#         'WHEELS_OFF':'wheels_off',
#         'ELAPSED_TIME':'elapsed_time',
#         'AIR_TIME':'air_time',
#         'DISTANCE':'distance',
#         'WHEELS_ON':'wheels_on',
#         'TAXI_IN':'taxi_in',
#         'SCHEDULED_ARRIVAL':'scheduled_arrival',
#         'ARRIVAL_TIME':'arrival_time',
#         'ARRIVAL_DELAY':'arrival_delay',
#         'DIVERTED':'is_diverted',
#         'CANCELLED':'is_cancelled'
#     }

#     df = df.rename(columns=rename_map)


#     needed_columns = ['flight_number','aircraft_id','airline_id','origin_airport_id','destination_airport_id',
#                       'date_id','cancellation_id','scheduled_departure','scheduled_time','departure_time','departure_delay',
#                       'taxi_out','wheels_off','elapsed_time','air_time','distance','wheels_on','taxi_in',
#                       'scheduled_arrival','arrival_time','arrival_delay','is_diverted','is_cancelled']

#     existing_final_cols = [c for c in needed_columns if c in df.columns]
#     result = df[existing_final_cols].copy()

#     show_null_summary(result, ['scheduled_time','cancellation_id','origin_airport_id','destination_airport_id','air_time','arrival_delay'])
#     result = drop_missing_scheduled_time(result, save_dropped=True, dropped_path='dropped_missing_scheduled_time.csv')

#     result = postprocess_cancellations(result, cancellation_db=None, add_unknown=False)
#     show_null_summary(result, ['scheduled_time','cancellation_id','origin_airport_id','destination_airport_id','air_time','arrival_delay'])

#     _log(f"done in {_time.time()-t0:.2f}s, result shape: {result.shape}")
#     return result

# def drop_missing_scheduled_time(df, save_dropped=True, dropped_path='dropped_missing_scheduled_time.csv'):
  
#     import os
#     total = len(df)
#     if 'scheduled_time' not in df.columns:
#         return df.copy()

#     missing = df['scheduled_time'].isna().sum()
#     print(f"[drop_missing_scheduled_time] total rows before = {total}, missing scheduled_time = {missing}")

#     if missing == 0:
#         return df.copy()

   
#     kept_df = df[df['scheduled_time'].notna()].copy()

#     return kept_df

    

# def postprocess_cancellations(df, cancellation_db=None, add_unknown=False):
#     """
#     Asegura coherencia entre is_cancelled y cancellation_id:
#     - si is_cancelled == 0 => cancellation_id = NULL
#     - si is_cancelled == 1 y cancellation_id es NULL => si add_unknown=True crea/mapea 'Unknown' (se necesita cancellation_db mutable fuera)
#     - si cancellation_db no se pasa, no crea ids
#     """
#     # Normalizar is_cancelled a 0/1 ints
#     if 'is_cancelled' in df.columns:
#         df['is_cancelled'] = pd.to_numeric(df['is_cancelled'], errors='coerce').fillna(0).astype(int)

#     # Si hay filas no canceladas, asegurarse cancellation_id es NULL
#     if 'cancellation_id' in df.columns:
#         df.loc[df['is_cancelled'] == 0, 'cancellation_id'] = pd.NA

#         # Si se pide, mapear valores faltantes de cancel_id a un "Unknown" en cancellation_db
#         if add_unknown and cancellation_db is not None:
#             # comprobar si ya existe cancellation_type 'Unknown' (o 'OTHER')
#             unk = cancellation_db[cancellation_db['cancellation_type'].str.lower().isin(['unknown','other','not specified'])]
#             if len(unk) > 0:
#                 unk_id = int(unk.iloc[0]['cancellation_id'])
#             else:
#                 print("[postprocess_cancellations] WARNING: add_unknown=True pero no se insertó 'Unknown' en cancellation_db automáticamente. Inserta manualmente en la dimensión o pásala al cargar.")
#                 unk_id = None
#             if unk_id is not None:
#                 df.loc[(df['is_cancelled'] == 1) & (df['cancellation_id'].isna()), 'cancellation_id'] = int(unk_id)

#     return df

# def show_null_summary(df, cols=None):
#     """Imprime resumen de nulos por columna (top) y total filas."""
#     total = len(df)
#     print(f"[null summary] total rows = {total}")
#     if cols is None:
#         cols = df.columns.tolist()
#     nulls = df[cols].isnull().sum().sort_values(ascending=False)
#     print(nulls[nulls > 0].to_string())

