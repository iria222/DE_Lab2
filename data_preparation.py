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



def prepare_qualifying_data(qualifying_data, drivers_csv, constructors_csv, races_csv, circuits_csv,
                            circuit_db, constructor_db, race_db, driver_db):
    """
    Prepara datos de qualifying mapeando Driver, Constructor, Race y Circuit 
    mediante claves de negocio.
    """
    _log_q("Preparando datos de qualifying (con circuit_id por nombre de circuito)...")
    
    df = qualifying_data.copy()
    
    drivers_csv_temp = drivers_csv[['driverId', 'forename', 'surname', 'dob']].rename(columns={
        'forename': 'driver_name', 'surname': 'driver_surname', 'dob': 'date_of_birth'
    })
    drivers_csv_temp['date_of_birth'] = pd.to_datetime(drivers_csv_temp['date_of_birth'], errors='coerce')
    constructors_csv_temp = constructors_csv[['constructorId', 'name']].rename(columns={'name': 'constructor_name'})
    races_csv_temp = races_csv[['raceId', 'year', 'name']].rename(columns={'name': 'race_name'})
    race_circuit_names = races_csv[['raceId', 'circuitId']].merge(
        circuits_csv[['circuitId', 'name']], on='circuitId', how='left'
    ).rename(columns={'name': 'circuit_name'}).drop(columns=['circuitId'])

    
    df = df.merge(drivers_csv_temp, on='driverId', how='left')
    df = df.merge(constructors_csv_temp, on='constructorId', how='left')
    df = df.merge(races_csv_temp, on='raceId', how='left')
    df = df.merge(race_circuit_names, on='raceId', how='left')
    df.drop(columns=['driverId', 'constructorId', 'raceId'], inplace=True, errors='ignore')
    
    # Eliminar filas donde la clave de negocio (circuit_name) es nula
    df = remove_rows_with_missing_keys(df, key_columns=['circuit_name'], logger_func=_log_q)
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


def prepare_pit_stops_data(pit_stops_data, drivers_csv, constructors_csv, races_csv,
                           constructor_db, race_db, driver_db):
   
    _log_p("Preparando datos de pit stops (solo claves de negocio)...")
    
    df = pit_stops_data.copy()
    

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
