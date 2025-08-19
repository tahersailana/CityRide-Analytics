import pandas as pd
import logging


def parse_timestamps(df, timestamp_cols):
    logging.info(f"parse_timestamps started. Columns: {list(df.columns)} | Timestamp cols: {timestamp_cols}")
    for col in timestamp_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', utc=True)
    logging.info(f"parse_timestamps result shape: {df.shape}, dtypes: {df.dtypes.to_dict()}")
    return df

def correct_numeric_types(df, numeric_cols):
    logging.info(f"correct_numeric_types started. Columns: {list(df.columns)} | Numeric cols: {numeric_cols}")
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    logging.info(f"correct_numeric_types result shape: {df.shape}, dtypes: {df.dtypes.to_dict()}")
    return df

def filter_invalid_trips(df):
    logging.info(f"filter_invalid_trips started. Columns: {list(df.columns)}")
    if 'fare_amount' in df.columns:
        df = df[df['fare_amount'] >= 0]
    if 'trip_distance' in df.columns:
        df = df[df['trip_distance'] > 0]
    if 'passenger_count' in df.columns:
        df = df[df['passenger_count'] > 0]
    logging.info(f"filter_invalid_trips result shape: {df.shape}, dtypes: {df.dtypes.to_dict()}")
    return df

TABLE_COLUMNS = [
    'pickup_datetime','dropoff_datetime','passenger_count','trip_distance',
    'payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount',
    'improvement_surcharge','total_amount','congestion_surcharge','airport_fee',
    'ehail_fee','trip_type','vendor_id','ratecode_id','store_and_fwd_flag',
    'pu_location_id','do_location_id','hvfhs_license_num','dispatching_base_num',
    'originating_base_num','affiliated_base_number','trip_miles','trip_time',
    'base_passenger_fare','bcf','sales_tax','tips','driver_pay','shared_request_flag',
    'shared_match_flag','access_a_ride_flag','wav_request_flag','wav_match_flag','sr_flag'
]

# Mapping for each file type
COLUMN_MAP = {
    'fhv': {
        'dispatching_base_num': 'dispatching_base_num',
        'pickup_datetime': 'pickup_datetime',
        'dropOff_datetime': 'dropoff_datetime',
        'PUlocationID': 'pu_location_id',
        'DOlocationID': 'do_location_id',
        'SR_Flag': 'sr_flag',
        'Affiliated_base_number': 'affiliated_base_number'
    },
    'green': {
        'VendorID': 'vendor_id',
        'lpep_pickup_datetime': 'pickup_datetime',
        'lpep_dropoff_datetime': 'dropoff_datetime',
        'store_and_fwd_flag': 'store_and_fwd_flag',
        'RatecodeID': 'ratecode_id',
        'PULocationID': 'pu_location_id',
        'DOLocationID': 'do_location_id',
        'passenger_count': 'passenger_count',
        'trip_distance': 'trip_distance',
        'fare_amount': 'fare_amount',
        'extra': 'extra',
        'mta_tax': 'mta_tax',
        'tip_amount': 'tip_amount',
        'tolls_amount': 'tolls_amount',
        'ehail_fee': 'ehail_fee',
        'improvement_surcharge': 'improvement_surcharge',
        'total_amount': 'total_amount',
        'payment_type': 'payment_type',
        'trip_type': 'trip_type',
        'congestion_surcharge': 'congestion_surcharge'
    },
    'yellow': {
        'VendorID': 'vendor_id',
        'tpep_pickup_datetime': 'pickup_datetime',
        'tpep_dropoff_datetime': 'dropoff_datetime',
        'passenger_count': 'passenger_count',
        'trip_distance': 'trip_distance',
        'RatecodeID': 'ratecode_id',
        'store_and_fwd_flag': 'store_and_fwd_flag',
        'PULocationID': 'pu_location_id',
        'DOLocationID': 'do_location_id',
        'payment_type': 'payment_type',
        'fare_amount': 'fare_amount',
        'extra': 'extra',
        'mta_tax': 'mta_tax',
        'tip_amount': 'tip_amount',
        'tolls_amount': 'tolls_amount',
        'improvement_surcharge': 'improvement_surcharge',
        'total_amount': 'total_amount',
        'congestion_surcharge': 'congestion_surcharge',
        'Airport_fee': 'airport_fee'
    },
    'fhvhv': {
        'hvfhs_license_num': 'hvfhs_license_num',
        'dispatching_base_num': 'dispatching_base_num',
        'originating_base_num': 'originating_base_num',
        'request_datetime': 'request_datetime',
        'on_scene_datetime': 'on_scene_datetime',
        'pickup_datetime': 'pickup_datetime',
        'dropoff_datetime': 'dropoff_datetime',
        'PULocationID': 'pu_location_id',
        'DOLocationID': 'do_location_id',
        'trip_miles': 'trip_miles',
        'trip_time': 'trip_time',
        'base_passenger_fare': 'base_passenger_fare',
        'tolls': 'tolls_amount',
        'bcf': 'bcf',
        'sales_tax': 'sales_tax',
        'congestion_surcharge': 'congestion_surcharge',
        'airport_fee': 'airport_fee',
        'tips': 'tips',
        'driver_pay': 'driver_pay',
        'shared_request_flag': 'shared_request_flag',
        'shared_match_flag': 'shared_match_flag',
        'access_a_ride_flag': 'access_a_ride_flag',
        'wav_request_flag': 'wav_request_flag',
        'wav_match_flag': 'wav_match_flag'
    },
}

def map_columns_to_table(df: pd.DataFrame, file_type: str, table_columns=TABLE_COLUMNS) -> pd.DataFrame:
    """
    Rename and reorder columns to match the table schema.
    Adds missing columns as None.
    """
    # Rename columns according to file type
    col_map = COLUMN_MAP[file_type]
    df = df.rename(columns=col_map)
    
    # Add missing columns
    for col in table_columns:
        if col not in df.columns:
            df[col] = None
    
    # Reorder columns to match table
    df = df[table_columns]
    
    return df