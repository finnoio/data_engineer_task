import json
from os import environ
from time import sleep
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, BigInteger, DateTime
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine import Engine
import pandas as pd
from h3 import point_dist


def extract_increment(psql_engine: Engine, current_ts: str):
    with psql_engine.begin() as connection:
        increment_query = f"""
            select device_id,
               temperature,
               location,
               to_timestamp(time::int) as event_dt
            from devices
            where to_timestamp(time::int)
             between '{current_ts}'::timestamp - interval '59 minutes 59 seconds'
              and '{current_ts}'::timestamp
            order by time
        """
        increment = pd.read_sql(increment_query, connection)
    return increment


def transform_increment(increment_df: pd.DataFrame, current_ts: datetime) -> pd.DataFrame:
    increment_df['location_lead'] = (increment_df.sort_values(by=['event_dt'], ascending=True)
                                     .groupby(['device_id'])['location'].shift(-1)).fillna(increment_df['location'])

    increment_df['distance'] = increment_df.apply(lambda x: calculate_distance(x['location'], x['location_lead']),
                                                  axis=1)

    increment_df = increment_df.groupby('device_id').agg(
        {
            'temperature': ['max'],
            'location': ['count'],
            'distance': ['sum']
        }
    )

    increment_df.columns = ['max_temperature', 'data_point_cnt', 'total_distance_km']
    increment_df['extract_dt'] = current_ts
    return increment_df


def load_increment(increment_df: pd.DataFrame, current_ts: str, mysql_engine: Engine) -> int:
    with mysql_engine.begin() as connection:
        # delete previously loaded data for idempotency
        connection.execute(f"""
            delete
            from devices_agg_data
            where extract_dt
                  between CAST('{current_ts}' as datetime) - interval 59 minute - interval 59 second
                  and CAST('{current_ts}' as datetime)
            """)
        inserted_rows = increment_df.to_sql('devices_agg_data', con=connection, if_exists='append')

    return inserted_rows


def calculate_distance(location: str, location_lead: str) -> int:
    location = json.loads(location)
    location_lead = json.loads(location_lead)
    return int(point_dist(
        (float(location['latitude']), float(location['longitude'])),
        (float(location_lead['latitude']), float(location_lead['longitude']))
    ))



if __name__ == '__main__':
    print('Waiting for the data generator...', flush=True)
    sleep(20)
    print('ETL Starting...', flush=True)

    while True:
        try:
            psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
            mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
            metadata_obj = MetaData()
            devices_agg_data = Table(
                'devices_agg_data', metadata_obj,
                Column('device_id', String(40)),
                Column('max_temperature', Integer),
                Column('data_point_cnt', Integer),
                Column('total_distance_km', BigInteger),
                Column('extract_dt', DateTime),
            )
            metadata_obj.create_all(mysql_engine)
            break
        except OperationalError:
            sleep(0.1)
    print('Connection to PostgresSQL/MySQL successful.', flush=True)
    current_ts = datetime.now().replace(second=0, microsecond=0, minute=0)
    while True:
        next_ts = current_ts + timedelta(hours=1)
        if datetime.now() > next_ts:
            current_ts = next_ts
            increment_df = extract_increment(psql_engine, str(current_ts))
            print(f'Increment received, current timestamp = {current_ts}', flush=True)

            transformed_increment = transform_increment(increment_df, current_ts)
            print(f'Increment transformed successfully', flush=True)
            print(f'Increment load starting...', flush=True)
            inserted_rows = load_increment(transformed_increment, str(current_ts), mysql_engine)
            print(f'Increment load finished, inserted rows: {inserted_rows}', flush=True)
