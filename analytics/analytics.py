import json
from os import environ
from time import sleep
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, BigInteger, DateTime
from sqlalchemy.exc import OperationalError
from sqlalchemy.engine.mock import MockConnection
import pandas as pd
from h3 import point_dist


def extract_increment(psql_conn: MockConnection, current_ts: str):
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
    return pd.read_sql(increment_query, psql_conn)


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


def load_increment(increment_df: pd.DataFrame, current_ts: str, mysql_engine: MockConnection) -> int :
    with mysql_engine.begin() as connection:
        # delete previously loaded data for idempotency
        mysql_conn.execute(f"""
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



# if __name__ == '__main__':
print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')
# # TODO: remove
# import os
# os.environ['TZ'] = 'UTC'
# # TODO: remove

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
        # psql_engine = create_engine('postgresql+psycopg2://postgres:password@localhost:6543/main', pool_pre_ping=True, pool_size=10)
        # mysql_engine = create_engine('mysql+pymysql://nonroot:nonroot@localhost:3306/analytics?charset=utf8', pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL/MySQL successful.')
mysql_conn, psql_conn = mysql_engine.connect(), psql_engine.connect()
current_ts = datetime.now() #.replace(second=0, microsecond=0, minute=0)
while True:
    if datetime.now() == current_ts + timedelta(minutes=5):
        current_ts += timedelta(minutes=5)
        increment_df = extract_increment(psql_conn, str(current_ts))
        print(f'Increment received, current timestamp = {current_ts}')

        transformed_increment = transform_increment(increment_df, current_ts)
        print(f'Increment transformed successfully')
        print(f'Increment load starting...')
        inserted_rows = load_increment(transformed_increment, str(current_ts), mysql_engine)
        print(f'Increment load finished, inserted rows: {inserted_rows}')


        # print(tabulate.tabulate(transformed_increment.sort_values(by=['device_id'], ascending=True), headers='keys', tablefmt='psql'))
