
import pandas as pd, os, argparse
from sqlalchemy import create_engine
from time import time

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db_name
    table_name = params.table_name
    url = params.url_csv
    gz_name = 'output.csv.gz'
    csv_name = 'output.csv'

    os.system(f"wget {url} -O {gz_name}" )
    os.system(f"gunzip {csv_name}" )

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df = pd.read_csv(csv_name, nrows=0, low_memory=False)

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, low_memory=False)

    #df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    while True:
        t_start = time()

        df = next(df_iter)

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('chunk inserted..., took %.3f second' % (t_end - t_start))

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest data from CSV to a Postgres database')

    parser.add_argument('--user', help='user name for postgres')  # user name for postgres
    parser.add_argument('--password', help='password for postgres')  # password for postgres
    parser.add_argument('--host', help='host of the postgres database') # host of the postgres database
    parser.add_argument('--port', help='port of the postgres database') # port of the postgres database
    parser.add_argument('--db_name', help='database name of the postgres database') # database name of the postgres database
    parser.add_argument('--table_name', help='table name in the postgres database') # table name in the postgres database
    parser.add_argument('--url_csv', help='url of the csv file') # url of the csv file

    args = parser.parse_args()

    main(args)




