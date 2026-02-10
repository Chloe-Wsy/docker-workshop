import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click 


dtype = {"store_and_fwd_flag": "string"}
parse_dates = ["tpep_pickup_datetime","tpep_dropoff_datetime"]

def ingest_data(url:str, engine, target_table:str, chunksize: int=100000,) -> pd.DataFrame:
    df_iter = pd.read_csv(
            url,
            dtype=dtype,
            parse_dates=parse_dates,
            iterator = True,
            chunksize = chunksize)

    first_chunk = next(df_iter)
    first_chunk.head(n=0).to_sql(name=target_table, con=engine, if_exists='replace')
    print(f"Table {target_table} created")

    first_chunk.to_sql(name=target_table, con=engine, if_exists='append')
    print("Inserted first chunk:", len(first_chunk))
    
    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(name=target_table, con=engine, if_exists='append')
        print("Inserted chunk:", len(df_chunk))

    print(f"Done ingesting to {target_table}")

@click.command()
@click.option("--pg-user", default='root', help="PostgreSQL username")
@click.option("--pg-pass", default='root', help="PostgreSQL password")
@click.option("--pg-host", default='localhost', help="PostgreSQL host")
@click.option("--pg-port", default=5432, type=int, help="PostgreSQL port")
@click.option("--pg-db", default='ny_taxi', help="PostgreSQL database name")
@click.option("--year", default=2021, type=int, help="Year of the taxi data.")
@click.option("--month", default=1, type=int, help="Month of the taxi data.")
@click.option("--chunksize", default=100000, type=int, help="Rows per chunk to ingest.")
@click.option("--target-table", default='yellow_taxi_data', help="Taget table name")\q
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'
    url = f'{url_prefix}/yellow_tripdata_{year:04d}-{month:02d}.csv.gz'
    ingest_data(url=url, engine=engine, target_table=target_table,chunksize=chunksize)

if __name__ == '__main__':
    main()


