import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
from lib_synapse import get_absolute_path,create_config,read_config

host='localhost'
user='root'
password=read_config()['my_sql_password']
database='synapse'

def mysql_create_Db(mydb):
    
    mycursor = mydb.cursor()
    mycursor.execute(f'create database if not exists {database}')
    mydb.commit()
    print(f"Database - {database} ready")
    mycursor.execute(f'use {database}')
    
    if mydb.is_connected():
        mycursor.close()
        mydb.close()

def mysql_insert_df(df,table):

    try:
        mydb = mysql.connector.connect(
            host='localhost',
            user='root',
            password=read_config()['my_sql_password'],
        )

        mysql_create_Db(mydb)
        
        # Create a SQLAlchemy engine to connect to the MySQL database
        engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')

        # Convert the Pandas DataFrame to a format for MySQL table insertion
        df.to_sql(table, con=engine, if_exists='replace', index=False)

        print(f"Table - {table} updated")

    except mysql.connector.Error as e:
        print(f"Error while connecting to MySQL: {e}")

def main(file_to_load):

    df = pd.read_parquet(get_absolute_path(file_to_load))
    
    if file_to_load == 'NYCTripSmall.parquet':
        table = 'nyctaxitripsmall'
        final_df = df[['VendorID','store_and_fwd_flag','RatecodeID','PULocationID','DOLocationID','passenger_count','trip_distance','fare_amount','extra','mta_tax','tip_amount','tolls_amount','ehail_fee','improvement_surcharge','total_amount','payment_type','trip_type','congestion_surcharge']]
    else:
        table = 'nyctaxitrip'
        final_df = df.head(100)
    
    mysql_insert_df(final_df, table)
    final_df.to_json(f'files/{table}.json',orient="records")
    
    print(f"JSON - {table} updated")

if __name__ == '__main__':
    main(file_to_load = 'NYCTrip.parquet')