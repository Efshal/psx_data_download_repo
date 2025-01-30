"""
MACH FORTY TWO 2025

Download Close Price and maintain 

Every day a few hours after stock market closes the PSX publishes a
market summary. This Summary has a lot of data, but for now we only
use it to maintain our 10 year history of close price and volume.
This script is meant to be run around 6 or 7pm on a weekday.
It will check our db and see what day(s) info is missing, and then
makes the corresponding request to the PSX to get it.
Response from PSX is saved, parsed and close price info is inserted into db.
We also save .pkl file containing full market summary in case needed.

Copyright (C) Mach 42 - All Rights Reserved
Unauthorized copying of this file, via any medium is strictly prohibited
Proprietary and confidential
Written by Murtaza Tunio - JAN 2025
Contact: tuniomurtaza (at) gmail.com
"""

import os
import sqlite3
import zipfile
import requests
import pandas as pd
import datetime as dt

# Set pandas options
pd.set_option('display.max_columns', 20)
pd.set_option('display.max_rows', 20)

# Database path
DB_PATH = os.getenv('DB_PATH', 'close_price_and_volume_daily.db')

def fetch_tables(db_file=DB_PATH):
    """
    Reads the sqlite db and fetches the price and volume tables.
    """
    connection = sqlite3.connect(db_file)
    price = pd.read_sql("SELECT * FROM price", connection, index_col='datetime')
    price.index = pd.to_datetime(price.index)
    price.sort_index()
    price.reindex(sorted(price.columns), axis=1)
    
    volume = pd.read_sql("SELECT * FROM volume", connection, index_col='datetime')
    volume.index = pd.to_datetime(volume.index)
    volume.sort_index()
    volume.reindex(sorted(volume.columns), axis=1)
    
    connection.close()
    return price, volume

def fetch_next_weekday(price, volume, date_to_get=None):
    """
    Reads price and volume tables and checks latest date in the table.
    Returns next date to fetch.
    """
    max_date_px = price.index.max()
    max_date_vo = volume.index.max()
    assert max_date_px == max_date_vo
    last_clp_date = max_date_px
    
    next_clp_date = last_clp_date + dt.timedelta(days=1)
    if next_clp_date.weekday() >= 5:  # Saturday=5, Sunday=6
        add_diff = 7 - next_clp_date.weekday()
        next_clp_date += dt.timedelta(days=add_diff)
    
    if date_to_get is None:
        return next_clp_date
    else:
        return pd.to_datetime(date_to_get)

def fetch_save_close_price_table(price, volume):
    """
    Fetches one day of close price data from PSX based on next missing day.
    Saves to directory 'day_raw' and returns path only.
    """
    base_url = "https://dps.psx.com.pk/download/mkt_summary/{DATE_HERE}.Z"
    next_weekday = fetch_next_weekday(price, volume)
    assert next_weekday.weekday() < 5  # Ensure not a weekend
    assert next_weekday <= dt.datetime.today()  # Ensure not future date
    date_to_fetch = next_weekday.strftime("%Y-%m-%d")
    dated_fetch_url = base_url.replace("{DATE_HERE}", date_to_fetch)
    
    # Download the file
    path_save = os.path.join('day_raw', "file.zip")
    response = requests.get(dated_fetch_url)
    with open(path_save, "wb") as file:
        file.write(response.content)
    
    # Extract and cleanup
    with zipfile.ZipFile(path_save, 'r') as zip_ref:
        file_names = zip_ref.namelist()
        assert len(file_names) == 1
        zip_ref.extractall('day_raw')
    old_path = os.path.join('day_raw', file_names[0])
    new_path = os.path.join('day_raw', date_to_fetch + ".lis")
    os.rename(old_path, new_path)
    os.remove(path_save)
    
    return new_path

def parse_downloaded_table(path_to_lis_file):
    """
    Reads *.lis file downloaded as CSV into memory.
    Names columns, saves dataframe as .pkl file.
    Also returns this table.
    """
    daily_in = pd.read_csv(path_to_lis_file, delimiter='|', header=None)
    daily_in = daily_in.drop(labels=[10, 11, 12], axis=1)
    daily_in.columns = ['datetime', 'symbol', 'sector', 'name', 'open', 'high', 'low', 'close', 'volume', 'ldclp']
    daily_in['datetime'] = pd.to_datetime(daily_in['datetime'])
    path_out = path_to_lis_file.replace('.lis', '.pkl')
    daily_in.to_pickle(path_out)
    
    return daily_in

def check_columns_db(daily_in):
    """
    Add missing columns (new symbols) to db.
    """
    assert daily_in['datetime'].nunique() == 1
    symbols = daily_in['symbol'].tolist()
    
    # Ensure all symbols are in the table
    db_file = DB_PATH
    connection = sqlite3.connect(db_file)
    cursor = connection.cursor()
    
    for table_name in ['price', 'volume']:
        added = []
        already = 0
        for symbol in symbols:
            alter_query = f'ALTER TABLE {table_name} ADD COLUMN "{symbol}" REAL'
            try:
                cursor.execute(alter_query)
                connection.commit()
                added.append(symbol)
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e):
                    already += 1
                else:
                    raise e
        print(f"{table_name.capitalize()}: Added {len(added)}, Already Present {already}, Total {len(symbols)}")
    
    connection.close()

def insert_data_db(daily_in):
    """
    Insert price and volume info to the tables in db.
    """
    data_date = str(daily_in['datetime'].iloc[0].normalize())
    prices = daily_in['close'].tolist()
    volumes = daily_in['volume'].tolist()
    symbols = daily_in['symbol'].tolist()
    
    db_file = DB_PATH
    connection = sqlite3.connect(db_file)
    cursor = connection.cursor()
    
    # Prepare symbols for SQL insertion
    formatted_symbols = [f'"{symbol}"' if '-' in symbol or '786' in symbol else symbol for symbol in symbols]
    sym_part = "(datetime, " + ", ".join(formatted_symbols) + ")"
    qs = "(" + ", ".join("?" * (len(formatted_symbols) + 1)) + ")"
    
    # Insert price data
    insert_query = f'INSERT INTO price {sym_part} VALUES {qs}'
    data = [data_date] + prices
    cursor.execute(insert_query, data)
    connection.commit()
    
    # Insert volume data
    insert_query = f'INSERT INTO volume {sym_part} VALUES {qs}'
    data = [data_date] + volumes
    cursor.execute(insert_query, data)
    connection.commit()
    
    connection.close()
    print(f"Updated {data_date}!")

def delete_last_row(table_name='price'):
    """
    Only for testing, deletes last row in the db.
    """
    db_path = DB_PATH
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"SELECT datetime FROM {table_name} ORDER BY datetime DESC LIMIT 1;")
        last_row = cursor.fetchone()
    
        if last_row:
            last_row_id = last_row[0]
            cursor.execute(f"DELETE FROM {table_name} WHERE datetime = ?;", (last_row_id,))
            conn.commit()
            print(f"Deleted the last row with id {last_row_id} in {table_name}.")
        else:
            print("The table is empty. No row to delete.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    price, volume = fetch_tables()  # Get saved data in memory
    lis_file_path = fetch_save_close_price_table(price, volume)  # Get close price from PSX
    daily_in = parse_downloaded_table(lis_file_path)  # Parse and save
    check_columns_db(daily_in)  # Add missing symbols to db before insert
    insert_data_db(daily_in)  # Insert data into db
