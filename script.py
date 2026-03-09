import sqlite3
import pandas as pd

def populate_database():

    conn = sqlite3.connect('shipment_database.db')
    cursor = conn.cursor()


    cursor.execute('''
    CREATE TABLE IF NOT EXISTS shipments (
        origin_warehouse TEXT,
        destination_store TEXT,
        product TEXT,
        on_time BOOLEAN,
        product_quantity INTEGER,
        driver_identifier TEXT
    )
    ''')


    df0 = pd.read_csv('data/shipping_data_0.csv')
    df0.to_sql('shipments', conn, if_exists='append', index=False)
    print(f"Successfully inserted {len(df0)} rows from Spreadsheet 0.")


    df1 = pd.read_csv('data/shipping_data_1.csv')
    df2 = pd.read_csv('data/shipping_data_2.csv')


    merged_df = pd.merge(df1, df2, on='shipment_identifier')


    grouped = merged_df.groupby([
        'origin_warehouse', 
        'destination_store', 
        'product', 
        'on_time', 
        'driver_identifier'
    ]).size().reset_index(name='product_quantity')


    final_df12 = grouped[[
        'origin_warehouse', 
        'destination_store', 
        'product', 
        'on_time', 
        'product_quantity', 
        'driver_identifier'
    ]]


    final_df12.to_sql('shipments', conn, if_exists='append', index=False)
    print(f"Successfully inserted {len(final_df12)} consolidated rows from Spreadsheets 1 & 2.")


    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM shipments")
    total = cursor.fetchone()[0]
    print(f"ETL Pipeline Complete. Total rows in database: {total}")
    
    conn.close()

if __name__ == "__main__":
    populate_database()