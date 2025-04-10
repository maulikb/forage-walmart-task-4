import sqlite3
import csv
import os

# Database connection
db_path = "/workspaces/forage-walmart-task-4/data/shipping_data.db"

# Function to initialize the database and create tables if they don't exist
def initialize_database():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Create the shipments table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS shipments (
            shipment_id TEXT,
            origin_warehouse TEXT,
            destination_store TEXT,
            product TEXT,
            on_time TEXT,
            product_quantity INTEGER,
            driver_identifier TEXT
        )
    """)
    conn.commit()
    conn.close()

# Function to insert data from shipping_data_0.csv
def insert_spreadsheet_0(file_path):
    if not os.path.exists(file_path):
        print(f"Error: File not found - {file_path}")
        return
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            cursor.execute("""
                INSERT INTO shipments (origin_warehouse, destination_store, product, on_time, product_quantity, driver_identifier)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                row['origin_warehouse'],
                row['destination_store'],
                row['product'],
                row['on_time'],
                row['product_quantity'],
                row['driver_identifier']
            ))
    conn.commit()
    conn.close()

# Function to insert data from shipping_data_1.csv and shipping_data_2.csv
def insert_spreadsheet_1_and_2(file_path_1, file_path_2):
    if not os.path.exists(file_path_1):
        print(f"Error: File not found - {file_path_1}")
        return
    if not os.path.exists(file_path_2):
        print(f"Error: File not found - {file_path_2}")
        return

    # Load spreadsheet 2 into a dictionary for quick lookup
    shipment_data = {}
    with open(file_path_2, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            shipment_data[row['shipment_identifier']] = {
                'origin_warehouse': row['origin_warehouse'],
                'destination_store': row['destination_store'],
                'driver_identifier': row['driver_identifier']
            }

    # Process spreadsheet 1 and insert combined data
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    with open(file_path_1, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            shipment_id = row['shipment_identifier']
            if shipment_id in shipment_data:
                origin = shipment_data[shipment_id]['origin_warehouse']
                destination = shipment_data[shipment_id]['destination_store']
                driver = shipment_data[shipment_id]['driver_identifier']
                cursor.execute("""
                    INSERT INTO shipments (shipment_id, product, on_time, origin_warehouse, destination_store, driver_identifier)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    shipment_id,
                    row['product'],
                    row['on_time'],
                    origin,
                    destination,
                    driver
                ))
    conn.commit()
    conn.close()

# Initialize the database
initialize_database()

# File paths
spreadsheet_0_path = "/workspaces/forage-walmart-task-4/data/shipping_data_0.csv"
spreadsheet_1_path = "/workspaces/forage-walmart-task-4/data/shipping_data_1.csv"
spreadsheet_2_path = "/workspaces/forage-walmart-task-4/data/shipping_data_2.csv"

# Insert data
insert_spreadsheet_0(spreadsheet_0_path)
insert_spreadsheet_1_and_2(spreadsheet_1_path, spreadsheet_2_path)