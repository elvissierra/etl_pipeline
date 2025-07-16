# Designing ETL Pipeline

#- Data source- csv
#- Transformation- include cleaning(removing null values) and filtering (based on criteria)
#- Data Destination- database, csv, or any other storage

# Data Extraction

import pandas as pd

def extract_data(file_path="data/Test_Data.csv"):
    data = pd.read_csv(file_path)
    return data

d = extract_data() # produces readable data
col = d["All customer-suggested fields edited?"] # stores the column "" into the variable col

# Data Transformation
#print(col) # print the col data

def transform_data(series: pd.Series) -> pd.Series:
    cleaned = series.dropna() # remove the null values
    filtered = cleaned[cleaned != "Yes"] # filter data to remove Yes values
    return filtered

transformed_data = transform_data(col) # process the col variable from extract_data through transform_data
for td in transformed_data: # iterate over indexes
    if td == "Yes": # conditional
        continue # skip due to conditional
    print(td)

# Data Loading
import sqlite3

def load_data(transformed_data, db_path): # parameters
    conn = sqlite3.conn(db_path) # database connection
    cursor = conn.cursor() # craeate connection
    #create database with fields
    cursor.execute = ("""
    CREATE TABLE IF NOT EXISTS report_data (
        place_id INTEGER PRIMARY KEY,
        edited_fields TEXT,
        last_editor_resolution TEXT
        suggested_fields TEXT
        ticket_type TEXT
        other_markings_made_along_with_procedural_marking? TEXT
        all_customer_suggested_fields_edited? TEXT
        place_id INTEGER
        popularity INTEGER
                      )
    """)

    for _, row in transformed_data.iterrows(): # iterate through transformed_data
        cursor.excute("""
            INSERT INTO report_data (
                      place_id, 
                      edited_fields, 
                      last_editor_resolution, 
                      suggested_fields, 
                      ticket_type, 
                      other_markings_made_along_with_procedural_marking, 
                      all_customer_suggested_fields_edited, 
                      place_id, 
                      popularity
                      ) 
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        row["place_id"], 
        row["edited_fields"], 
        row["last_editor_resolution"], 
        row["suggested_fields"], 
        row["ticket_type"], 
        row["other_markings_made_along_with_procedural_marking"], 
        row["all_customer_suggested_fields_edited"], 
        row["place_id"], 
        row["popularity"]))

    conn.commit() # ensures all changes are saved
    conn.close() # close connection

load_data(transformed_data, "data/report_data.db") # call load_data with parameters of transformed_data and the database path

# Pipeline Orchestration- Managing and coordinating steps in ETL pipeline to ensure correct run

def run_etl_pipeline():

    data = d

    transformed_data = transform_data(data)

    load_data(transformed_data, "data/report_data.db")

run_etl_pipeline() # call function to execute ETL pipeline

# Scheduling *** look into cron jobs ***

import time

def run_etl_pipeline():
    pass

while True:
    run_etl_pipeline() # run function
    time.sleep(86400) # wait x amount of time and rerun function

# Automation- handle errors, retry operations, notify on errors ***  ***

def run_etl_pipeline():
    try:
        data = d

        transformed_data = transform_data(data)

        load_data(transformed_data, "data/report_data.db")
    except Exception as e:
        print(f"Error ocurred: {e}")

run_etl_pipeline()