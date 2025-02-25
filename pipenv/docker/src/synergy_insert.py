
import sys
import shutil
import tkinter as tk
from tkinter import ttk
import mysql.connector
from pandas import read_csv
from lib_synergy import read_config,get_absolute_path,start_mysql_service

def get_valid_string(prompt):
    while True:
        value = input(prompt).strip()
        if value:
            return value
        else:
            print("Invalid input. Please enter a non-empty string. :)")

def mysql_insert_record(first_name,last_name,training):

    mydb = mysql.connector.connect(
        host="172.20.0.3",
        #port=3307,
        user="root",
        password=read_config()['my_sql_password'],
        database="synergy",
    )

    mycursor = mydb.cursor()

    try:

        sql = "INSERT INTO synergy.training (FirstName,LastName,TrainingName) VALUES (%s, %s, %s)"
        val = (first_name,last_name,training)
        mycursor.execute(sql, val)

        mydb.commit()

        print(mycursor.rowcount, f"{last_name} record inserted - {training} training.")
    
    except mysql.connector.Error as e:
        print(f"Error while connecting to MySQL: {e}")

    finally:
        if mydb.is_connected():
            mycursor.close()
            mydb.close()
    
def get_training_csv():

    try:
        full_path = get_absolute_path('training.csv')
        training_data = read_csv(full_path)
        # convert string to list 
        list_of_training_data = training_data['training'].tolist()

        return list_of_training_data
    
    except FileNotFoundError as e:
        print(f"File not found: {e}")
        return []
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")
        return []

def file_copy_rename():
    
    try:

        full_path = get_absolute_path('main.py')
        new_file_name = get_absolute_path('main.pyw')
        
        shutil.copy(full_path,new_file_name)
        
        print('Exe file updated to most recent!')
        
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except Exception as e:
        print(f"An error occurred while copying the file: {e}")

mysql_insert_record('Brooke','Hannah','Docker Expert')