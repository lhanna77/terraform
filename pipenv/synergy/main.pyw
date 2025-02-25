
import shutil
import tkinter as tk
from tkinter import ttk
import mysql.connector
from pandas import read_csv
from lib_synergy import create_config,read_config,get_absolute_path,start_mysql_service

def get_valid_string(prompt):
    while True:
        value = input(prompt).strip()
        if value:
            return value
        else:
            print("Invalid input. Please enter a non-empty string. :)")

def mysql_insert_record(first_name,last_name,training):

    mydb = mysql.connector.connect(
        host="localhost",
        #port="3307",
        user="root",
        password=read_config()['my_sql_password'],
        database="synergy"
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

# Function to display the selected name and training option, then exit
def submit_and_exit():
    
    try:
    
        first_name = first_name_entry.get()
        last_name = last_name_entry.get()
        selected_training = combo_box.get()
        
        if first_name and last_name and selected_training:
            # Print the values to the console
            print(f"First Name: {first_name}")
            print(f"Last Name: {last_name}")
            print(f"Training Option: {selected_training}")
            
            mysql_insert_record(first_name,last_name,selected_training)
            
            # Exit the program after submitting
            root.quit() # This will exit the tkinter main loop
            root.destroy() # This will close the window
            
        else:
            result_label.config(text="Please enter both names and select a training option.")

    except Exception as e:
        print(f"An error occurred : {e}")
        root.quit() # This will exit the tkinter main loop
        root.destroy() # This will close the window

#Start

#Attempt to start mysql windows service
start_mysql_service()

# Creating the main window
root = tk.Tk()
root.title("Synergy Training Selection")

# First name label and entry
tk.Label(root, text="First Name").grid(row=0, column=0, padx=10, pady=5)
first_name_entry = tk.Entry(root)
first_name_entry.grid(row=0, column=1, padx=10, pady=5)

# Last name label and entry
tk.Label(root, text="Last Name").grid(row=1, column=0, padx=10, pady=5)
last_name_entry = tk.Entry(root)
last_name_entry.grid(row=1, column=1, padx=10, pady=5)

# Button to confirm and submit the selection
submit_button = tk.Button(root, text="Submit", command=submit_and_exit)
submit_button.grid(row=2, column=0, columnspan=2, pady=10)

# ComboBox for training options
tk.Label(root, text="Synergy Training Options").grid(row=3, column=0, padx=10, pady=5)
list_of_training_data = get_training_csv()
combo_box = ttk.Combobox(root, values=list_of_training_data)
combo_box.grid(row=3, column=1, padx=10, pady=5)

# Label to display validation error (if any)
result_label = tk.Label(root, text="", fg="blue")
result_label.grid(row=4, column=0, columnspan=2, padx=10, pady=10)

# Run the Tkinter loop
root.mainloop()

file_copy_rename()