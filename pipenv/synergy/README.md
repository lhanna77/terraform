# Synergy Training Selection

This is a Python-based application that allows users to select training options and insert the selected information into a MySQL database. 
It uses a simple Tkinter GUI interface.

## Features
- **Graphical User Interface (GUI)** using Tkinter for ease of use.
- **MySQL Database Integration** for storing training information.
- **CSV Reading** for loading available training options.
- **Error Handling** for database connections, file operations, and input validation.
- **File Management** for copying and renaming files automatically.

## Requirements

To run this project, you will need the following dependencies:

- Python 3.x
- `mysql-connector-python`
- `pandas`
- `tkinter` (bundled with Python standard library)
- A running MySQL server

Install the required Python libraries by running:

```bash
pip install mysql-connector-python pandas

MySQL Setup

1. Create a MySQL database named synergy with a table called training. The table schema should look like this:

CREATE DATABASE synergy;
USE synergy;

CREATE TABLE training (
    id INT AUTO_INCREMENT PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    TrainingName VARCHAR(100)
);

2. Add your MySQL password to a configuration file that is read by the read_config() function. Ensure you have the file create_read_config_mysql.py to handle configurations. An example configuration file could look like this:

def read_config():
    return {
        "my_sql_password": "your_mysql_password"
    }

def get_absolute_path(file_name):
    import os
    return os.path.abspath(file_name)

Running the Application

1. Clone the Repository:

git clone https://github.com/lhanna77/synergy

2. Ensure you have the necessary files:

main.py (the main script)

training.csv (a CSV file with a list of training options)

create_read_config_mysql.py (configuration script)

3. Run the Application:

python main.py

Input Fields:

First Name: Enter your first name.

Last Name: Enter your last name.

Training Options: Select a training option from the dropdown.

Click "Submit" to save the information to the database. Once submitted, the program will close automatically.

Error Handling

Database Errors: Any issues related to MySQL connection or query execution are caught, and an appropriate error message is displayed.

File Not Found: If the training.csv file or other required files are missing, the program will display an error and terminate.

Input Validation: The application checks for empty input fields and invalid selections before proceeding.

File Management

The script automatically copies main.py to a new file named main.pyw using the file_copy_rename function. This ensures the latest version of the script is renamed for different execution contexts.

License

This project is licensed under the MIT License. See the LICENSE file for details.

### Explanation:
- **Installation**: Lists dependencies and how to install them.
- **MySQL Setup**: Provides instructions on setting up the MySQL database.
- **Running the Application**: Gives step-by-step instructions for running the app.
- **Error Handling**: Explains the types of errors that are handled.
- **File Management**: Describes the file copy and renaming functionality.
- **License**: Placeholder for any potential license information.