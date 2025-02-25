import os
import configparser

def get_absolute_path(file_name):
    
    absolute_path = os.path.dirname(__file__)
    relative_path = file_name
    full_path = os.path.join(absolute_path, relative_path).replace('\\','/')
    
    return full_path

def create_config():
    config = configparser.ConfigParser()

    # Add sections and key-value pairs
    config['General'] = {'my_sql_password': 'Synergy21SB.'}

    # Write the configuration to a file
    with open(get_absolute_path('config.ini'), 'w') as configfile:
        config.write(configfile)

def read_config():
    # Create a ConfigParser object
    config = configparser.ConfigParser()

    # Read the configuration file
    config.read(get_absolute_path('config.ini'))

    # Access values from the configuration file
    my_sql_password = config.get('General', 'my_sql_password')

    # Return a dictionary with the retrieved values
    config_values = {
        'my_sql_password': my_sql_password
    }

    return config_values

if __name__ == "__main__":
    create_config()