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
    config['General'] = {'my_sql_password': 'synergyHB!1'}

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

def start_mysql_service():

    import win32serviceutil
    import win32service

    service_name = 'MySQL80'

    try:
        # Check the current status of the service
        status = win32serviceutil.QueryServiceStatus(service_name)[1]

        if status == win32service.SERVICE_RUNNING:
            print(f"{service_name} is already running.")
        else:
            # If not running, start the service
            win32serviceutil.StartService(service_name)
            print(f"{service_name} started successfully.")

    except Exception as e:
        print(f"Failed to check or start {service_name}: {e}")