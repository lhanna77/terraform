import configparser

def create_config():
    config = configparser.ConfigParser()

    # Add sections and key-value pairs
    config['General'] = {'salesforce_username': 'lee.hannah@monster.co.uk.qa1',
                        'salesforce_password': 'sftest123',
                        'salesforce_security_token': 'xh4KL4lfw2j2ashqTBbomEysp',
                        'salesforce_domain': 'test'}

    # Write the configuration to a file
    with open('config.ini', 'w') as configfile:
        config.write(configfile)

def read_config():
    # Create a ConfigParser object
    config = configparser.ConfigParser()

    # Read the configuration file
    config.read('config.ini')

    # Access values from the configuration file
    salesforce_username = config.get('General', 'salesforce_username')
    salesforce_password = config.get('General', 'salesforce_password')
    salesforce_security_token = config.get('General', 'salesforce_security_token')
    salesforce_domain = config.get('General', 'salesforce_domain')

    # Return a dictionary with the retrieved values
    config_values = {
        'salesforce_username': salesforce_username,
        'salesforce_password': salesforce_password,
        'salesforce_security_token': salesforce_security_token,
        'salesforce_domain': salesforce_domain
    }

    return config_values

if __name__ == "__main__":
    create_config()