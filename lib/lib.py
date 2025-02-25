import os

def get_absolute_path(file_name):
    
    absolute_path = os.path.dirname(__file__)
    relative_path = file_name
    full_path = os.path.join(absolute_path, relative_path).replace('\\','/')
    
    return full_path