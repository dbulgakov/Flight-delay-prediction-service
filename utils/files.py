import os


def save_text_file(file_data, file_name, location=''):
    with open('{}/{}'.format(location, file_name), 'w') as f:
        f.write(file_data)


def ensure_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

