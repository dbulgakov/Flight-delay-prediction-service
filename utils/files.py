import os
import glob


def save_text_file(file_data, file_name, location=''):
    with open('{}/{}'.format(location, file_name), 'w') as f:
        f.write(file_data)


def ensure_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def get_file_list(folder_name='', prefix=''):
    if len(folder_name) > 0 and not folder_name.endswith('/'):
        folder_name = '{}/'.format(folder_name)

    return glob.glob(folder_name + '*' + prefix)


def get_all_files_from_subfolders(folder_name):
    subfolder_list = get_file_list(folder_name)
    file_list = []

    for subfolder in subfolder_list:
        file_list.extend(get_file_list(subfolder))

    return file_list

