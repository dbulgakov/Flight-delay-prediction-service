import os
import glob
import pickle


def save_text_file(file_data, file_name, location=''):
    with open('{}/{}'.format(location, file_name), 'w') as f:
        f.write(file_data)


def save_binary_file(file_data, file_name, location=''):
    n_bytes = 2**31
    max_bytes = 2**31 - 1
    bytes_out = pickle.dumps(file_data)
    with open('{}/{}'.format(location, file_name), 'w+b') as f_out:
        for idx in range(0, n_bytes, max_bytes):
            f_out.write(bytes_out[idx:idx+max_bytes])


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

