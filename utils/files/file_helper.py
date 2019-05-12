import os
import glob
import pickle


def save_text_file(file_data, file_name, location=''):
    with open('{}/{}'.format(location, file_name), 'w') as f:
        f.write(file_data)


def save_binary_file(file_data, file_name, location=''):
    max_bytes = 2**31 - 1
    bytes_out = pickle.dumps(file_data)
    with open('{}/{}'.format(location, file_name), 'w+b') as f_out:
        for idx in range(0, len(bytes_out), max_bytes):
            f_out.write(bytes_out[idx:idx+max_bytes])


def load_binary_file(location, file_name):
    max_bytes = 2**31 - 1
    bytes_in = bytearray(0)
    full_path = '{}/{}'.format(location, file_name)
    input_size = os.path.getsize(full_path)
    with open(full_path, 'rb') as f_in:
        for _ in range(0, input_size, max_bytes):
            bytes_in += f_in.read(max_bytes)
    return pickle.loads(bytes_in)


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

