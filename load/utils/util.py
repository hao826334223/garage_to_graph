import os.path
import time
import glob


def get_root_dir():
    """
    Get project root path
    :return: root path, type: str
    """
    current_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.dirname(current_dir)
    return root_dir


def get_local_time():
    """
    Get local time
    :return: %Y%m%d%H%M%S, type: str
    """
    localtime = time.strftime('%Y%m%d%H%M%S', time.localtime(time.time()))
    return localtime


def get_files_path(files_dir, suffix):
    """
    Get files path and files name
    :return: files path, files name, type: list
    """
    files_path = glob.glob(files_dir + os.sep + f'*.{suffix}')
    files_name = [name.split('/')[-1].split('.')[0] for name in files_path]
    return files_path, files_name
