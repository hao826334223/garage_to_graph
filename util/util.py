import os
from datetime import datetime


def change_datetime_format(date_time_str):
    datetime_obj = datetime.fromisoformat(date_time_str)
    return datetime_obj.strftime("%Y%m%d-%H:%M:%S")


def store_scraped_data(df, ticker, start_time, end_time, interval):
    outdir = os.path.join(os.getcwd(), 'outputs', ticker, interval)
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    start_time = change_datetime_format(start_time)
    end_time = change_datetime_format(end_time)
    file_name = f"{outdir}/{ticker}_{start_time}_{end_time}.parquet"
    df.to_parquet(file_name)
    print("File stored: ", file_name)


def get_files_path():
    outdir = os.path.join(os.getcwd(), 'outputs')
    if not os.path.exists(outdir):
        print("outputs folder not exist")
        raise FileExistsError('outputs folder not exist"')

    path_list = []
    for root, dirs, files in os.walk(outdir):
        for f in files:
            path_list.append(os.path.join(root, f))
    return path_list
