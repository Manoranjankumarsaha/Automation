import os
import pandas as pd
from configparser import ConfigParser


# config = ConfigParser(interpolation=None)
# config.read('../../config.ini')
# input_folder = config['jsontoxlsx']['input_folder']
# output_folder = config['jsontoxlsx']['output_folder']


def convert_json_to_excel(input_folder, output_folder):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    json_files = [file for file in os.listdir(
        input_folder) if file.endswith('.json')]
    for json_file in json_files:
        json_file_path = os.path.join(input_folder, json_file)
        with open(json_file_path, 'r') as file:
            json_data = pd.read_json(file)
        excel_file_name = json_file.replace('.json', '.xlsx')
        excel_file_path = os.path.join(output_folder, excel_file_name)
        json_data.to_excel(excel_file_path, index=False)


# convert_json_to_excel(input_folder, output_folder)
