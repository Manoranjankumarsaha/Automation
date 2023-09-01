import json
from core.IntenderAutomation import filedownloads
from core.IntenderAutomation import uncompress
from core.IntenderAutomation import transcriptModular
from core.IntenderAutomation import insightsModular
from core.IntenderAutomation import jsonToxlsx
from core.IntenderAutomation import mapping
from core.IntenderAutomation import flattening
from configparser import ConfigParser

config = ConfigParser(interpolation=None)
config.read('config.ini')

# Common configs
bucket_name = config['s3details']['bucket_name']
headers = {'Content-Type': 'application/json'}

# File download in local to process
folder_name = config['download_files_from_s3']['s3_folder_name']
local_directory = config['download_files_from_s3']['local_directory']

# Uncompress configs
INPUT_PATH = config["uncompression_file_locations"]["compressed_files_location"]
AUDIO_PATH = config["uncompression_file_locations"]["AUDIO_PATH"]
OUTPUT_PATH = config["uncompression_file_locations"]["uncompressed_files_location"]
s3_folder_name = config["uncompression_file_locations"]['s3_folder_to_store_uncompressed_files']

# Transcript configs
url = config['transcribe']['transcribe_url']
uncompressed_files_folder = config['transcribe']['uncompressed_files_to_transcribe_s3']
local_folder = config['transcribe']['local_folder_to_save_output_files']
s3_folder = config['transcribe']['s3_folder_to_save_output_files']
unprocessed_file_folder = config['transcribe']['local_folder_uncompressed_files']
processed_file_folder = config['transcribe']['folder_to_save_processed_files']


# Insights configs
raw_url = config['raw_ans']['raw_url']
refined_url = config['refined_ans']['refined_url']
question_db = config['question_db']['qdb_intender']
local_folder_raw = config['raw_ans']['folder_to_save_output_files']
s3_folder_raw = config['raw_ans']['s3_folder_raw']
local_folder_refined = config["refined_ans"]['folder_to_save_output_files']
s3_folder_refined = config["refined_ans"]['s3_folder_refined']
transcribe_output_folder_path = config['transcribe']['local_folder_to_save_output_files']

# Json to xlsx configs
input_folder_json = config['jsontoxlsx']['input_folder']
output_folder_json = config['jsontoxlsx']['output_folder']


# Mapping configs
input_folder_map = config['mapping']['input_folder_mapping']
output_folder_map = config['mapping']['output_folder_mapping']
features_list = config['mapping']['features_list']

# Flattening configs
input_folder_flatten = config['flattening']['input_folder_flatten']
output_folder_flatten = config['flattening']['output_folder_flatten']

# GET INPUT FILES TO PROCESS
# filedownloads.download_files_from_s3(bucket_name, folder_name, local_directory)

# UNCOMPRESSION
# uncompress.uncompress(OUTPUT_PATH, INPUT_PATH, AUDIO_PATH)
# uncompress.upload_files_to_s3(OUTPUT_PATH, bucket_name, s3_folder_name)

# TRANSCRIBE
transcriptModular.transcribe(
    local_folder, url, headers, bucket_name, uncompressed_files_folder, unprocessed_file_folder, processed_file_folder)
transcriptModular.upload_files_to_s3(local_folder, bucket_name, s3_folder)

# # INSIGHTS
# insightsModular.change_parameters(question_db, transcribe_output_folder_path,
#                                   raw_url, headers, local_folder_raw, refined_url, local_folder_refined)
# insightsModular.upload_files_to_s3(
#     local_folder_raw, bucket_name, s3_folder_raw)
# insightsModular.upload_files_to_s3(
#     local_folder_refined, bucket_name, s3_folder_refined)

# # JSON TO XLSX CONVERSION
# jsonToxlsx.convert_json_to_excel(input_folder_json, output_folder_json)

# # FLATTENING
# dataframes = flattening.read_xlsx_files_convert_to_df(input_folder_flatten)
# transpose_df_all = flattening.transpose_df(dataframes)
# df_q_col = flattening.questions_as_column(transpose_df_all)
# df_with_filename_columns = flattening.add_filename(df_q_col)
# drp_col_df_except_refined = flattening.drop_all_cols_except_ref_ans(
#     df_with_filename_columns)
# exploded_dataframes = flattening.explode_df(drp_col_df_except_refined)
# flattening.save_to_excel(exploded_dataframes, output_folder_flatten)

# # MAPPING
# feature_list_df = mapping.store_features_list_as_dataframe(features_list)
# dataframes = mapping.read_xlsx_files_convert_to_df(input_folder_map)
# result_dataframes = mapping.map_df(
#     dataframes, feature_list_df, output_folder_map)
