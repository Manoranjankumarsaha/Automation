import glob
import json
import os
import time
import requests
import boto3
from botocore.exceptions import ClientError
import logging
import shutil
# from configparser import ConfigParser


# config = ConfigParser(interpolation=None)
# config.read('../../config.ini')
# bucket_name = config['s3details']['bucket_name']
# # Parameters for transcribe
# url = config['transcribe']['transcribe_url']
# headers = {'Content-Type': 'application/json'}
# # This is the folder in s3 where we have stored our audio file to that needs to be transcribed
# folder_prefix = config['transcribe']['uncompressed_files_to_transcribe_s3']
# # Parameter values to upload transcripted file to s3
# local_folder = config['transcribe']['local_folder_to_save_output_files']
# s3_folder = config['transcribe']['s3_folder_to_save_output_files']


def get_s3_object_paths(bucket_name, folder_prefix):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)
    object_paths = []
    if 'Contents' in response:
        for item in response['Contents']:
            object_paths.append(item['Key'])
    while response['IsTruncated']:
        response = s3.list_objects_v2(
            Bucket=bucket_name, Prefix=folder_prefix, ContinuationToken=response['NextContinuationToken'])
        for item in response['Contents']:
            object_paths.append(item['Key'])
    return object_paths


# To get exact path of the files in s3 to pass in transcribe parameter ["data"]
def get_paths(bucket_name, folder_prefix):
    try:
        paths = get_s3_object_paths(bucket_name, folder_prefix)
        paths = [path for path in paths if path is not None]
        return paths
    except ClientError as e:
        logging.error(e)
        return False


# Filters out wave files only to trascribe and returns path of those wave files
def get_wave_files(bucket_name, folder_prefix):
    result_paths = get_paths(bucket_name, folder_prefix)
    wave_paths = [path for path in result_paths if ".wav" in path]
    return wave_paths


def create_directory_if_not_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
    else:
        print(f"Directory '{directory_path}' already exists.")
# create_directory_if_not_exists(directory_path)


def call_transcript_api_with_retries(url, params, headers, max_retries=3):
    for i in range(max_retries):
        response = requests.post(url=url, data=params, headers=headers)
        if response.status_code == 200 and 'status_code' in response.json() and response.json()['status_code'] == 0:
            return response.json()
        else:
            print(
                f"Transcribe API call failed with status code {response.status_code} & message {response.text}. Retrying... (Attempt {i + 1})")
            time.sleep(2)
    print(f"Transcribe API call failed after {max_retries} retries.")
    return None


def transcribe(local_folder, url, headers, bucket_name, folder_prefix, unprocessed_file_folder, processed_file_folder):
    create_directory_if_not_exists(local_folder)
    create_directory_if_not_exists(processed_file_folder)
    try:
        param = {
            "data": "",
            "number_of_speakers": 2,
            "lang": "en",
            "enable_pre_process": 0,
            "keep_original_lang": 0
        }
        result_path = get_wave_files(bucket_name, folder_prefix)
        for path in result_path:
            # Replaces data in param with each wave file path
            param["data"] = f"/home/ubuntu/s3_bucket/{path}"
            filename = param["data"].split("/")[-1]
            print(filename)
            text_filename = filename.replace(".wav", ".txt")
            json_filename = filename.replace(".wav", ".json")
            text_file_path = f'{local_folder}/{text_filename}'
            json_file_path = f'{local_folder}/{json_filename}'
            json_param = json.dumps(param)
            print(json_param)
            response_transcribe = call_transcript_api_with_retries(
                url, json_param, headers)
            if response_transcribe is None:
                print(
                    f"No valid response for file: {filename}. Skipping to the next file.")
                continue

            transcribe_result = response_transcribe
            status_code = transcribe_result.get('status_code', -1)
            status_message = transcribe_result.get('status_message', -1)

            if status_code == 0 and 'transcribed_text' in transcribe_result:
                transcribed_text = transcribe_result['transcribed_text']
                with open(text_file_path, 'w') as file:
                    file.write(str(transcribed_text))
                print("Data has been written to the text file:", text_file_path)
                with open(json_file_path, "w") as json_file:
                    json.dump(transcribe_result, json_file)
                print("Data has been written to the JSON file:", json_file_path)
                shutil.move(os.path.join(unprocessed_file_folder, filename), os.path.join(
                    processed_file_folder, filename))
            else:
                print(
                    f"Some error occurred and status code is {status_code}, {status_message}")

    except ClientError as e:
        logging.error(e)
        return False

# Function to upload files to s3


def upload_files_to_s3(local_folder_path, bucket_name, s3_folder_path=''):
    s3 = boto3.client('s3')

    for root, dirs, files in os.walk(local_folder_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            s3_key = os.path.join(s3_folder_path, file)

            s3.upload_file(local_file_path, bucket_name, s3_key)

            print(
                f"Uploaded {local_file_path} to S3 bucket {bucket_name} with key: {s3_key}")


# transcribe()
# upload_files_to_s3(local_folder, bucket_name, s3_folder)
