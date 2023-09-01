import os
import os.path
from collections import deque
import glob
import wave
import boto3
from configparser import ConfigParser


# config = ConfigParser(interpolation=None)
# config.read('../../config.ini')
# print(config)

# bucket_name = config['s3details']['bucket_name']
# INPUT_PATH = config["uncompression_file_locations"]["compressed_files_location"]
# AUDIO_PATH = config["uncompression_file_locations"]["AUDIO_PATH"]
# OUTPUT_PATH = config["uncompression_file_locations"]["uncompressed_files_location"]
# s3_folder_name = config["uncompression_file_locations"]['s3_folder_to_store_uncompressed_files']


def uncompress(OUTPUT_PATH, INPUT_PATH, AUDIO_PATH):
    if not os.path.exists(OUTPUT_PATH):
        print("output directory does not exist")
        os.makedirs(OUTPUT_PATH)

    files = glob.glob(os.path.join(INPUT_PATH, AUDIO_PATH))
    print(f"files--{files}")

    with open(os.path.join(OUTPUT_PATH, "lengths.csv"), 'w') as outfile:
        outfile.write("filename;length\n")
        for filename in files:
            input_file = os.path.basename(filename)
            print(f"input_file-->{input_file}")
            output_file = os.path.normpath(
                os.path.join(OUTPUT_PATH, input_file))
            print(f"output_file-->{output_file}")
            cmd = f'ffmpeg -i "{filename}" -ss 00:00:00 "{output_file}"'
            os.system(cmd)
            with wave.open(output_file, 'r') as audio:
                frames = audio.getnframes()
                rate = audio.getframerate()
                length = frames / float(rate)
                outfile.write(f"{input_file};{length:.2f}\n")


def upload_files_to_s3(local_folder, s3_bucket, s3_folder):
    s3 = boto3.client('s3')
    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_path = os.path.join(root, file)
            s3_key = os.path.join(s3_folder, file)
            print(f"Uploading {local_path} to S3://{s3_bucket}/{s3_key}")
            try:
                s3.upload_file(local_path, s3_bucket, s3_key)
                print(f"Uploaded {local_path} to S3://{s3_bucket}/{s3_key}")
            except Exception as e:
                print(f"Failed to upload {local_path} to S3: {e}")


# uncompress()
# upload_files_to_s3(OUTPUT_PATH, bucket_name, s3_folder_name)
