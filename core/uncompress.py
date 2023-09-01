import os
import os.path
from collections import deque
import glob
import wave
import boto3
from configparser import ConfigParser


def unCompressInit():
    config = ConfigParser(interpolation=None)
    config.read(os.getcwd()+'/config.ini')
    bucket_name = config['s3details']['bucket_name']
    AUDIO_PATH = config["uncompression_file_locations"]["AUDIO_PATH"]
    INPUT_PATH = config["uncompression_file_locations"]["local_input_dir"]
    OUTPUT_PATH = config["uncompression_file_locations"]["local_output_dir"]
    S3_INPUT_PATH = config["uncompression_file_locations"]["compressed_files_s3_location"]
    S3_OUTPUT_PATH = config["uncompression_file_locations"]["uncompressed_files_s3_location"]

    return bucket_name, INPUT_PATH, AUDIO_PATH, OUTPUT_PATH, S3_INPUT_PATH, S3_OUTPUT_PATH


def create_directory_if_not_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
    else:
        print(f"Directory '{directory_path}' already exists.")


def download_files_from_s3(bucket_name, folder_name, local_directory):
    create_directory_if_not_exists(INPUT_PATH)
    create_directory_if_not_exists(OUTPUT_PATH)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    objects = bucket.objects.filter(Prefix=folder_name)
    for obj in objects:
        file_name = obj.key.split('/')[-1]
        if file_name:
            local_file_path = local_directory + '/' + file_name
            bucket.download_file(obj.key, local_file_path)
            print(f'Downloaded {obj.key} to {local_file_path}')

# download_files_from_s3(bucket_name, folder_name, local_directory)


def delete_local_folders(path):
    os.remove(os.path.join(os.getcwd(), path))


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


bucket_name, INPUT_PATH, AUDIO_PATH, OUTPUT_PATH, S3_INPUT_PATH, S3_OUTPUT_PATH = unCompressInit()
download_files_from_s3(bucket_name, S3_INPUT_PATH, INPUT_PATH)
uncompress(OUTPUT_PATH, INPUT_PATH, AUDIO_PATH)
upload_files_to_s3(OUTPUT_PATH, bucket_name, S3_OUTPUT_PATH)
# delete_local_folders(INPUT_PATH)
# delete_local_folders(OUTPUT_PATH)
