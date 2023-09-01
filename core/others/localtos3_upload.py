import boto3
import os

bucket_name="ixolerator-cloud"
s3_client =  boto3.client('s3', aws_access_key_id="AKIATWZQTUP5P4QMO43H", aws_secret_access_key="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")
directory_path = r'D:\s3'

def df_to_excel_s3_upload(local_file_path,bucket_name,s3_object_key):
    s3_client.upload_file(local_file_path, bucket_name, s3_object_key)


# Get a list of file paths in the directory
file_paths = [os.path.join(directory_path, filename) for filename in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, filename))]

# Print the list of file paths
for local_file_path in file_paths:
    print(local_file_path)
    filename = local_file_path.split("\\")[-1]
    s3_object_key = f'MeghnadAutomation/Supro_mini/Owner/Lot_2/getIdeas/{filename}'
    print(s3_object_key)
    # df_to_excel_s3_upload(local_file_path,bucket_name,s3_object_key)
    break
