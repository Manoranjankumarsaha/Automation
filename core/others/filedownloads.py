import boto3
# from configparser import ConfigParser


# config = ConfigParser(interpolation=None)
# config.read('../../config.ini')
# bucket_name = config['s3details']['bucket_name']
# folder_name = config['download_files_from_s3']['s3_folder_name']
# local_directory = config['download_files_from_s3']['local_directory']


def download_files_from_s3(bucket_name, folder_name, local_directory):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    # Filter objects in the bucket based on the folder prefix
    objects = bucket.objects.filter(Prefix=folder_name)

    for obj in objects:
        # Extract the file name from the full S3 object key
        file_name = obj.key.split('/')[-1]

        # Skip directories
        if file_name:
            # Generate the local file path
            local_file_path = local_directory + '/' + file_name

            # Download the file
            bucket.download_file(obj.key, local_file_path)
            print(f'Downloaded {obj.key} to {local_file_path}')


# download_files_from_s3(bucket_name, folder_name, local_directory)
