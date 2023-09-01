import json
import boto3


from common import s3FileListing, initCombineTranscript, s3_Client, move_s3_object

# Replace these with your own AWS credentials and bucket information

# aws_access_key_id = 'AKIATWZQTUP5P4QMO43H'

# aws_secret_access_key = 'T7rV2eXE+YaEQv6Gef8Qy+MqP39FhhDXTHJOWI9b'

bucket_name = 'ixolerator-cloud'

# Path to the folder in the S3 bucket

# folder_prefix = 'MeghnadAutomation/Supro_mini/Owner/Lot_1/TestingUpdated/Batch7/Transcript/txt/'
# destination = 'MeghnadAutomation/Supro_mini/Owner/Lot_1/TestingUpdated/Batch7/Transcript/splitOriginalTranscript/'
# file_ext = 'txt'

# Create a new S3 client
s3 = s3_Client()


def get_transcript_data_from_s3(filename):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=filename)
        transcript_data = json.loads(response['Body'].read().decode('utf-8'))
        return transcript_data
    except Exception as e:
        print("An error occurred while fetching transcript data:", e)
        return []


def get_common_prefix(paths):
    path_components = [path.split('/') for path in paths]
    min_length = min(len(components) for components in path_components)
    common_components = []
    for i in range(min_length):
        if all(components[i] == path_components[0][i] for components in path_components):
            common_components.append(path_components[0][i])
        else:
            break
    common_prefix = '/'.join(common_components)
    return common_prefix


def combine_transcripts():
    try:
        allfilepath = s3FileListing(bucket_name, folder_prefix, file_ext)
        list_of_unique_codes = []
        code_to_filenames = {}
        text_contents = []
        for path in allfilepath:
            prefix = get_common_prefix(allfilepath)
            # print(prefix)
            ext = (path.split("/")[-1]).split(".")[-1].lower()
            if ext == file_ext:
                filename = path.split("/")[-1]
                if filename:
                    splitted_filename = filename.split(".")[0]
                    codes = splitted_filename.split("_")[2]
                    if codes not in list_of_unique_codes:
                        list_of_unique_codes.append(codes)
                    if codes not in code_to_filenames:
                        code_to_filenames[codes] = [filename]
                    else:
                        code_to_filenames[codes].append(filename)

        for code, filenames in code_to_filenames.items():
            # print(f"{code}:{filenames}")
            combined_transcripts = []
            for filename in filenames:
                filename_with_path = f"{prefix}/{filename}"
                transcript_data = get_transcript_data_from_s3(
                    filename_with_path)
                # print(transcript_data)
                combined_transcripts.extend(transcript_data)
                # print(combined_transcripts)
                destination_key = f"{destination}{filename}"
                move_s3_object(bucket_name, filename_with_path,
                               destination_key)
            combined_filename = f"{prefix}/Supro_Owner_{code}.txt"
            s3.put_object(
                Bucket=bucket_name,
                Key=combined_filename,
                Body=json.dumps(combined_transcripts)
            )
    except Exception as e:
        print("An error occurred:", e)


folder_prefix, destination, file_ext = initCombineTranscript()
combine_transcripts()
