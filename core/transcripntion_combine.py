import json
import boto3


from common import s3FileListing, s3_Client, move_s3_object
bucket_name = 'ixolerator-cloud'
s3 = s3_Client()
input_path = "BoleroFuelTankProject/Bolero Fuel tank study Owner/Batch3/Transcript/txt/"
output_path = "BoleroFuelTankProject/Bolero Fuel tank study Owner/Batch3/Transcript_combined_test/"
file_ext = 'txt'

def get_transcript_data_from_s3(filename):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=filename)
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
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
        allfilepath = s3FileListing(bucket_name, input_path, file_ext)
        list_of_unique_codes = []
        code_to_filenames = {}
        for path in allfilepath:
            prefix = get_common_prefix(allfilepath)
            ext = (path.split("/")[-1]).split(".")[-1].lower()
            if ext == file_ext:
                filename = path.split("/")[-1]
                if filename:
                    splitted_filename = filename.split(f".{ext}")[0]
                    nm = splitted_filename.split("_")
                    codes='_ '.join(nm[:-1])
                    if codes not in list_of_unique_codes:
                        list_of_unique_codes.append(codes)
                    if codes not in code_to_filenames:
                        code_to_filenames[codes] = [filename]
                    else:
                        code_to_filenames[codes].append(filename)
        
        if len(list_of_unique_codes)>0:
            for code, filenames in code_to_filenames.items():
                print(code)
                print(filenames)
                combined_transcripts = []
                for filename in filenames:
                    filename_with_path = f"{prefix}/{filename}"
                    transcript_data = get_transcript_data_from_s3(filename_with_path)
                    combined_transcripts.extend(transcript_data)
                    destination_key = f"{output_path}{filename}"
                    print(destination_key)
                    # move_s3_object(bucket_name, filename_with_path,destination_key)
                
                combined_filename = f"{prefix}/{code}.txt"
                print(combined_filename)
                # s3.put_object(
                #     Bucket=bucket_name,
                #     Key=combined_filename,
                #     Body=json.dumps(combined_transcripts)
                # )
                print(combined_transcripts)
                break
    except Exception as e:
        print("An error occurred:", e)

combine_transcripts()