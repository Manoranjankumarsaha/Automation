import boto3
from botocore.exceptions import ClientError
import logging
import re
from common import s3FileListing, initUniqueFileCount

bucket_name, folder_prefix = initUniqueFileCount()
# bucket_name = 'ixolerator-cloud'
# folder_prefix_lot1 = 'MeghnadAutomation/Supro_mini/Owner/Lot 1/Uncompressed/'
# folder_prefix_lot2 = 'MeghnadAutomation/Supro_mini/Owner/Lot_2/Uncompressed/'
# folder_prefix_lot3 = 'MeghnadAutomation/Supro_mini/Owner/Lot_3/Uncompressed/'
# files_Lot1 = s3FileListing(bucket_name, folder_prefix_lot1, "wav")
# files_Lot2 = s3FileListing(bucket_name, folder_prefix_lot2, "wav")
files_Lot = s3FileListing(bucket_name, folder_prefix, "wav")


def get_all_files_in_all_lots():
    all_files = []
    # for file in files_Lot1:
    #     all_files.append(file)
    # for file in files_Lot2:
    #     all_files.append(file)
    for file in files_Lot:
        all_files.append(file)
    return all_files, len(all_files)


total_files_in_lots, number = get_all_files_in_all_lots()
# print(total_files_in_lots, number)


def classify_and_count_unique_files(file_paths):
    total_count = 0
    unique_files = set()

    for path in file_paths:
        file_name = path.split("/")[-1]
        if "_call" in file_name:
            continue
        total_count += 1
        unique_files.add(file_name)
    return total_count, unique_files


total_count, unique_files = classify_and_count_unique_files(
    total_files_in_lots)

# Display the results
# print("Total count of unique files: ", len(unique_files))
# print(unique_files)


def count_unique_files_splitted(file_path, unique_file_set):
    all_files = []
    unique_files = []
    # def count_unique_files_splitted(file_path):
    for path in file_path:
        file_name_all = path.split("/")[-1]
        file_pref = file_name_all.split(".")[0]
        all_files.append(file_pref)
        # print(file_pref)
    for file in unique_file_set:
        file_name_unique = file.split(".")[0]
        unique_files.append(file_name_unique)
    print(f"all_files:{all_files}")
    print(f"unique_files:{unique_files}")
    print(f"all_files:{len(all_files)}")
    print(f"unique_files:{len(unique_files)}")
    file_count = 0


count_unique_files_splitted(total_files_in_lots, unique_files)
