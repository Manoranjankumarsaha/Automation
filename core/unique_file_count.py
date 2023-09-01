import pandas as pd

from common import s3FileListing, initUniqueFileCount


def unique_files_count(bucket_name, folder_prefix):
    files_Lot = s3FileListing(bucket_name, folder_prefix, "wav")
    print(f"Total Files:{len(files_Lot)}")
    logs=[]
    k = 0
    unique_count=0
    rec_log={ "file":'',"count":0,"status":"","unique_count":0}
    for path in files_Lot:
        k= k+1
        filename=path.split("/")[-1]
        ext=filename.split(".")[-1].lower()
        if ext=="wav":
            if "_call" in filename:
                logs.append({ "file":filename,"count":k,"status":"_call","unique_count":unique_count})
            else:
                unique_count= unique_count+1
                logs.append({ "file":filename,"count":k,"status":"unique","unique_count":""})
    if len(files_Lot)==0:
            logs.append(rec_log)

    print(f"\n**********************************Unique count : {folder_prefix}*********************************************************") 
    df = pd.DataFrame(logs)
    print(df)
    print("\n*****************************************************************************************************************************************************") 

    # file_path = r'D:\s3\Unique_count.csv'
    # df.to_csv(file_path, index=False)

bucket_name, folder_prefix = initUniqueFileCount()
unique_files_count(bucket_name, folder_prefix)
