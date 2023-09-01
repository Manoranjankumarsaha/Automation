from common import s3FileListing,s3_Client

bucket_name="ixolerator-cloud"
source_dir="MeghnadAutomation/Supro_mini/Owner/Latest Revised files/"
dest_dir="MeghnadAutomation/Supro_mini/Owner/Latest Revised files2/"
group_item_size=10

def folderSplit():
    s3_client = s3_Client()
    allfilepath = s3FileListing(bucket_name, source_dir,'wav')
    original_list = list(range(1, len(allfilepath)+1))
    grouped_lists = [original_list[i:i+group_item_size] for i in range(0, len(original_list), group_item_size)]
   
   
    for i, group in enumerate(grouped_lists):
        s_dir=f"{source_dir}batch{i + 1}"
        d_dir=f"{dest_dir}batch{i + 1}"
        print(group)
        for g in group:
            # print(g)
            idx=g-1
            destination_key=f"{s_dir}{allfilepath[idx]}"
            source_key=f"{d_dir}{allfilepath[idx]}"
            print(f"{source_key}:=>{destination_key}")
            # s3_client.copy_object(
            #     Bucket=bucket_name,
            #     CopySource={'Bucket': source_dir, 'Key': source_key},
            #     Key=destination_key
            # )   


# folderSplit()


def getAllFiles():
    s_d="BoleroFuelTankProject/Bolero Fuel tank study Owner/Batch2/Transcript/txt/"
    allfiles = s3FileListing(bucket_name, s_d,'txt')
    allfilepath=[]
    for fp in allfiles:
        allfilepath.append(fp)
    print(allfilepath)


getAllFiles()
