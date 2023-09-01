from io import BytesIO
import openpyxl
import pandas as pd
import pandas as pd
import os
import boto3

from common import s3_Client, initMapping


def read_xlsx_files_from_s3_folder(bucket_name, folder_path):
    s3_client = s3_Client()
    response = s3_client.list_objects_v2(
        Bucket=bucket_name, Prefix=folder_path
    )
    dataframes_dict = {}
    for obj in response.get('Contents', []):
        file_key = obj['Key']
        file_name = file_key.split('/')[-1]
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
            content = response['Body'].read()
            if content:
                wb = openpyxl.load_workbook(BytesIO(content), read_only=True)
                sheet = wb.active
                df = pd.DataFrame(sheet.values)
                dataframes_dict[file_name] = df
        except Exception as e:
            print(f"Error processing {file_name}: {e}")
    return dataframes_dict


def Column_name(dataframe):
    Column_name = {}
    for filename, df in dataframe.items():
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])
        Column_name[filename] = df
    return Column_name


def read_excel_from_s3_to_df(bucket_name, file_key):
    s3_client = s3_Client()
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        content = response['Body'].read()
        if content:
            wb = openpyxl.load_workbook(BytesIO(content), read_only=True)
            sheet = wb.active
            data = sheet.values
            columns = next(data)
            df = pd.DataFrame(data, columns=columns)
            return df
    except Exception as e:
        print(f"Error reading {file_key}: {e}")
        return None


def map_and_save_to_s3(dataframe, features_list, bucket_name, folder_path):
    s3_client = s3_Client()
    mapped_dfs = {}
    for filename, df in dataframe.items():
        df = df.applymap(str)
        no_columns = len(df.columns)
        for x in range(0, no_columns):
            right_join = pd.merge(df,
                                  features_list[[
                                      'L3-M&M-Revised', 'Sub -attributes (L2)']],
                                  left_on=df.columns[x],
                                  right_on='L3-M&M-Revised',
                                  how='left')
            right_join.rename(columns={
                'Sub -attributes (L2)': f'{df.columns[x]}_L2'
            }, inplace=True)
            right_join.drop(
                'L3-M&M-Revised', axis=1, inplace=True)
            df = right_join
            right_join.dropna(axis=1, how='all', inplace=True)
            mapped_dfs[filename] = right_join
    for filename, df in mapped_dfs.items():
        filename = filename.split(".")[0]
        excel_content = BytesIO()
        with pd.ExcelWriter(excel_content, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        excel_content.seek(0)
        s3_key = f"{folder_path}/{filename}_mapped.xlsx"
        s3_client.upload_fileobj(excel_content, bucket_name, s3_key)
        print(f"{filename}_mapped.xlsx uploaded to {s3_key}")
    return mapped_dfs


rtn = initMapping()
dataframes = read_xlsx_files_from_s3_folder(
    rtn['bucket_name'], rtn['input_path'])
df_new_col = Column_name(dataframes)
feature_list_df = read_excel_from_s3_to_df(
    rtn['bucket_name'], rtn['feature_list'])
result_dataframes = map_and_save_to_s3(
    dataframes, feature_list_df, rtn['bucket_name'], rtn['output_path'])
