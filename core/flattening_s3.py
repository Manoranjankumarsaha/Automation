from io import BytesIO
import os
import json
import pandas as pd
import itertools
import boto3
import openpyxl
import math
from common import s3_Client, initFlattening


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


def transpose_df(param_df):
    transposed_df = {}
    for filename, df in param_df.items():
        transposed_df[filename] = df.transpose()
    return transposed_df


def questions_as_column(dataframe):
    questions_column = {}
    for filename, df in dataframe.items():
        df.columns = df.iloc[1]
        df = df.drop(df.index[1])
        questions_column[filename] = df
    return questions_column


def add_filename(dataframes):
    filename_column_df = {}
    for filename, df in dataframes.items():
        df.insert(0, 'File Name', filename)
        filename_column_df[filename] = df

    return filename_column_df


def drop_all_row_except_ref_ans(dataframe):
    dropped_row_df = {}
    for filename, df in dataframe.items():
        index_to_keep = 2
        df = df.iloc[[index_to_keep]]
        dropped_row_df[filename] = df
    return dropped_row_df


def remove_questionaire_col(dataframe):
    dropped_col_df = {}
    for filename, df in dataframe.items():
        df = df.drop(columns='Questionnaire')
        dropped_col_df[filename] = df
    return dropped_col_df


def normalize_value(value):
    # If the value is a list, return the list directly
    if isinstance(value, list):
        # If there's only one value in the list, remove square brackets and single quotes
        if len(value) == 1:
            return value[0].strip("[]'")
        return value
    # Convert the value to a string and then split it
    values = str(value).split(', ')
    # Remove square brackets and single quotes from each value
    normalized_values = [v.strip("[]'") for v in values]
    return normalized_values


def normalize_dataframe(df):
    # Create a list to store all possible combinations
    all_combinations = []

    for _, row in df.iterrows():
        # Normalize each cell in the row and create all possible combinations
        combinations = list(itertools.product(
            *[normalize_value(cell) for cell in row]
        ))
        all_combinations.extend(combinations)

    # Create a new DataFrame with all possible combinations
    df_normalized = pd.DataFrame(all_combinations, columns=df.columns)

    # Remove square brackets and single quotes from all values in the DataFrame
    df_normalized = df_normalized.applymap(lambda x: x.replace("['", "").replace("']", "").replace(
        "{", "").replace("}", "").replace("[", "").replace("]", "") if isinstance(x, str) else x)

    return df_normalized


def explode_df(dataframe):
    exploded_dataframes = {}

    for filename, df in dataframe.items():
        # Normalize the DataFrame
        df_normalized = normalize_dataframe(df)

        # Merge the normalized DataFrame with the previous ones (if any)
        if filename in exploded_dataframes:
            exploded_dataframes[filename] = pd.concat(
                [exploded_dataframes[filename], df_normalized], ignore_index=True)
        else:
            exploded_dataframes[filename] = df_normalized

        print(
            f"DataFrame for {filename} is successfully normalized and stored. New shape: {exploded_dataframes[filename].shape}")
    return exploded_dataframes


def save_to_s3_excel(exp_dataframe, bucket_name, output_folder_path):
    s3_client = boto3.client('s3')
    for filename, df in exp_dataframe.items():
        filename_without_extension = filename.split(".")[0]
#         print(filename_without_extension)
        # Convert the DataFrame to Excel content
        excel_content = BytesIO()
        with pd.ExcelWriter(excel_content, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        excel_content.seek(0)
        # Upload the Excel content to S3
        s3_key = f"{output_folder_path}/{filename_without_extension}_normalized.xlsx"
        s3_client.upload_fileobj(excel_content, bucket_name, s3_key)
        print(f"{filename_without_extension}_normalized.xlsx uploaded to {s3_key}")


rtn = initFlattening()
dataframes = read_xlsx_files_from_s3_folder(
    rtn['bucket_name'], rtn['input_path'])
transpose_df_all = transpose_df(dataframes)
df_q_col = questions_as_column(transpose_df_all)
df_with_filename_columns = add_filename(df_q_col)
drp_row_df_except_refined = drop_all_row_except_ref_ans(
    df_with_filename_columns)
drp_col_que_df = remove_questionaire_col(drp_row_df_except_refined)
exploded_dataframes = explode_df(drp_col_que_df)
save_to_s3_excel(exploded_dataframes, rtn['bucket_name'], rtn['output_path'])
