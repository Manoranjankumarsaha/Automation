import os
import json
import pandas as pd
import itertools

input_folder = '../../files/Owner Files/Excel Files'
# output_folder = '../../files/Owner Files/Flattened Files'


def create_directory_if_not_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
    else:
        print(f"Directory '{directory_path}' already exists.")


def read_xlsx_files_convert_to_df(input_folder):
    xlsx_files = [file for file in os.listdir(
        input_folder) if file.endswith('.xlsx')]
    dataframes = {}
    for xlsx_file in xlsx_files:
        xlsx_file_path = os.path.join(input_folder, xlsx_file)
        print(xlsx_file_path)
        try:
            dataframe = pd.read_excel(xlsx_file_path, engine='openpyxl')
        except Exception as e:
            print(f"Error reading '{xlsx_file}': {e}")
            continue  # Skip this file and move to the next one
        file_name_without_extension = os.path.splitext(xlsx_file)[0]
        dataframes[file_name_without_extension] = dataframe
    return dataframes


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


def drop_all_cols_except_ref_ans(dataframe):
    dropped_col_df = {}
    for filename, df in dataframe.items():
        index_to_keep = 2
        df = df.iloc[[index_to_keep]]
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


def save_to_excel(exp_dataframe, folder_path):
    create_directory_if_not_exists(folder_path)
    for filename, df in exp_dataframe.items():
        # Save the DataFrame to an Excel file in the specified folder
        file_path = f"{folder_path}/{filename}_normalized.xlsx"
        df.to_excel(file_path, index=False)


dataframes = read_xlsx_files_convert_to_df(input_folder)
transpose_df_all = transpose_df(dataframes)
df_q_col = questions_as_column(transpose_df_all)
df_with_filename_columns = add_filename(df_q_col)
drp_col_df_except_refined = drop_all_cols_except_ref_ans(
    df_with_filename_columns)
exploded_dataframes = explode_df(drp_col_df_except_refined)
print(exploded_dataframes)
# save_to_excel(exploded_dataframes, output_folder)
