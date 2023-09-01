import pandas as pd
import pandas as pd
import os

# input_folder = '../../files/Owner Files/Flattened Files'
# output_folder = '../../files/Owner Files/Mapped Files'
# feature_list = '../../files/M&M_Feature_list.xlsx'


def create_directory_if_not_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
    else:
        print(f"Directory '{directory_path}' already exists.")


def store_features_list_as_dataframe(features_list):
    feature_list = pd.read_excel(features_list)
    featurelist_df = pd.DataFrame(feature_list)
    return (featurelist_df)


def read_xlsx_files_convert_to_df(input_folder):
    xlsx_files = [file for file in os.listdir(
        input_folder) if file.endswith('.xlsx')]
    dataframes = {}
    for xlsx_file in xlsx_files:
        xlsx_file_path = os.path.join(input_folder, xlsx_file)
        # print(xlsx_file_path)
        try:
            dataframe = pd.read_excel(xlsx_file_path, engine='openpyxl')
        except Exception as e:
            print(f"Error reading '{xlsx_file}': {e}")
            continue  # Skip this file and move to the next one
        file_name_without_extension = os.path.splitext(xlsx_file)[0]
        dataframes[file_name_without_extension] = dataframe
    return dataframes


def delete_file(file_path):
    try:
        os.remove(file_path)
        print(f"File '{file_path}' has been deleted.")
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except PermissionError:
        print(f"Permission denied. Unable to delete '{file_path}'.")
    except Exception as e:
        print(f"An error occurred while deleting '{file_path}': {e}")

# Example usage:
# file_path = '../../files/Owner Files/Flattened Files\\~$Owner_XUV300_C220554233 (3)_normalized.xlsx'
# delete_file(file_path)


def map_df(dataframe, features_list, output_directory):
    right_join_df = {}
    for filename, df in dataframe.items():
        df = df.applymap(str)
        no_columns = len(df.columns)
        for x in range(0, no_columns):
            right_join = pd.merge(df,
                                  features_list[[
                                      'Feature Support (Not to be shared with client)', 'Feature - L2']],
                                  left_on=df.columns[x],
                                  right_on='Feature Support (Not to be shared with client)',
                                  how='left')

            # Update column names
            right_join.rename(columns={
                'Feature - L2': f'{df.columns[x]}_L2'
            }, inplace=True)

            right_join.drop(
                'Feature Support (Not to be shared with client)', axis=1, inplace=True)
            df = right_join
            # Remove empty columns (columns with all NaN values)
            right_join.dropna(axis=1, how='all', inplace=True)
            right_join_df[filename] = right_join

        # Save DataFrame to Excel with the corresponding filename and directory
        excel_filename = f'{filename}_mapped.xlsx'
        excel_path = os.path.join(output_directory, excel_filename)
        # Modify index=False if you want to include the index
        df.to_excel(excel_path, index=False)

    return right_join_df


# feature_list_df = store_features_list_as_dataframe(feature_list)
# dataframes = read_xlsx_files_convert_to_df(input_folder)
# result_dataframes = map_df(dataframes, feature_list_df, output_folder)
