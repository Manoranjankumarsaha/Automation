import copy
import glob
import json
import os
import requests
import boto3
from botocore.exceptions import ClientError
import logging
# from configparser import ConfigParser
import time

# config = ConfigParser(interpolation=None)
# config.read('../../config.ini')
# bucket_name = config['s3details']['bucket_name']
# # Parameters for raw url
# raw_url = config['raw_ans']['raw_url']
# refined_url = config['refined_ans']['refined_url']
# headers = {'Content-Type': 'application/json'}
# # Question db
# question_db = config['question_db']['qdb_intender']
# Parameters to pass as data in the API
rawparams = {"data_parameters": {"data_type": "", "data": "", "mode": "conversation", "input_text_column_name": "seq", "input_speaker_column_name": "speaker"}, "pre_processing_parameters": {"do_punctuation_restoration": 0, "non_en_lang": 0}, "company_brand_info_parameters": {"focal_company_brand_info": {"Mahindra": ["XUV 300"]}, "competitor_company_brand_info": {"Mahindra": ["E20", "Verito", "Vibe", "Logan", "Quanto", "NuvoSport", "Bolero", "Bolero Plus", "Bolero Camper", "Bolero Pickup", "Bolero Neo", "Thar", "KUV 100", "KUV 100 NXT", "TUV 300", "TUV 300 Plus", "Scorpio", "XUV 500", "XUV 700", "Xylo", "Marazzo", "Alturas", "Rexton", "Jeeto", "Supro", "Ssangyong", "Quanto"], "Skoda": ["Fabia", "Rapid", "Octavia", "Superb", "Kodiaq", "Kushaq", "Karoq", "Slavia"], "Mercedez": ["A-Class", "C-Class", "C-Class Cabriolet", "E-Class", "S-Class", "CLS", "GLC Coupe", "GLC", "GLA", "GLE", "GLS", "G Class"], "Tata": ["Nano", "Indica V2", "Indica Vista", "Indica Xeta", "Bolt", "Tiago", "Altroz", "Zest", "Indigo CS", "Tigor", "Nexon", "Sumo", "Harrier", "Safari", "Hexa", "Punch"], "BMW": ["2-series", "3-series", "4-series", "5-series", "6-series", "7-series", "8-series", "M2", "M5", "M7", "M8", "X1", "X3", "X4", "X5", "X7"], "Volkswagen": ["Beetle", "Polo", "Polo GTI", "Ameo", "Vento", "Jetta", "Passat", "Taigun", "Tiguan", "Toarage", "Allspace", "T-Roc"], "Toyota": ["Liva Hatchback", "Glanza", "Etios Sedan", "Yaris", "Corolla", "Prius", "Camry", "Innova", "Innova Crysta", "Fortuner", "Landcruiser", "Prado", "Vellfire", "Urbancruiser", "Camper"], "Maruti Suzuki": ["Alto", "Ignis", "Baleno", "Swift", "S-Presso", "A-star", "Ritz", "Swift Dzire", "Swift Dzire Tour", "Celerio", "Celerio X", "Ciaz", "Ertiga", "S-Cross", "Gypsy", "Vitara Brezza", "XL 6", "Wagon R", "Swift VXi", "Zen", "Dzire"], "Chevrolet": ["Aveo U-VA", "Aveo", "Beat", "Spark", "Enjoy", "Sail", "Tavera", "Cruze", "Optra SRV", "Optra", "Daewoo Matiz", "Trailblazer"], "Audi": ["A3", "A3 Cabriolet", "A4", "A6", "A7", "A8L", "R8", "Q2", "Q3", "Q5", "Q7", "Q8"], "Kia": ["Seltos", "Carens", "Sonet", "Carnival", "K7"], "Nissan": ["Micra", "Datsun-go", "Datsun Redi-go", "Sunny", "Teana", "GT-R", "Go+", "Terrano", "Kicks", "Magnite"], "Honda": ["Brio", "Jazz", "Amaze", "City", "Civic", "Accord", "WR-V", "BR-V", "CR-V"], "MG": [
    "Hector", "ZS-EV", "Hector Plus"], "Hyundai": ["Eon", "Santro Xing", "i20", "Elite i20", "i20 Active", "i10", "Aura", "Xcent", "Accent", "Verna", "Elantra", "Creta", "Alcazar", "Venue", "Kona", "Tucson", "Santa Fe"], "Renault": ["Kwid", "Pulse", "Captur", "Duster", "Kiger", "Lodgy"], "Fiat": ["Awventura", "Palio", "UNO", "Punto", "Punto Evo", "Grande Punto", "Abarth", "Abarth Punto", "Urban Cross", "Linea", "Compass", "Wrangler"], "HM": ["Outlander", "Pajero Sport"], "Force Motors": ["Trax", "Force One", "Gurkha"], "Ford": ["Figo", "Fusion", "Figo Aspire", "Freestyle", "Aspire", "Mustang", "Ecosport", "Endevaour"], "Jeep": ["Compass"], "JCB": [], "Tesla": [], "Yamaha": []}, "aliases": {"XUV": ["xcv", "xcb", "xev", "xt-v", "xevw"], "XUV 300": ["xuv300", "xuv-300", "xuv:300", "3oo"], "XUV 500": ["xuv500", "xuv-500", "xuv:500"], "XUV 700": ["xuv700", "xuv-700", "xuv:700"], "KUV 100": ["kuv100", "kuv-100", "kuv:100"], "KUV 100 NXT": ["kuv 100nxt", "kuv 100-nxt", "kuv 100:nxt"], "TUV 300": ["tuv300", "tuv-300", "tuv:300"], "TUV 300 plus": ["tuv 300plus", "tuv 300-plus", "tuv 300:plus"], "Vitara Brezza": ["brezza", "vitara"], "WR-V": ["wrv"], "BR-V": ["brv"], "CR-V": ["crv"], "Wagon R": ["wagonr"], "Ertiga": ["artiga", "atega", "attica"], "Datsun-go": ["datsun go"], "Datsun Redi-go": ["datsun redi go"], "i10": ["i-10"], "i20": ["i-20"], "A-Class": ["a class", "aclass"], "C-class": ["c class", "cclass"], "E-class": ["e class", "eclass"], "S-class": ["s class", "sclass"], "2-series": ["2 series", "2series", "two series"], "3-series": ["3 series", "3series", "three series"], "4-series": ["4 series", "4series", "four series"], "5-series": ["5 series", "5series", "five series"], "6-series": ["6 series", "6series", "six series"], "7-series": ["7 series", "7series", "seven series"], "8-series": ["8 series", "8series", "eight series"], "GT-R": ["gtr"], "Alto": ["ato"], "Dzire": ["desire"], "Xcent": ["excent"], "Gurkha": ["gorkha"], "Magnite": ["magnet"], "Swift vxi": ["vxi"], "Santro": ["sentro"], "Tata": ["thata"], "Venue": ["vanu"], "Scorpio": ["scarpu"], "Baleno": ["boleno"], "Quanto": ["konto"], "Kushaq": ["cushat"], "Creta": ["quetta"]}}, "question_db_parameters": {"question_db_type": "csv", "question_db": "", "get_verbatim_answers": 1}}

refinedparams = {"company_brand_info_parameters": {"focal_company_brand_info": {"Mahindra": ["XUV 300"]}, "competitor_company_brand_info": {"Mahindra": ["E20", "Verito", "Vibe", "Logan", "Quanto", "NuvoSport", "Bolero", "Bolero Plus", "Bolero Camper", "Bolero Pickup", "Bolero Neo", "Thar", "KUV 100", "KUV 100 NXT", "TUV 300", "TUV 300 Plus", "Scorpio", "XUV 500", "XUV 700", "Xylo", "Marazzo", "Alturas", "Rexton", "Jeeto", "Supro", "Ssangyong", "Quanto"], "Skoda": ["Fabia", "Rapid", "Octavia", "Superb", "Kodiaq", "Kushaq", "Karoq", "Slavia"], "Mercedez": ["A-Class", "C-Class", "C-Class Cabriolet", "E-Class", "S-Class", "CLS", "GLC Coupe", "GLC", "GLA", "GLE", "GLS", "G Class"], "Tata": ["Nano", "Indica V2", "Indica Vista", "Indica Xeta", "Bolt", "Tiago", "Altroz", "Zest", "Indigo CS", "Tigor", "Nexon", "Sumo", "Harrier", "Safari", "Hexa", "Punch"], "BMW": ["2-series", "3-series", "4-series", "5-series", "6-series", "7-series", "8-series", "M2", "M5", "M7", "M8", "X1", "X3", "X4", "X5", "X7"], "Volkswagen": ["Beetle", "Polo", "Polo GTI", "Ameo", "Vento", "Jetta", "Passat", "Taigun", "Tiguan", "Toarage", "Allspace", "T-Roc"], "Toyota": ["Liva Hatchback", "Glanza", "Etios Sedan", "Yaris", "Corolla", "Prius", "Camry", "Innova", "Innova Crysta", "Fortuner", "Landcruiser", "Prado", "Vellfire", "Urbancruiser", "Camper"], "Maruti Suzuki": ["Alto", "Ignis", "Baleno", "Swift", "S-Presso", "A-star", "Ritz", "Swift Dzire", "Swift Dzire Tour", "Celerio", "Celerio X", "Ciaz", "Ertiga", "S-Cross", "Gypsy", "Vitara Brezza", "XL 6", "Wagon R", "Swift VXi", "Zen", "Dzire"], "Chevrolet": [
    "Aveo U-VA", "Aveo", "Beat", "Spark", "Enjoy", "Sail", "Tavera", "Cruze", "Optra SRV", "Optra", "Daewoo Matiz", "Trailblazer"], "Audi": ["A3", "A3 Cabriolet", "A4", "A6", "A7", "A8L", "R8", "Q2", "Q3", "Q5", "Q7", "Q8"], "Kia": ["Seltos", "Carens", "Sonet", "Carnival", "K7"], "Nissan": ["Micra", "Datsun-go", "Datsun Redi-go", "Sunny", "Teana", "GT-R", "Go+", "Terrano", "Kicks", "Magnite"], "Honda": ["Brio", "Jazz", "Amaze", "City", "Civic", "Accord", "WR-V", "BR-V", "CR-V"], "MG": ["Hector", "ZS-EV", "Hector Plus"], "Hyundai": ["Eon", "Santro Xing", "i20", "Elite i20", "i20 Active", "i10", "Aura", "Xcent", "Accent", "Verna", "Elantra", "Creta", "Alcazar", "Venue", "Kona", "Tucson", "Santa Fe"], "Renault": ["Kwid", "Pulse", "Captur", "Duster", "Kiger", "Lodgy"], "Fiat": ["Awventura", "Palio", "UNO", "Punto", "Punto Evo", "Grande Punto", "Abarth", "Abarth Punto", "Urban Cross", "Linea", "Compass", "Wrangler"], "HM": ["Outlander", "Pajero Sport"], "Force Motors": ["Trax", "Force One", "Gurkha"], "Ford": ["Figo", "Fusion", "Figo Aspire", "Freestyle", "Aspire", "Mustang", "Ecosport", "Endevaour"], "Jeep": ["Compass"], "JCB": [], "Tesla": [], "Yamaha": []}}, "question_db_parameters": {"question_db_type": "csv", "question_db": ""}, "raw_answer_parameters": {"raw_answer_db_type": "", "raw_answer_db": "", "input_raw_answer_column_name": "Raw_Answers"}}

# # Parameter values to upload Raw Output file to s3
# local_folder_raw = config['raw_ans']['folder_to_save_output_files']
# s3_folder_raw = config['raw_ans']['s3_folder_raw']
# # Parameter values to upload Refined Output file to s3
# local_folder_refined = config["refined_ans"]['folder_to_save_output_files']
# s3_folder_refined = config["refined_ans"]['s3_folder_refined']
# transcribe_output_folder_path = config['transcribe']['local_folder_to_save_output_files']


def call_raw_ans_api_with_retries(url, params, headers, max_retries=3):
    for i in range(max_retries):
        raw_ans_output = requests.post(url=url, json=params, headers=headers)
        if raw_ans_output.status_code == 200 and 'status_code' in raw_ans_output.json() and raw_ans_output.json()['status_code'] == 0:
            return raw_ans_output.json()['raw_answer_result']
        else:
            print(
                f"Raw API call failed with status code {raw_ans_output.status_code}. Retrying... (Attempt {i + 1})")
            time.sleep(2)
    print(f"Raw API call failed after {max_retries} retries.")
    return None


def call_refined_ans_api_with_retries(url, params, headers, max_retries=3):
    for i in range(max_retries):
        refined_ans_output = requests.post(
            url=url, json=params, headers=headers)
        if refined_ans_output.status_code == 200 and 'status_code' in refined_ans_output.json() and refined_ans_output.json()['status_code'] == 0:
            return refined_ans_output.json()['refined_answer_result']
        else:
            print(
                f"Refined API call failed with status code {refined_ans_output.status_code}. Retrying... (Attempt {i + 1})")
            time.sleep(2)
    print(f"Refined API call failed after {max_retries} retries.")
    return None


def replace_quotes(data):
    a = str(data)
    b = a.replace("'caller'", "\"caller\"").replace(
        "'respondent'", "\"respondent\"").replace(" '", " \"").replace("', ", " \", ")
    return b


def create_directory_if_not_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")
    else:
        print(f"Directory '{directory_path}' already exists.")

# create_directory_if_not_exists(directory_path)


def get_raw_param_data_from_transcribe_output(folder_path_to_read_files):
    transcribed_data = []
    for filename in os.listdir(folder_path_to_read_files):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path_to_read_files, filename)
            with open(file_path, 'r') as file:
                try:
                    json_data = json.load(file)
                    transcribed_text = json_data.get("transcribed_text")
                    if transcribed_text is not None:
                        transcribed_text = replace_quotes(transcribed_text)
                        transcribed_data.append((filename, transcribed_text))
                except json.JSONDecodeError:
                    print(f"Error decoding JSON in {filename}")
    return transcribed_data


def change_parameters(question_db, transcribe_output_folder_path, raw_url, headers, local_folder_raw, refined_url, local_folder_refined):
    create_directory_if_not_exists(local_folder_raw)
    create_directory_if_not_exists(local_folder_refined)
    transcribed_list = get_raw_param_data_from_transcribe_output(
        transcribe_output_folder_path)
    text_paths = []
    params = []
    for filename, transcribed_text in transcribed_list:
        print(filename)
        rawparams_copy = copy.deepcopy(rawparams)
        rawparams_copy["data_parameters"]["data_type"] = "list"
        rawparams_copy["data_parameters"]["data"] = eval(transcribed_text)
        rawparams_copy["question_db_parameters"]["question_db"] = question_db
        text_paths.append(filename)
        params.append(rawparams_copy)
        # print(rawparams_copy)
        raw_ans_text = call_raw_ans_api_with_retries(
            raw_url, rawparams_copy, headers)
        if raw_ans_text is not None:
            store_raw_ans_text(filename, raw_ans_text, local_folder_raw)
            print(f"Raw ans for {filename} saved successfully")
        refinedparams_copy = copy.deepcopy(refinedparams)
        refinedparams_copy["question_db_parameters"]["question_db"] = question_db
        refinedparams_copy["raw_answer_parameters"]["raw_answer_db_type"] = "json"
        refinedparams_copy["raw_answer_parameters"]["raw_answer_db"] = raw_ans_text
        params.append(refinedparams_copy)
        # print(refinedparams_copy)
        refined_ans_text = call_refined_ans_api_with_retries(
            refined_url, refinedparams_copy, headers)
        if refined_ans_text is not None:
            store_refined_ans_text(
                filename, refined_ans_text, local_folder_refined)
            print(f"Refined ans for {filename} saved successfully")
    return params


def store_raw_ans_text(filename, raw_ans_text, local_folder_raw):
    output_folder = local_folder_raw
    output_file = os.path.join(output_folder, f"{filename}")
    with open(output_file, 'w') as f:
        json.dump(raw_ans_text, f)


def store_refined_ans_text(filename, refined_ans_text, local_folder_refined):
    output_folder = local_folder_refined
    output_file = os.path.join(output_folder, f"{filename}")
    with open(output_file, 'w') as f:
        json.dump(refined_ans_text, f)


def upload_files_to_s3(local_folder_path, bucket_name, s3_folder_path=''):
    s3 = boto3.client('s3')

    for root, dirs, files in os.walk(local_folder_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            s3_key = os.path.join(s3_folder_path, file)

            s3.upload_file(local_file_path, bucket_name, s3_key)

            print(
                f"Uploaded {local_file_path} to S3 bucket {bucket_name} with key: {s3_key}")


# change_parameters(question_db, transcribe_output_folder_path,
#                   raw_url, headers, local_folder_raw, refined_url, local_folder_refined)
# upload_files_to_s3(local_folder_raw, bucket_name, s3_folder_raw)
# upload_files_to_s3(local_folder_refined, bucket_name, s3_folder_refined)
