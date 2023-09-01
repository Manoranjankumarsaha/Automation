S3PATH = "/home/ubuntu/s3_bucket/"
# S3PATH = "/home/ubuntu/"
AUDIO_EXT = "wav"
S3_BUCKET_IFNO = {
    "service_name": 's3',
    "bucket_name": "ixolerator-cloud",
    "region_name": "ap-south-1",
    "aws_access_key_id": "AKIATWZQTUP5P4QMO43H",
    "aws_secret_access_key": "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"
}

GPU_SERVER = "https://stt.meghnad.inxiteout.ai:5011"
VTI_SERVER = "http://demolive.meghnad.inxiteout.ai:5009"
IDEA_SERVER = "http://demolive.meghnad.inxiteout.ai:5007"
# VTI_SERVER = "http://13.234.134.87:5009"
# IDEA_SERVER = "http://13.234.134.87:5007"

TRANSCRIBE_URL = GPU_SERVER+"/transcribe"
GET_RAW_URL = VTI_SERVER+"/get_qa_raw_answers"
GET_REFINED_URL = VTI_SERVER + "/get_qa_refined_answers"
GET_IDEA_URL = IDEA_SERVER+"/get_ideas"

# TRANSCRIBE_PARAM = {"data": "", "number_of_speakers": 2,"lang": "en", "enable_pre_process": 0, "keep_original_lang": 0}
TRANSCRIBE_PARAM = {"data": "", "number_of_speakers": 2, "lang": "en",
                    "enable_post_process_segmentation": 1, "root_s3": "/home/ubuntu/s3_bucket/"}

GET_IDEA_PAYLOAD = {
    "data_parameters": {
        "data_type": "list",
        "data": []
    },
    "company_name_parameters": {
        "industry": "commercial vehicle"
    }
}
# RAW_PAYLOAD = {
#     "data_parameters": {"data": "", "data_type": "txt", "mode": "conversation"},
#     "pre_processing_parameters": {"do_punctuation_restoration": 0, "non_en_lang": 0},
#     "company_brand_info_parameters": {
#         "focal_company_brand_info": {
#             "Mahindra": ["supro profit mini"]
#         },
#         "competitor_company_brand_info": {
#             "Mahindra": ["Jeeto", "Bolero Pickup", "Supro Profit Maxi", "Supro profit CNG Duo"],
#             "Tata": ["Ace Gold", "Intra V10"],
#             "Maruti Suzuki": ["Super Carry"],
#             "Ashok Leyland": ["Dost"],
#             "Scooters India Limited": ["Vikram"]
#         },
#         "aliases": {
#             "supro": ["supra", "supero", "supera", "supreme"],
#             "supro profit mini": ["supro mini", "profit truck"],
#             "Ace Gold": ["chhota hathi", "chota hathi", "chhota haathi", "chota haathi"],
#             "Profit": ["Prophet"],
#             "Mahindra": ["Mhindra", "Mahendra", "Mehindra"],
#             "Ashok Leyland": ["ashok Lehlam"],
#             "Bolero": ["bulero", "beloy"],
#             "jeeto": ["jeetu", "jito"]
#         }
#     },
#     "question_db_parameters": {"question_db": "", "question_db_type": "csv", "get_verbatim_answers": 1}
# }


# V2 PARAMS:-----------------------------------------------------
# RAW_PAYLOAD = {
#     "data_parameters": {"data": "", "data_type": "txt", "mode": "conversation"},
#     "pre_processing_parameters": {"do_punctuation_restoration": 0, "non_en_lang": 0},
#     "company_brand_info_parameters": {"focal_company_brand_info": {"Mahindra": ["supro profit mini"]},
#                                       "competitor_company_brand_info": {
#         "Mahindra": ["Jeeto", "Bolero Pikup", "Bolero Maxx Pikup", "Supro Profit Maxi", "Supro profit CNG Duo", "Alpha Auto"],
#         "Tata": ["Ace Gold", "Intra", "Magic"],
#         "Maruti Suzuki": ["Super Carry"],
#         "Ashok Leyland": ["Dost"],
#         "Scooters India Limited": ["Vikram"],
#         "Auto": ["chakado"]
#     },
#         "aliases": {
#         "supro": ["supra", "supero", "supera", "supreme"],
#         "supro profit mini": ["supro mini", "profit truck", "profit mini", "mini profit"],
#         "Ace Gold": ["chhota hathi", "chota hathi", "chhota haathi", "chota haathi", "ace"],
#         "Profit": ["Prophet"], "Mahindra": ["Mhindra", "Mahendra", "Mehindra"],
#         "Ashok Leyland": ["ashok Lehlam"], "Bolero Pikup": ["bulero", "beloy", "bolero", "bolero pickup"],
#         "Bolero Maxx Pikup": ["bolero max", "max pickup", "bolero max pickup"], "jeeto": ["jeetu", "jito"],
#         "Supro Profit Maxi": ["supro maxi", "profit maxi", "maxi profit", "profit max", "supro max"],
#         "Supro profit CNG Duo": ["supro cng", "supro duo", "profit duo"]}},
#     "question_db_parameters": {"question_db": "", "question_db_type": "csv", "get_verbatim_answers": 1}}


# V3 PARAMS:---------------------------------------------------------
# RAW_PAYLOAD = {"data_parameters": {"data": "", "data_type": "txt", "mode": "conversation"},
#                "pre_processing_parameters": {"do_punctuation_restoration": 0, "non_en_lang": 0},
#                "company_brand_info_parameters":
#                {"focal_company_brand_info": {"Mahindra": ["supro profit mini"]},
#                 "competitor_company_brand_info": {"Mahindra": ["Jeeto", "Jeeto Plus", "Bolero Pikup", "Bolero Maxx Pikup", "Supro Profit Maxi", "Supro profit CNG Duo", "Alpha Auto"], "Tata": ["Ace Gold", "Intra", "Magic"], "Maruti Suzuki": ["Super Carry"], "Ashok Leyland": ["Dost"], "Scooters India Limited": ["Vikram"], "Auto": ["chakado"]},
#                 "aliases": {"supro": ["supra", "supero", "supera", "supreme"], "supro profit mini": ["supro mini", "profit truck", "profit mini", "mini profit"], "Ace Gold": ["chhota hathi", "chota hathi", "chhota haathi", "chota haathi", "ace", "small elephant", "tata s"], "Profit": ["Prophet"], "Mahindra": ["Mhindra", "Mahendra", "Mehindra"], "Ashok Leyland": ["ashok Lehlam"], "Bolero Pikup": ["bulero", "beloy", "bolero", "bolero pickup"], "Bolero Maxx Pikup": ["bolero max", "max pickup", "bolero max pickup"], "jeeto": ["jeetu", "jito", "zeta"], "Supro Profit Maxi": ["supro maxi", "profit maxi", "maxi profit", "profit max", "supro max", "maxi truck"], "Supro profit CNG Duo": ["supro cng", "supro duo", "profit duo"], "Carry": ["kairi", "hikari"], "pikup": ["pickup", "pick up"]}},
#                "question_db_parameters": {"question_db": "", "question_db_type": "csv", "get_verbatim_answers": 1}}


# V4 PARAMS:-----------------------------------------------------------------
RAW_PAYLOAD = {"data_parameters": {"data": "", "data_type": "txt", "mode": "conversation"}, "pre_processing_parameters": {"do_punctuation_restoration": 0, "non_en_lang": 0}, "company_brand_info_parameters": {"focal_company_brand_info": {"Mahindra": ["supro profit mini"]}, "competitor_company_brand_info": {"Mahindra": ["Jeeto", "Jeeto Plus", "Bolero Pikup", "Bolero Maxx Pikup", "Supro Profit Maxi", "Supro profit CNG Duo", "Alpha Auto"], "Tata": ["Ace Gold", "Intra", "Magic"], "Maruti Suzuki": ["Super Carry"], "Ashok Leyland": ["Dost"], "Scooters India Limited": ["Vikram"], "Auto": ["Auto", "Chakado"]}, "aliases": {"supro": ["supra", "supero", "supera", "supreme"], "supro profit mini": ["supro mini", "profit truck", "profit mini", "mini profit"], "Ace Gold": ["chhota hathi", "chota hathi", "chhota haathi", "chota haathi", "ace", "small elephant", "tata s"], "Profit": ["Prophet"], "Mahindra": ["Mhindra", "Mahendra", "Mehindra"], "Ashok Leyland": ["ashok Lehlam"], "Bolero Pikup": ["bulero", "beloy", "bolero", "bolero pickup"], "Bolero Maxx Pikup": ["bolero max", "max pickup", "bolero max pickup"], "jeeto": ["jeetu", "jito", "zeta"], "Supro Profit Maxi": ["supro maxi", "profit maxi", "maxi profit", "profit max", "supro max", "maxi truck"], "Supro profit CNG Duo": ["supro cng", "supro duo", "profit duo"], "Carry": ["kairi", "hikari"], "pikup": ["pickup", "pick up"]}},
               "question_db_parameters": {"question_db": "", "question_db_type": "csv", "get_verbatim_answers": 1, "high_complexity": 1}}


# REFINED_PAYLOAD = {
#     "company_brand_info_parameters": {
#         "focal_company_brand_info": {
#             "Mahindra": ["supro profit mini"]
#         },
#         "competitor_company_brand_info": {
#             "Mahindra": ["Jeeto"],
#             "Tata": ["Ace Gold", "Intra V10"],
#             "Maruti Suzuki": ["Super Carry"],
#             "Ashok Leyland": ["Dost"]
#         }
#     },
#     "question_db_parameters": {
#         "question_db": "",
#         "question_db_type": "csv"
#     },
#     "raw_answer_parameters": {
#         "raw_answer_db_type": "json",
#         "raw_answer_db": [],
#         "input_raw_answer_column_name": "Raw_Answers"
#     }
# }

# V2 PARAMS:----------------------------------------------------------------
# REFINED_PAYLOAD = {
#     "company_brand_info_parameters": {
#         "focal_company_brand_info": {"Mahindra": ["Jeeto", "Bolero Pikup", "Bolero Maxx Pikup", "Supro Profit Maxi", "Supro profit CNG Duo"],
#                                      "Tata": ["Ace Gold", "Intra", "Magic"],
#                                      "Maruti Suzuki": ["Super Carry"],
#                                      "Ashok Leyland": ["Dost"],
#                                      "Scooters India Limited": ["Vikram"],
#                                      "Auto": ["Auto"]
#                                      },
#     },
#     "question_db_parameters": {
#         "question_db": "",
#         "question_db_type": "csv"
#     },
#     "raw_answer_parameters": {
#         "raw_answer_db_type": "json",
#         "raw_answer_db": [],
#         "input_raw_answer_column_name": "Raw_Answers"
#     }
# }


# V3 REFINED PARAMS
# REFINED_PAYLOAD = {"company_brand_info_parameters": {"focal_company_brand_info": {"Mahindra": ["Jeeto", "Bolero Pikup", "Bolero Maxx Pikup", "Supro Profit Maxi", "Supro profit CNG Duo"], "Tata": ["Ace Gold", "Intra", "Magic"], "Maruti Suzuki": ["Super Carry"], "Ashok Leyland": ["Dost"], "Scooters India Limited": ["Vikram"], "Auto": ["Auto"]}},
#                    "question_db_parameters": {"question_db": "", "question_db_type": "csv"},
#                    "raw_answer_parameters": {"raw_answer_db_type": "json", "raw_answer_db": [], "input_raw_answer_column_name": "Raw_Answers"}}


# V4 PARAMS:-------------------------------------------------------------
REFINED_PAYLOAD = {"company_brand_info_parameters": {"focal_company_brand_info": {"Mahindra": ["supro profit mini"]}, "competitor_company_brand_info": {"Mahindra": ["Jeeto", "Jeeto Plus", "Bolero Pikup", "Bolero Maxx Pikup", "Supro Profit Maxi", "Supro profit CNG Duo", "Alpha Auto"], "Tata": ["Ace Gold", "Intra", "Magic"], "Maruti Suzuki": [
    "Super Carry"], "Ashok Leyland": ["Dost"], "Scooters India Limited": ["Vikram"], "Auto": ["Auto", "Chakado"]}}, "question_db_parameters": {"question_db": "", "question_db_type": "csv"}, "raw_answer_parameters": {"raw_answer_db_type": "json", "raw_answer_db": [], "input_raw_answer_column_name": "Raw_Answers"}}
