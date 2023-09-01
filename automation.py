def main():
    process_list = {
        "Uncompress": 0,
        "Transcript": 1,
        "GetIdea": 1,
        "GetAnswer": 0,
        "Flattening": 0,
        "Mapping": 0,
    }
    for key, value in process_list.items():
        if key == "Uncompress" and value == 1:
            pass

        if key == "Transcript" and value == 1:
            from core.common import initTranscriptEngine
            from core.transcription import transcribe
            trn_config = initTranscriptEngine()
            transcribe(**trn_config)
        if key == "GetIdea" and value == 1:
            from core.common import initGetIdeas
            from core.get_ideas import get_ideas_api
            get_idea_config = initGetIdeas()
            get_ideas_api(**get_idea_config)

        if key == "GetAnswer" and value == 1:
            pass

        if key == "Flattening" and value == 1:
            pass

        if key == "Mapping" and value == 1:
            pass


if __name__ == "__main__":
    main()
