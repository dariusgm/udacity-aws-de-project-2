import glob
import json
import os


def combine_events(base_path):
    """
    This combines all log events from the local copy of the udacity s3 bucket
    and writes it out as one single file.

    This improves the COPY execution on redshift a lot.
    :param base_path: working directory, where all files are resolved
    :return: None
    """
    if not os.path.exists("events.json"):
        result = []
        for file in glob.glob(base_path + "/**/*.json", recursive=True):
            with open(file, 'rt') as log_reader:
                # extend the payload with the file date for analysis or partition use
                file_suffix = file.split(os.sep)[-1]
                file_date = file_suffix.replace("-events.json", "")
                event_year, event_month, event_day = file_date.split("-")

                for line in log_reader:
                    json_payload = json.loads(line)
                    json_payload['event_day'] = event_day
                    json_payload['event_month'] = event_month
                    json_payload['event_year'] = event_year
                    result.append(json_payload)

        with open("events.json", "wt") as json_writer:
            for line in result:
                json_writer.write(json.dumps(line) + "\n")


def combine_songs(base_path):
    """
    This combines all song data from the local copy of the udacity s3 bucket
    and writes it out as one single file.

    This improves the COPY execution on redshift a lot.
    :param base_path: working directory, where all files are resolved
    :return: None
    """
    if not os.path.exists("songs.json"):
        result = []
        for file in glob.glob(base_path + "/**/*.json", recursive=True):
            with open(file, 'rt') as log_reader:
                line = log_reader.read()
                json_payload = json.loads(line)
                result.append(json_payload)

        with open("songs.json", "wt") as json_writer:
            for line in result:
                json_writer.write(json.dumps(line) + "\n")


def main():
    log_path = "log_data"
    song_path = "song_data"
    combine_events(os.path.join(".", log_path))
    combine_songs(os.path.join(".", song_path))


if __name__ == '__main__':
    main()
