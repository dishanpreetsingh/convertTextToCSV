import csv
import boto3
import os
from dotenv import load_dotenv
import chardet

load_dotenv()

aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_REGION")

bucket_name = "cerebrumdev"  
folder_name = "csvFiles/" 

input_file = os.getenv("INPUT_FILE_PATH")


output_file = "output_file.csv" 
s3_file_key = folder_name + os.path.basename(output_file) 


def detect_file_encoding(file_path, num_bytes=100000):

    with open(file_path, "rb") as file:
        raw_data = file.read(num_bytes)
        result = chardet.detect(raw_data)
        encoding = result["encoding"]
        print(f"Detected file encoding: {encoding}")
        return encoding


def convert_large_text_to_csv(input_file, output_file, delimiter="\t", chunk_size=100000):

    try:
        encoding = detect_file_encoding(input_file)

        with open(input_file, "r", encoding=encoding) as infile, open(output_file, "w", encoding="utf-8", newline="") as outfile:
            writer = csv.writer(outfile, quoting=csv.QUOTE_ALL)
            chunk = []

            for line_number, line in enumerate(infile, start=1):
                chunk.append(line.strip().split(delimiter))

                if line_number % chunk_size == 0:
                    writer.writerows(chunk)
                    chunk = []

            if chunk:
                writer.writerows(chunk)

            print(f"CSV conversion completed! File saved as: {output_file}")
    except Exception as e:
        print(f"An error occurred during conversion: {e}")


def upload_to_s3(file_path, bucket_name, s3_file_key):
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region,
        )

        s3_client.upload_file(file_path, bucket_name, s3_file_key)
        print(f"File uploaded successfully to s3://{bucket_name}/{s3_file_key}")
    except Exception as e:
        print(f"An error occurred during upload: {e}")


if __name__ == "__main__":
    convert_large_text_to_csv(input_file, output_file, delimiter="\t")

    upload_to_s3(output_file, bucket_name, s3_file_key)
