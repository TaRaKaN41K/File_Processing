import os
import argparse
from dotenv import load_dotenv

from Scaner import Scaner


def main():
    # Загружает переменные из .env
    load_dotenv()

    extracted_files_directory = os.getenv("EXTRACTED_FILES_DIRECTORY")
    downloads_directory = os.getenv("DOWNLOADS_DIRECTORY")
    scan_results_directory = os.getenv("SCAN_RESULTS_DIRECTORY")
    rules_directory = os.getenv("RULES_DIRECTORY")

    virus_bucket_name = os.getenv('VIRUS_BUCKET_NAME')
    scan_result_bucket_name = os.getenv('SCAN_RESULTS_BUCKET_NAME')

    archive_path = os.getenv("ARCHIVES_PATH")
    archive_password = os.getenv("MINIO_PASS")

    endpoint_url = os.getenv('MINIO_URL')
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    region_name = os.getenv('REGION_NAME')

    # Создаем парсер аргументов командной строки
    parser = argparse.ArgumentParser(description="Скачивание файлов с VX Underground за указанный день месяца.")
    parser.add_argument("day", type=int, help="День месяца (1-31)")

    # Парсим аргументы
    args = parser.parse_args()

    day = args.day

    Scaner.scan(
        day=day,
        extracted_files_directory=extracted_files_directory,
        downloads_directory=downloads_directory,
        scan_results_directory=scan_results_directory,
        rules_directory=rules_directory,
        virus_bucket_name=virus_bucket_name,
        scan_result_bucket_name=scan_result_bucket_name,
        archive_path=archive_path,
        archive_password=archive_password,
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region_name,
    )


if __name__ == '__main__':
    main()

