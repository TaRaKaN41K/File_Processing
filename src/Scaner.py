import py7zr
import os
import shutil
import requests
import yara
import json
from requests import Response

from MinIO_client import MinIOClient


class Scaner:
    @staticmethod
    def scan(
            day: int,
            archive_path: str,
            archive_password: str,
            downloads_directory: str,
            extracted_files_directory: str,
            rules_directory: str,
            scan_results_directory: str,
            scan_result_bucket_name: str,
            virus_bucket_name: str,
            endpoint_url: str,
            aws_access_key_id: str,
            aws_secret_access_key: str,
            region_name: str
    ) -> None:
        try:
            # Скачиваем файлы с VX Underground по числу месяца
            download_archive_path: str = Scaner.__download_files(
                day=day,
                archive_path=archive_path,
                downloads_directory=downloads_directory
            )

            # Извлекаем архив в указанную директорию
            Scaner.__extract_files(
                archive_path=download_archive_path,
                archive_password=archive_password,
                extracted_files_directory=extracted_files_directory
            )

            # Загружаем YARA правила
            rules: yara.Rules = Scaner.__load_yara_rules(rules_directory=rules_directory)

            # Сканируем файлы с YARA
            results: list[dict] = Scaner.__scan_files_with_yara(
                rules=rules,
                files_directory=extracted_files_directory
            )

            # Сохраняем результаты в JSON-файле
            Scaner.__save_scan_results_to_json(
                results=results,
                output_file=f'{scan_results_directory}/scan_results.json'
            )

            Scaner.__upload_in_minio_s3(
                endpoint_url=endpoint_url,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
                scan_result_bucket_name=scan_result_bucket_name,
                virus_bucket_name=virus_bucket_name,
                extracted_files_directory=extracted_files_directory,
                scan_results_directory=scan_results_directory,
            )

            # Удаляем директории из локального хранилища
            Scaner.__delete_directories(
                list_directories=[extracted_files_directory, downloads_directory, scan_results_directory]
            )
        except Exception as e:
            raise Exception(f"Error: {e}")

    @staticmethod
    def __upload_in_minio_s3(
            endpoint_url: str,
            aws_access_key_id: str,
            aws_secret_access_key: str,
            region_name: str,
            scan_result_bucket_name: str,
            virus_bucket_name: str,
            extracted_files_directory: str,
            scan_results_directory: str,
    ) -> None:

        minio: MinIOClient = MinIOClient(
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

        # Создаём buckets для результатов сканирования и архива с вирусами
        for bucket_name in [scan_result_bucket_name, virus_bucket_name]:

            if not minio.create_bucket(
                bucket_name=bucket_name
            ):
                raise Exception("Error creating bucket")

        # Список с парами директорий и бакетов
        directories_and_buckets: list[tuple[str, str]] = [
            (extracted_files_directory, virus_bucket_name),
            (scan_results_directory, scan_result_bucket_name)
        ]

        # Проходим по парам и загружаем файлы в соответствующие бакеты в MinIO (S3)
        for directory, bucket_name in directories_and_buckets:

            if not minio.upload_files(
                src_directory=directory,
                bucket_name=bucket_name
            ):
                raise Exception(f"Error uploading files to bucket {bucket_name}")

    @staticmethod
    def __download_vx_file(
            url: str,
            save_path: str
    ) -> str:

        try:
            print(f"Начинаем скачивание файла с URL: {url}")

            response: Response = requests.get(url, allow_redirects=True)

            # Проверка статуса ответа (исключение при 4xx, 5xx)
            response.raise_for_status()

            # Создание директории для сохранения файла, если она не существует
            if os.makedirs(os.path.dirname(save_path), exist_ok=True):
                print(f"Папка {save_path} была создана.")

                # Чтение контента и запись в файл
            with open(save_path, 'wb') as file:
                file.write(response.content)  # запись содержимого ответа

            print(f"Файл сохранён как {save_path}")
            return save_path

        except requests.exceptions.HTTPError as http_err:
            raise Exception(f"HTTP ошибка при скачивании файла: {http_err}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Ошибка при скачивании файла: {e}")
        except Exception as e:
            raise Exception(f"Произошла ошибка: {e}")

    @staticmethod
    def __download_files(
            day: int,
            archive_path: str,
            downloads_directory: str
    ) -> str:

        day_str: str = f'0{day}' if 1 <= day <= 9 else f'{day}'

        # Формируем URL и пути для скачивания файлов
        url_virus_sign_collection: str = f"{archive_path}{day_str}.7z"

        save_path_virus_sign_collection: str = f"{downloads_directory}/VirusSign Collection/Virussign.2024.11.{day_str}.7z"

        # Скачивание файлов
        path: str = Scaner.__download_vx_file(url=url_virus_sign_collection, save_path=save_path_virus_sign_collection)

        return path

    @staticmethod
    def __extract_files(
            archive_path: str,
            archive_password: str,
            extracted_files_directory: str
    ) -> bool:

        try:
            # Проверяем, существует ли архив
            if not os.path.exists(archive_path):
                raise Exception(f"Архив не найден: {archive_path}")

            print(f"Извлечение архива в {extracted_files_directory}")
            # Извлечение файлов из архива
            if archive_path.endswith(".7z"):
                with py7zr.SevenZipFile(archive_path, mode='r', password=archive_password) as z:
                    z.extractall(path=extracted_files_directory)
                    print(f"Папка {extracted_files_directory} была создана.")
                    print(f"Извлечено {len(z.list())} файлов в {extracted_files_directory}")
            else:
                raise Exception("Неподдерживаемый формат архива.")
            return True

        except Exception as e:
            raise Exception(f"Произошла ошибка: {e}")

    @staticmethod
    def __load_yara_rules(rules_directory: str) -> yara.Rules:
        """Загружает и компилирует все YARA-правила из указанной директории."""
        rule_files: list[str] = [
            os.path.join(rules_directory, file)
            for file in os.listdir(rules_directory)
            if file.endswith('.yar') or file.endswith('.yara')
        ]

        if not rule_files:
            raise FileNotFoundError(f"Не найдено YARA-правил в директории: {rules_directory}")

        rules: yara.Rules = yara.compile(filepaths={str(i): rule_files[i] for i in range(len(rule_files))})

        return rules

    @staticmethod
    def __scan_files_with_yara(rules: yara.Rules, files_directory: str) -> list[dict]:
        """Сканирует файлы из директории с использованием набора YARA-правил."""
        scan_results: list[dict[str, str | list[str]]] = []

        if not os.path.isdir(files_directory):
            raise FileNotFoundError(f"Указанная директория не существует: {files_directory}")

        for root, _, files in os.walk(files_directory):
            for file in files:
                file_path: str = os.path.join(root, file)
                matches: yara.match = rules.match(file_path)

                for match in matches:
                    # Сканируем строки внутри каждого совпадения
                    strings: list[str] = [str(string_match) for string_match in match.strings]
                    scan_results.append({
                        'file': file,
                        'rule': match.rule,
                        'tags': match.tags,
                        'meta': match.meta,
                        'strings': strings
                    })
        return scan_results

    @staticmethod
    def __save_scan_results_to_json(results: list[dict], output_file: str) -> None:
        """Сохраняет результаты сканирования в JSON файл."""
        # Извлекаем путь к папке из пути к файлу
        folder: str = os.path.dirname(output_file)

        # Проверяем, существует ли папка, если нет — создаем
        if not os.path.exists(folder):
            os.makedirs(folder)
            print(f"Папка {folder} была создана.")

        # Сохраняем результаты в JSON файл
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=4)
        print(f"Результаты сканирования сохранены в {output_file}")

    @staticmethod
    def __delete_directories(
            list_directories: list[str]
    ) -> None:

        for directory in list_directories:
            shutil.rmtree(directory)
            print(f"Удалена {directory}")
