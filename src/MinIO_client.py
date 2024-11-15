import boto3
import os
from botocore.exceptions import ClientError
from botocore.client import BaseClient
from typing import Dict


class MinIOClient:
    def __init__(
            self,
            endpoint_url: str,
            aws_access_key_id: str,
            aws_secret_access_key: str,
            region_name: str,
    ) -> None:

        connection_parameters: dict[str, str] = {
            'endpoint_url': endpoint_url,
            'aws_access_key_id': aws_access_key_id,
            'aws_secret_access_key': aws_secret_access_key,
            'region_name': region_name,
        }

        # Подключение к MinIO
        self.s3_client: BaseClient = self.__create_connection_with_minio(connection_parameters=connection_parameters)

    @staticmethod
    def __create_connection_with_minio(connection_parameters: Dict[str, str]) -> BaseClient | bool:

        try:
            s3_client: BaseClient = boto3.client(
                's3',
                endpoint_url=connection_parameters['endpoint_url'],
                aws_access_key_id=connection_parameters['aws_access_key_id'],
                aws_secret_access_key=connection_parameters['aws_secret_access_key'],
                region_name=connection_parameters['region_name'],
            )

            return s3_client
        except ClientError as e:
            raise Exception(f"Ошибка при подключении к MinIO: {e}")

    def create_bucket(self, bucket_name: str) -> bool:
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as e:
            error_code: str = e.response['Error']['Code']
            if error_code == '404':
                print(f"Бакет '{bucket_name}' не существует.")
            elif error_code == '403':
                print(f"Доступ к бакету '{bucket_name}' ограничен.")
            else:
                print(f"Ошибка при проверке бакета '{bucket_name}': {e}")

            try:
                self.s3_client.create_bucket(Bucket=bucket_name)
                print(f"Бакет '{bucket_name}' создан")
                return True
            except Exception as e:
                raise Exception(f"Ошибка при создании бакета '{bucket_name}': {e}")

    def upload_files(self, src_directory: str, bucket_name: str) -> bool:
        try:
            # Загрузка файлов в MinIO (S3)
            src_files: list[str] = os.listdir(src_directory)

            print("Начало загрузки в MinIO (S3)")
            print(f"Всего файлов: {len(src_files)}")

            download_files_count: int = 0
            for file_name in src_files:
                file_path: str = os.path.join(src_directory, file_name)
                with open(file_path, 'rb') as file_data:

                    # Загрузка файла в MinIO
                    self.s3_client.upload_fileobj(file_data, bucket_name, file_name)
                    download_files_count += 1

            print(f"Загружено {download_files_count}/{len(src_files)} файлов в MinIO (S3)")
            return True

        except Exception as e:
            raise Exception(f"Произошла ошибка: {e}")
