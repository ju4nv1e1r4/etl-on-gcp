from google.cloud import storage

class CloudStorageOps:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(bucket_name)

    def load_parquet_from_bucket(self, file_path: str):
        """Baixa um arquivo Parquet do bucket e retorna os bytes"""
        blob = self.bucket.blob(file_path)
        
        try:
            parquet_data = blob.download_as_bytes()
            return parquet_data
        except Exception as e:
            print(f"Erro ao baixar parquet: {e}")
            return None

    def list_from_bucket(self):
        """Lista todos os arquivos no bucket"""
        blobs = self.storage_client.list_blobs(self.bucket_name)
        for blob in blobs:
            print(blob.name)

    def delete_from_bucket(self, file_path):
        """Deleta um arquivo específico do bucket"""
        my_bucket = self.storage_client.bucket(self.bucket_name)
        blob = my_bucket.blob(file_path)
        generation_match_precondition = None

        blob.reload()
        generation_match_precondition = blob.generation
        blob.delete(if_generation_match=generation_match_precondition)

        return print(f"File deleted: '{file_path}'.")

if __name__ == "__main__":
    CloudStorageOps()