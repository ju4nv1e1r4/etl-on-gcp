package workers

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/storage"
)

func UploadParquetOnGCS(parquetData[]byte, bucketName string, objectName string) error {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %w", err)
	}

	defer client.Close()
	log.Printf("Tamanho dos dados para upload: %d bytes", len(parquetData))

	bucket := client.Bucket(bucketName)
	object := bucket.Object(objectName)
	w := object.NewWriter(ctx)

	if _, err := w.Write(parquetData); err != nil {
		return fmt.Errorf("erro ao escrever no GCS: %w", err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("erro ao fechar o writer do GCS: %w", err)
	}

	log.Printf("Arquivo Parquet salvo no GCS: gs://%s/%s", bucketName, objectName)
	return nil
}