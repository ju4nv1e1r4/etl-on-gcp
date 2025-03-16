package workers

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/storage"
)

// UploadParquetOnGCS uploads a parquet file to a bucket in Google Cloud Storage.
// This function receives: parquet file, bucket name and file name in the destination.
func UploadParquetOnGCS(parquetData[]byte, bucketName string, objectName string) error {
	ctx := context.Background()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %w", err)
	}

	defer client.Close()
	log.Printf("Data size for upload: %d bytes", len(parquetData))

	bucket := client.Bucket(bucketName)
	object := bucket.Object(objectName)
	w := object.NewWriter(ctx)

	if _, err := w.Write(parquetData); err != nil {
		return fmt.Errorf("error writing to GCS: %w", err)
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("error closing GCS writer: %w", err)
	}

	log.Printf("Parquet file saved in GCS: gs://%s/%s", bucketName, objectName)
	return nil
}