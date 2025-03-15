package main

import (
	"extract/constants"
	"extract/internal"
	"extract/requests"
	"extract/workers"
	"log"
	"fmt"
)

func main()  {
	constants.InitLogger()
	
	showTitle := "Cars"
	jsonData := requests.ExtractMoviesByTitle(showTitle)

	parquetData, err := internal.JSONtoParquet(jsonData)
	if err != nil {
		log.Fatalf("Erro ao converter JSON para Parquet: %v", err)
	}

	bucketName := "streaming-data-for-ml"
	objectName := fmt.Sprintf("data/%s.parquet", showTitle)
	
	if err := workers.UploadParquetOnGCS(parquetData, bucketName, objectName); err != nil {
		log.Fatalf("Erro ao fazer upload para GCS: %v", err)
	}
}