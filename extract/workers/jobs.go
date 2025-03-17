package workers

import (
	"extract/constants"
	"extract/internal"
	"extract/requests"
	"log"
	"fmt"
)

// Run performs all extraction, compression and upload work in the cloud.
func Run()  {
	constants.InitLogger()
	
	showTitle := "Indiana Jones"
	jsonData := requests.ExtractMoviesByTitle(showTitle)

	parquetData, err := internal.JSONtoParquet(jsonData)
	if err != nil {
		log.Fatalf("Error converting JSON to Parquet: %v", err)
	}

	bucketName := "streaming-data-for-ml"
	objectName := fmt.Sprintf("data/%s.parquet", showTitle)
	
	if err := UploadParquetOnGCS(parquetData, bucketName, objectName); err != nil {
		log.Fatalf("Error uploading to GCS: %v", err)
	}
}