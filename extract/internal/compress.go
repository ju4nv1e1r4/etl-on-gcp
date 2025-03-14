package internal

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

func JSONtoParquet(jsonData []byte) ([]byte, error) {
    var showDetails []ShowDetails
    err := json.Unmarshal(jsonData, &showDetails)
    if err != nil {
        var singleShow ShowDetails
        err = json.Unmarshal(jsonData, &singleShow)
        if err != nil {
            return nil, fmt.Errorf("erro decode json: %w", err)
        }
        showDetails = []ShowDetails{singleShow}
    }
    
    var parquetData []ShowDetailsParquet
    for _, show := range showDetails {
        genresJSON, _ := json.Marshal(show.Genres)
        directorsJSON, _ := json.Marshal(show.Directors)
        castJSON, _ := json.Marshal(show.Cast)
        imageSetJSON, _ := json.Marshal(show.ImageSet)
        streamingOptionsJSON, _ := json.Marshal(show.StreamingOptions)
        
        parquetShow := ShowDetailsParquet{
            ItemType:         show.ItemType,
            ShowType:         show.ShowType,
            ID:               show.ID,
            ImdbID:           show.ImdbID,
            TmdbID:           show.TmdbID,
            Title:            show.Title,
            Overview:         show.Overview,
            ReleaseYear:      show.ReleaseYear,
            OriginalTitle:    show.OriginalTitle,
            Genres:           string(genresJSON),
            Directors:        string(directorsJSON),
            Cast:             string(castJSON),
            Rating:           show.Rating,
            Runtime:          show.Runtime,
            ImageSet:         string(imageSetJSON),
            StreamingOptions: string(streamingOptionsJSON),
        }
        parquetData = append(parquetData, parquetShow)
    }
    
    buf := new(bytes.Buffer)
    
    pw, err := writer.NewParquetWriterFromWriter(
        buf,
        new(ShowDetailsParquet),
        4,
    )
    if err != nil {
        return nil, fmt.Errorf("erro ao criar Parquet Writer: %w", err)
    }
    pw.RowGroupSize = 128 * 1024 * 1024 // 128M
    pw.CompressionType = parquet.CompressionCodec_SNAPPY
    
    for _, rec := range parquetData {
        if err := pw.Write(rec); err != nil {
            return nil, fmt.Errorf("erro ao escrever Parquet: %w", err)
        }
    }
    
    if err := pw.WriteStop(); err != nil {
        return nil, fmt.Errorf("erro ao finalizar a escrita do Parquet: %w", err)
    }
    
    return buf.Bytes(), nil
}