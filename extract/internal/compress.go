package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
    "strings"

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
        // Processa os gêneros para formato string separado por vírgulas
        var genreNames []string
        for _, genre := range show.Genres {
            genreNames = append(genreNames, genre.Name)
        }
        genresStr := strings.Join(genreNames, ",")
        
        directorsJSON, _ := json.Marshal(show.Directors)
        castJSON, _ := json.Marshal(show.Cast)

        // Extrai informações de streaming se disponíveis
        streamingServiceName := ""
        streamingType := ""
        streamingQuality := ""
        streamingLink := ""
        var availableSince int64 = 0
        var subtitlesStr string = ""

        if len(show.StreamingOptions.US) > 0 {
            // Pega o primeiro serviço de streaming disponível nos EUA
            streamOption := show.StreamingOptions.US[0]
            streamingServiceName = streamOption.Service.Name
            streamingType = streamOption.Type
            streamingQuality = streamOption.Quality
            streamingLink = streamOption.Link
            availableSince = streamOption.AvailableSince
            
            // Processa legendas para formato mais amigável para coluna
            var subtitleLanguages []string
            for _, subtitle := range streamOption.Subtitles {
                subtitleLanguages = append(subtitleLanguages, subtitle.Locale.Language)
            }
            subtitlesStr = strings.Join(subtitleLanguages, ",")
        }

        parquetShow := ShowDetailsParquet{
            ItemType:      show.ItemType,
            ShowType:      show.ShowType,
            ID:            show.ID,
            ImdbID:        show.ImdbID,
            TmdbID:        show.TmdbID,
            Title:         show.Title,
            Overview:      show.Overview,
            ReleaseYear:   show.ReleaseYear,
            OriginalTitle: show.OriginalTitle,
            Genres:        genresStr,
            Directors:     string(directorsJSON),
            Cast:          string(castJSON),
            Rating:        show.Rating,
            Runtime:       show.Runtime,

            // Extrai URLs específicas de imagens
            PosterW240:   show.ImageSet.VerticalPoster.W240,
            PosterW480:   show.ImageSet.VerticalPoster.W480,
            BackdropW720: show.ImageSet.HorizontalBackdrop.W720,

            // Informações do serviço de streaming
            StreamingServiceName: streamingServiceName,
            StreamingType:        streamingType,
            StreamingQuality:     streamingQuality,
            StreamingLink:        streamingLink,
            AvailableSince:       availableSince,
            Subtitles:            subtitlesStr,
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