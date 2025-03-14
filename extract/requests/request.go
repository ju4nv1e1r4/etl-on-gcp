package requests

import (
	"extract/constants"
	"fmt"
	"io"
	"log"
	"net/http"

	"go.uber.org/zap"
)

func ExtractMoviesByTitle(keyWord string) []byte {
	aPIKey, err := constants.LoadEnvVars("RAPID_API_KEY")
	if err != nil {
		constants.Logger.Error("Erro ao carregar a chave da API da Fonte de dados",
			zap.Error(err),
		)
	}

	url := fmt.Sprintf("https://streaming-availability.p.rapidapi.com/shows/search/title?country=us&title=%s&series_granularity=show&show_type=movie&output_language=en", keyWord)
	req, _ := http.NewRequest("GET", url, nil)

	req.Header.Add("x-rapidapi-key", aPIKey)
	req.Header.Add("x-rapidapi-host", "streaming-availability.p.rapidapi.com")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	return body
}