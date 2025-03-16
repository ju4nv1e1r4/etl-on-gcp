package requests

import (
	"extract/constants"
	"fmt"
	"io"
	"log"
	"net/http"

	"go.uber.org/zap"
)

// ExtractMoviesByTitle performs data extraction from an API that contains movie data 
// and other information about these movies.
// This function takes a keyword and searches for movies related to this keyword, 
// then returns bytes of the files.
func ExtractMoviesByTitle(keyWord string) []byte {
	aPIKey, err := constants.LoadEnvVars("RAPID_API_KEY")
	if err != nil {
		constants.Logger.Error("Error loading Data Source API key",
			zap.Error(err),
		)
	}
	// This URL contains other configurable parameters.
	// If you want to know more about it, check out the API documentation: 
	// https://rapidapi.com/movie-of-the-night-movie-of-the-night-default/api/streaming-availability
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