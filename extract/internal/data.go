package internal

type ShowDetails struct {
	ItemType         string           `json:"itemType"`
	ShowType         string           `json:"showType"`
	ID               string           `json:"id"`
	ImdbID           string           `json:"imdbId"`
	TmdbID           string           `json:"tmdbId"`
	Title            string           `json:"title"`
	Overview         string           `json:"overview"`
	ReleaseYear      int              `json:"releaseYear"`
	OriginalTitle    string           `json:"originalTitle"`
	Genres           []Genre          `json:"genres"`
	Directors        []string         `json:"directors"`
	Cast             []string         `json:"cast"`
	Rating           int              `json:"rating"`
	Runtime          int              `json:"runtime"`
	ImageSet         ImageSet         `json:"imageSet"`
	StreamingOptions StreamingOptions `json:"streamingOptions"`
}

type Genre struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type ImageSet struct {
	VerticalPoster     PosterURLs `json:"verticalPoster"`
	HorizontalPoster   PosterURLs `json:"horizontalPoster"`
	VerticalBackdrop   PosterURLs `json:"verticalBackdrop"`
	HorizontalBackdrop PosterURLs `json:"horizontalBackdrop"`
}

type PosterURLs struct {
	W240  string `json:"w240"`
	W360  string `json:"w360"`
	W480  string `json:"w480"`
	W600  string `json:"w600"`
	W720  string `json:"w720"`
	W1080 string `json:"w1080,omitempty"`
	W1440 string `json:"w1440,omitempty"`
}

type StreamingOptions struct {
	US []StreamingOption `json:"us"`
}

type StreamingOption struct {
	Service        Service       `json:"service"`
	Type           string        `json:"type"`
	Link           string        `json:"link"`
	Quality        string        `json:"quality"`
	Audios         []interface{} `json:"audios"`
	Subtitles      []Subtitle    `json:"subtitles"`
	ExpiresSoon    bool          `json:"expiresSoon"`
	AvailableSince int64         `json:"availableSince"`
}

type Service struct {
	ID              string     `json:"id"`
	Name            string     `json:"name"`
	HomePage        string     `json:"homePage"`
	ThemeColorCode  string     `json:"themeColorCode"`
	ImageSet        ServiceImageSet `json:"imageSet"`
}

type ServiceImageSet struct {
	LightThemeImage string `json:"lightThemeImage"`
	DarkThemeImage  string `json:"darkThemeImage"`
	WhiteImage      string `json:"whiteImage"`
}

type Subtitle struct {
	ClosedCaptions bool   `json:"closedCaptions"`
	Locale         Locale `json:"locale"`
}

type Locale struct {
	Language string `json:"language"`
}

type ShowDetailsParquet struct {
	ItemType         string `parquet:"name=itemType, type=BYTE_ARRAY, convertedtype=UTF8"`
	ShowType         string `parquet:"name=showType, type=BYTE_ARRAY, convertedtype=UTF8"`
	ID               string `parquet:"name=id, type=BYTE_ARRAY, convertedtype=UTF8"`
	ImdbID           string `parquet:"name=imdbId, type=BYTE_ARRAY, convertedtype=UTF8"`
	TmdbID           string `parquet:"name=tmdbId, type=BYTE_ARRAY, convertedtype=UTF8"`
	Title            string `parquet:"name=title, type=BYTE_ARRAY, convertedtype=UTF8"`
	Overview         string `parquet:"name=overview, type=BYTE_ARRAY, convertedtype=UTF8"`
	ReleaseYear      int    `parquet:"name=releaseYear, type=INT32"`
	OriginalTitle    string `parquet:"name=originalTitle, type=BYTE_ARRAY, convertedtype=UTF8"`
	Genres           string `parquet:"name=genres, type=BYTE_ARRAY, convertedtype=UTF8"`
	Directors        string `parquet:"name=directors, type=BYTE_ARRAY, convertedtype=UTF8"`
	Cast             string `parquet:"name=cast, type=BYTE_ARRAY, convertedtype=UTF8"`
	Rating           int    `parquet:"name=rating, type=INT32"`
	Runtime          int    `parquet:"name=runtime, type=INT32"`
	ImageSet         string `parquet:"name=imageSet, type=BYTE_ARRAY, convertedtype=UTF8"`
	StreamingOptions string `parquet:"name=streamingOptions, type=BYTE_ARRAY, convertedtype=UTF8"`
}