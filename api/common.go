package api

type JsonStringValue struct {
	Value string `json:"value"`
}

type JsonFloat64Value struct {
	Value float64 `json:"value"`
}

type JsonRefData struct {
	ID                 int    `json:"id" db:"id"`
	Symbol             string `json:"symbol" db:"symbol"`
	SymbolAlphaVantage string `json:"symbolAlphaVantage" db:"symbol_alpha_vantage"`
}

type JsonMarketData struct {
	ID        int     `json:"id" db:"id"`
	RefDataID int     `json:"refDataId" db:"ref_data_id"`
	Last      float64 `json:"last" db:"last"`
}
