package servermodels

import "encoding/json"

type Error struct {
	ErrorMessage string `json:"error_message"`
	TraceID      string `json:"trace_id"`
	StatusCode   int    `json:"status_code"`
}

func (err Error) Marshal() []byte {
	b, marsahlErr := json.Marshal(err)
	if marsahlErr != nil {
		panic(marsahlErr)
	}
	return b
}
