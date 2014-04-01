package sink

import (
	"encoding/json"
)

type EMail struct {
	RecipientMail []string `json:"recipient"`
	RecipientHost []string `json:"recipientHost"`
	Content       []byte   `json:"content"`
}

type Datasink interface {
	PutMail(e *EMail) error
}

type redissink struct {
	storer Storer
}

func (s *redissink) PutMail(e *EMail) error {
	c := s.storer.GetConnection()
	defer c.Close()

	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := c.Do("RPUSH", "spool", b); err != nil {
		return err
	}
	return nil
}

func NewDatasink(s Storer) Datasink {
	return &redissink{s}
}
