package main

import (
	"bytes"
	"flag"
	"github.com/ulrichSchreiner/bulkmail/pkg/sink"
	"log"

	"github.com/ulrichSchreiner/go-smtpd/smtpd"
)

var spool sink.Datasink
var address = flag.String("address", "localhost:2500", "address and port")
var redisAddress = flag.String("redis", "localhost:6379", "redis server:port")

type env struct {
	rcpts []smtpd.MailAddress
	lines bytes.Buffer
}

func (e *env) AddRecipient(rcpt smtpd.MailAddress) error {
	e.rcpts = append(e.rcpts, rcpt)
	return nil
}

func (e *env) BeginData() error {
	if len(e.rcpts) == 0 {
		return smtpd.SMTPError("554 5.5.1 Error: no valid recipients")
	}
	return nil
}

func (e *env) Write(line []byte) error {
	e.lines.Write(line)
	e.lines.WriteByte('\n')
	return nil
}

func (e *env) Close() error {
	var email sink.EMail
	for _, ad := range e.rcpts {
		email.RecipientHost = append(email.RecipientHost, ad.Hostname())
		email.RecipientMail = append(email.RecipientMail, ad.Email())
	}
	email.Content = e.lines.Bytes()
	spool.PutMail(&email)
	return nil
}

func onNewMail(c smtpd.Connection, from smtpd.MailAddress) (smtpd.Envelope, error) {
	log.Printf("new mail from %q", from)
	return &env{}, nil
}

func main() {
	s := &smtpd.Server{
		Addr:      *address,
		OnNewMail: onNewMail,
	}
	storer := sink.NewStorer(*redisAddress)
	spool = sink.NewDatasink(storer)

	err := s.ListenAndServe()
	if err != nil {
		log.Fatalf("ListenAndServe: %v", err)
	}
}
