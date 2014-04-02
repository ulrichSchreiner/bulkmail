package main

import (
	"flag"
	"github.com/ulrichSchreiner/bulkmail/pkg/sink"
	"log"
	"net/smtp"
	"sync"
)

var forwardserver = flag.String("f", "", "forward mails to this SMTP server")
var redisAddress = flag.String("redis", "localhost:6379", "redis server:port")
var numforwarders = flag.Int("n", 10, "number of parallel forwarders")

func forwardMessages(spool sink.Datasink) {
	for {
		mail, err := spool.PullMail()
		if err == nil {
			for _, rcpt := range mail.Recipients {
				host := *forwardserver
				/*
					mx, err := net.LookupMX(rcpt.Host)
					if err != nil {
						log.Printf("Cannot find MX entries for host '%s': %s\n", rcpt.Host, err)
						continue
					}
					client, err := smtp.Dial(mx[0].Host + ":25")
				*/
				client, err := smtp.Dial(host)
				if err != nil {
					log.Printf("cannot contact mailserver '%s': %s\n", host, err)
					continue
				}
				client.Mail(mail.From)
				client.Rcpt(rcpt.Recipient)
				wc, err := client.Data()
				if err != nil {
					log.Printf("Error when retrieving smtp-client data stream: %s\n", err)
					client.Close()
					continue
				}
				if _, err = wc.Write(mail.Content); err != nil {
					log.Printf("error writing mail content to output: %s\n", err)
				} else {
					log.Printf("delivered mail to %s\n", rcpt.Recipient)
				}
				wc.Close()
				client.Close()
			}
		} else {
			log.Printf("Error when pulling mail from spool: %s\n", err)
		}
	}

}

func main() {
	flag.Parse()

	storer := sink.NewStorer(*redisAddress)
	spool := sink.NewDatasink(storer)
	var wg sync.WaitGroup
	for i := 0; i < *numforwarders; i++ {
		wg.Add(1)
		go func() {
			forwardMessages(spool)
			wg.Done()
		}()
	}
	wg.Wait()
}
