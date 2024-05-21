package main

import (
	"context"
	"log"

	"github.com/apache/arrow/go/arrow/flight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/encoding/gzip"

)

func main() {
	// not sure what this does
	var creds credentials.TransportCredentials = insecure.NewCredentials()

	client, err := flight.NewClientWithMiddleware(
		"http://localhost:8002",
		nil,
		nil,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024 * 1024 * 1024)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(1024 * 1024 * 1024)),
	)
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	// Two WLM settings can be provided upon initial authentication with the dremio
	// server flight endpoint:
	//  - routing-tag
	//  - routing-queue
	ctx := metadata.NewOutgoingContext(context.TODO(),
		metadata.Pairs("routing-tag", "test", "routing-queue", "Queries"))

	if ctx, err = client.AuthenticateBasicToken(ctx, "admin", "admin"); err != nil {
		log.Fatal(err)
	}

	log.Println("[INFO] Authentication was successful.")

	json := `{
		"query": "select * from teststream",
		"startTime": "10days",
		"endTime": "now",
	}`

	tix := flight.Ticket{
		Ticket: []byte(json),
	}

	// retrieve the result set as a stream of Arrow record batches.
	stream, err := client.DoGet(ctx, &tix)
	if err != nil {
		log.Fatal(err)
	}

	rdr, err := flight.NewRecordReader(stream)
	if err != nil {
		log.Fatal(err)
	}
	defer rdr.Release()

	log.Println("[INFO] Reading query results.")
	for rdr.Next() {
		rec := rdr.Record()
		defer rec.Release()
		log.Println(rec)
	}
}
