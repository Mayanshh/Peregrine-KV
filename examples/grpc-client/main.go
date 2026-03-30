package main

import (
	"context"
	"flag"
	"log"
	"time"

	kvpb "github.com/Mayanshh/Peregrine-KV/internal/kvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "KV server address")
	op := flag.String("op", "put", "put|get|delete")
	key := flag.String("key", "k1", "key")
	val := flag.String("val", "v1", "value (for put)")
	flag.Parse()

	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := kvpb.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch *op {
	case "put":
		resp, err := client.Put(ctx, &kvpb.PutRequest{Key: []byte(*key), Value: []byte(*val)})
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("put ok=%v err=%q", resp.Ok, resp.Error)
	case "get":
		resp, err := client.Get(ctx, &kvpb.GetRequest{Key: []byte(*key)})
		if err != nil {
			log.Fatal(err)
		}
		if resp.Found {
			log.Printf("get found=true value=%q", string(resp.Value))
		} else {
			log.Printf("get found=false err=%q", resp.Error)
		}
	case "delete":
		resp, err := client.Delete(ctx, &kvpb.DeleteRequest{Key: []byte(*key)})
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("delete ok=%v err=%q", resp.Ok, resp.Error)
	default:
		log.Fatalf("unknown op=%s", *op)
	}
}

