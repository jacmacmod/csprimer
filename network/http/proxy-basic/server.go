package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	// todo create server that forwards messages
	fs := http.FileServer(http.Dir("./static"))
	http.Handle("/", fs)

	port := ":9000"
	fmt.Printf("Serving at port %s\n", port)
	log.Fatal(http.ListenAndServe(port, nil))
}
