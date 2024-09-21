package main

import (
	"fmt"
	"log"
	"net/http"
)

func main() {
	fs := http.FileServer(http.Dir("./static"))
	http.Handle("/", fs)

	port := ":9000"

	s := &http.Server{
		Addr: port,
	}
	// s.SetKeepAlivesEnabled(false) (from first exercise)
	fmt.Printf("Serving at port %s\n", port)

	log.Fatal(s.ListenAndServe())
}
