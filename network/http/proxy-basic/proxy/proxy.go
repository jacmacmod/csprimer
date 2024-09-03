package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
)

func main() {
	handler := func(w http.ResponseWriter, r *http.Request) {
		uri := fmt.Sprintf("http://localhost:9000%s", r.URL.Path)
		r, err := http.NewRequest("GET", uri, nil)
		if err != nil {
			log.Fatal(err)
		}

		resp, err := http.DefaultClient.Do(r)
		if err != nil {
			log.Fatalf("Error in request %v\n", err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
		}

		w.WriteHeader(200)
		w.Write(body)
	}

	http.HandleFunc("/", handler)
	fmt.Println("Listening on port 8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// fmt.Printf("body: %s\n, headers,", body)
// for key, values := range resp.Header {
// 	for _, value := range values {
// 		fmt.Printf("key %s value %s\n", key, value)
// 		w.Header().Add(key, value)
// 	}
// }
