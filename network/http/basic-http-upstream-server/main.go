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

	s := &http.Server{Addr: port}

	// s.SetKeepAlivesEnabled(false) (from first exercise)
	fmt.Printf("Serving at port %s\n", port)

	log.Fatal(s.ListenAndServe())

	// e := echo.New()
	// e.Logger.SetLevel(log.DEBUG)

	// e.Use(middleware.Logger())
	// e.GET("/", func(c echo.Context) error {
	// 	fmt.Println("done")
	// 	return c.String(http.StatusOK, "Hello, World!")
	// })
	// e.Static("/static", "static")
	// e.Logger.Fatal(e.Start(":9000"))
}
