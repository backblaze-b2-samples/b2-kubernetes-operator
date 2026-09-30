/*
Copyright 2026 Backblaze, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Command b2fake serves the in-memory fake B2 API for end-to-end tests. It
// is not a B2 emulator and must never be used for anything else.
package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"github.com/backblaze-b2-samples/b2-operator/internal/b2/b2fake"
)

func main() {
	addr := flag.String("listen", ":8080", "Address to listen on.")
	advertise := flag.String("advertise-url", "http://b2fake:8080", "URL returned to clients as apiUrl.")
	keyID := flag.String("master-key-id", "e2eaccount01", "Master application key ID (also the account ID).")
	key := flag.String("master-key", "e2e-master-key", "Master application key.")
	flag.Parse()

	s := b2fake.NewUnstarted()
	s.SetMasterKey(*keyID, *key)
	s.SetURL(*advertise)
	srv := &http.Server{Addr: *addr, Handler: s, ReadHeaderTimeout: 10 * time.Second}
	log.Printf("fake B2 listening on %s (advertising %s)", *addr, *advertise)
	log.Fatal(srv.ListenAndServe())
}
