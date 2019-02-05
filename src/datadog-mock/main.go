// Copyright (c) 2017-2018, Jan Cajthaml <jan.cajthaml@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"github.com/vmihailenco/msgpack"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
)

func main(){
	os.Exit(realMain())
}

func realMain() int {
	// FIXME arg handling
	udpPorts := os.Args[1:2]
	sinks, err := start(udpPorts)
	if err != nil {
		fmt.Println(err)
		return 1
	}
	for i, _ := range sinks {
		defer sinks[i].Close()
	}

	// FIXME arg handling
	httpPorts := os.Args[2:3]
	httpErr := httpStart(httpPorts)
	if httpErr != nil {
		fmt.Println(err)
		return 1
	}

	// FIXME signal handling
	c := make(chan []bool)
	<- c
	return 0
}

func start(ports []string) ([]Sink, error) {
	var sinks []Sink

	// TODO customize function
	f := func(event []byte) {
		fmt.Println(string(event))
	}
	for _, portStr := range ports {
		port, err := strconv.Atoi(portStr)
		if err != nil {
			for i, _ := range sinks {
				sinks[i].Close()
			}
			return []Sink{}, err
		}
		sink := NewSink(port, f)
		sinks = append(sinks, sink)
	}
	for i, _ := range sinks {
		fmt.Println("Starting DataDog Mock Server: " + strconv.Itoa(sinks[i].Port()))
		go sinks[i].Run()
	}
	return sinks, nil
}

// yattsuke
func httpStart(ports []string) error {

	// TODO customize function
	http.HandleFunc("/v0.3/traces", func(w http.ResponseWriter, r *http.Request){
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			fmt.Println(err)
			return
		}
		var bodyMsg interface{}
		parseErr := msgpack.Unmarshal(body, &bodyMsg)
		if parseErr != nil {
			fmt.Println(parseErr)
			return
		}
		fmt.Println(bodyMsg)
	})
	for _, port := range ports {
		go func(port string) {
			fmt.Println("Starting DataDog Mock Server: " + port)
			err := http.ListenAndServe(":"+port, nil)
			if err != nil {
				panic(err)
			}
		}(port)
	}
	return nil
}
