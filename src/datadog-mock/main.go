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
	"encoding/json"
	"github.com/vmihailenco/msgpack"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
)

func init() {
	sinkLogger = log.New(os.Stdout, "", log.Lmicroseconds)
	sinkLogger = log.New(os.Stderr, "", log.Lmicroseconds)
}

var mainLogger = log.New(os.Stdout, "", log.Lmicroseconds)
var mainErrLogger = log.New(os.Stdout, "", log.Lmicroseconds)

func main(){
	os.Exit(realMain())
}

var pattern = regexp.MustCompile(`(?P<name>.*?):(?P<value>.*?)\|(?P<type>[a-z])(\|(?P<sample_rate>\d+(\.\d+)?))?(?:\|#(?P<tags>.*))?`)

func defaultDDStatsFunc(event DDEvent){
	bodyJsonByte, err := json.Marshal(event)
	if err != nil {
		mainLogger.Println(err)
		return
	}
	mainLogger.Println(string(bodyJsonByte))
}

func defaultDDFunc(event DDEvent){
	bodyJsonByte, err := json.Marshal(event)
	if err != nil {
		mainLogger.Println(err)
		return
	}
	mainLogger.Println(string(bodyJsonByte))
}

func realMain() int {
	// FIXME arg handling
	udpPorts := os.Args[1:2]
	outFunc := defaultDDStatsFunc
	sinks, err := start(udpPorts, outFunc)
	if err != nil {
		return 1
	}
	for i, _ := range sinks {
		defer sinks[i].Close()
	}

	// FIXME arg handling
	httpPorts := os.Args[2:3]
	httpErr := httpStart(httpPorts, defaultDDFunc)
	if httpErr != nil {
		mainErrLogger.Println(err)
		return 1
	}

	// FIXME signal handling
	c := make(chan []bool)
	<- c
	return 0
}

func start(ports []string, outFunc func(DDStatsEvent)) ([]Sink, error) {
	var sinks []Sink

	for _, portStr := range ports {
		port, err := strconv.Atoi(portStr)
		if err != nil {
			for i, _ := range sinks {
				sinks[i].Close()
			}
			return []Sink{}, err
		}
		sink := NewSink(port, outFunc)
		sinks = append(sinks, sink)
	}
	for i, _ := range sinks {
		mainLogger.Println("Starting DataDog Mock Server: " + strconv.Itoa(sinks[i].Port()))
		go sinks[i].Run()
	}
	return sinks, nil
}

// yattsuke
func httpStart(ports []string, outFunc func(event DDEvent)) error {

	// TODO customize function
	http.HandleFunc("/v0.3/traces", func(writer http.ResponseWriter, request *http.Request) {
		bodyByte, err := ioutil.ReadAll(request.Body)
		var event DDEvent
		parseErr := msgpack.Unmarshal(bodyByte, &event)
		if parseErr != nil {
			mainLogger.Println(parseErr)
			return
		}
		if err != nil {
			mainLogger.Println(err)
			return
		}
		outFunc(event)
	})
	for _, port := range ports {
		go func(port string) {
			mainLogger.Println("Starting DataDog Mock Server: " + port)
			err := http.ListenAndServe(":"+port, nil)
			if err != nil {
				panic(err)
			}
		}(port)
	}
	return nil
}
