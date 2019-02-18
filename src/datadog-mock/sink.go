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
	"log"
	"net"
	"regexp"
	"strconv"
)

type sink struct {
	port            int
	done            *chan bool
	inputStreamConn *net.UDPConn
	outputFunc      func(event DDStatsDEvent)
}

// Sink is UDPSink
type Sink interface {
	Port() int
	Run()
	Close()
}

// BufferSize is upper boundary of UDP receive window
const BufferSize int = 1024

var sinkLogger *log.Logger
var sinkErrLogger *log.Logger

// DDStatsDEvent is Datadog StatsD event
type DDStatsDEvent = interface{}

// DDEvent is Datadog message pack event
type DDEvent = interface{}

var pattern = regexp.MustCompile(`(?P<name>.*?):(?P<value>.*?)\|(?P<type>[a-z])(\|(?P<sample_rate>\d+(\.\d+)?))?(?:\|#(?P<tags>.*))?`)

func parseEvent(event []byte) (DDStatsDEvent, error) {
	parsed := string(event)
	match := pattern.FindStringSubmatch(parsed)
	if len(match) < 3 {
		return nil, fmt.Errorf("parse error: %s", event)
	}
	result := make(map[string]string)
	for i, name := range pattern.SubexpNames() {
		if i != 0 && name != "" {
			result[name] = match[i]
		}
	}
	return result, nil
}

// NewSink creates new UDP sink with channel for complete events
func NewSink(port int, outputFunc func(event DDStatsDEvent)) Sink {
	done := make(chan bool)
	return &sink{done: &done, port: port, outputFunc: outputFunc}
}

func (s *sink) Port() int {
	return s.port
}

// Run starts UDP sink with UPD stream source
func (s *sink) Run() {
	stream, err := net.ResolveUDPAddr("udp", ":"+strconv.Itoa(s.port))
	if err != nil {
		sinkErrLogger.Println(err)
		return
	}
	inputStreamConn, err := net.ListenUDP("udp", stream)

	if err != nil {
		sinkErrLogger.Println(err)
		return
	}
	s.inputStreamConn = inputStreamConn

	buf := make([]byte, BufferSize)
	for {
		n, _, err := s.inputStreamConn.ReadFromUDP(buf)
		if err != nil {
			sinkErrLogger.Println(err)
			continue
		}

		event, err := parseEvent(buf[0:n])
		if err != nil {
			sinkErrLogger.Println(err)
		}

		go s.outputFunc(event)

		select {
		case _, ok := <-*s.done:
			if !ok {
				sinkLogger.Printf("port %d close\n", s.port)
				break
			}
		default:
		}
	}
}

func (s *sink) Close() {
	if err := s.inputStreamConn.Close(); err != nil {
		sinkErrLogger.Println(err)
	}
	close(*s.done)
}
