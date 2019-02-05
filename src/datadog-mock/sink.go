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
	"net"
	"os"
	"strconv"
)

type sink struct {
	port int
	done *chan bool
	inputStreamConn *net.UDPConn
	outputFunc func([]byte)
}

type Sink interface {
	Port() int
	Run()
	Close()
}

// BufferSize is upper boundary of UDP receive window
const BufferSize int = 1024

// NewSink creates new UDP sink with channel for complete events
func NewSink(port int, outputFunc func([]byte)) Sink {
	done := make(chan bool)
	return &sink{done: &done, port: port, outputFunc: outputFunc}
}

func (s *sink) Port() int {
	return s.port
}

// Run starts UDP sink with UPD stream source
func (s *sink) Run() {
	stream, err := net.ResolveUDPAddr("udp", ":" + strconv.Itoa(s.port))
	inputStreamConn, err := net.ListenUDP("udp", stream)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	s.inputStreamConn = inputStreamConn

	buf := make([]byte, BufferSize)
	for {
		n, _, err := s.inputStreamConn.ReadFromUDP(buf)
		if err != nil {
			fmt.Println(err)
			continue
		}

		go s.outputFunc(buf[0:n])

		select {
		case _, ok := <- *s.done:
			if !ok {
				fmt.Println("close")
				break
			}
		default:
		}
	}
}

func (s *sink) Close() {
	s.inputStreamConn.Close()
	close(*s.done)
}
