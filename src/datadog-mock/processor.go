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
	"os"
	"regexp"
	"strings"

	"github.com/ScottMansfield/nanolog"
)

var h nanolog.Handle

func init() {
	nanolog.SetWriter(os.Stdout)
	h = nanolog.AddLogger("%s")
}

var pattern = regexp.MustCompile(`(?P<name>.*?):(?P<value>.*?)\|(?P<type>[a-z])(?:\|#(?P<tags>.*))?`)

func processEvent(event []byte){
	parsed := string(event)
	fmt.Println(parsed)
}

func _processEvent(event []byte) {
	parsed := string(event)
	match := pattern.FindStringSubmatch(parsed)

	if len(match) < 3 {
		return
	}

	nanolog.Log(h, strings.TrimSuffix(parsed, "\n")+"\r\n")
	nanolog.Flush()
}
