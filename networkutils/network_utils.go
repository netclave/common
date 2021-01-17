/*
 * Copyright @ 2020 - present Blackvisor Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package networkutils

import (
	"net/http"
	"strings"
)

func EnableCors(w *http.ResponseWriter) {
	(*w).Header().Set("Access-Control-Allow-Origin", "*")
}

func ParseIP(remoteAddress string) string {
	ipPort := remoteAddress

	//log.Printf(ipPort)

	ipPortSplit := strings.Split(ipPort, ":")

	ip := ""

	for i := 0; i < len(ipPortSplit)-1; i++ {
		ip = ip + ipPortSplit[i]

		if i < len(ipPortSplit)-2 {
			ip = ip + ":"
		}
	}

	return ip
}

func GetRemoteAddress(req *http.Request) string {
	ipAddress := req.RemoteAddr
	fwdAddress := req.Header.Get("X-Forwarded-For") // capitalisation doesn't matter
	if fwdAddress != "" {
		// Got X-Forwarded-For
		ipAddress = fwdAddress // If it's a single IP, then awesome!

		// If we got an array... grab the first IP
		ips := strings.Split(fwdAddress, ", ")
		if len(ips) > 1 {
			ipAddress = ips[0]
		}
	}

	return ipAddress
}
