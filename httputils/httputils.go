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

package httputils

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/netclave/common/cryptoutils"
	"github.com/netclave/common/jsonutils"
)

func RequestToMap(request *jsonutils.Request) map[string]interface{} {
	result := map[string]interface{}{}
	result["id"] = request.ID
	result["idSignature"] = request.IDSignature
	result["key"] = request.Key
	result["nonceID"] = request.NonceID
	result["nonceResponse"] = request.NonceResponse
	result["publicKey"] = request.PublicKey
	result["response"] = request.Response
	result["signature"] = request.Signature

	return result
}

func MapToRequest(data map[string]interface{}) *jsonutils.Request {
	request := &jsonutils.Request{}

	request.ID = data["id"].(string)
	request.IDSignature = data["idSignature"].(string)
	request.Key = data["key"].(string)
	request.NonceID = data["nonceID"].(string)
	request.NonceResponse = data["nonceResponse"].(string)
	request.PublicKey = data["publicKey"].(string)
	request.Response = data["response"].(string)
	request.Signature = data["signature"].(string)

	return request
}

func MakePostRequest(url string, request *jsonutils.Request, decrypt bool, privateKey string, cryptoStorage *cryptoutils.CryptoStorage) (string, string, *jsonutils.Request, error) {
	message := RequestToMap(request)
	bytesRepresentation, err := json.Marshal(message)
	if err != nil {
		log.Println("Can not marshal: " + err.Error())
		return "", "", nil, err
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(bytesRepresentation))
	if err != nil {
		log.Println("Can not post request: " + err.Error())
		return "", "", nil, err
	}

	response, err := jsonutils.ParseResponse(resp)
	if err != nil {
		log.Println("Can not parse response: " + err.Error())
		return "", "", nil, err
	}

	if response.Code == "200" {
		requestParsed := MapToRequest(response.Data.(map[string]interface{}))

		if decrypt == true {
			responseText, id, err := jsonutils.VerifyAndDecrypt(requestParsed, privateKey, cryptoStorage)

			if err != nil {
				log.Println("Can not verify and decrypt: " + err.Error())
				return "", "", nil, err
			}

			return responseText, id, requestParsed, nil
		}

		responseText, id, err := jsonutils.VerifyAndDecrypt(requestParsed, "", cryptoStorage)
		if err != nil {
			log.Println("Can not verify: " + err.Error())
			return "", "", nil, err
		}

		return responseText, id, requestParsed, nil
	}

	log.Println("Response status is: " + response.Status)

	return "", "", nil, errors.New(response.Status)
}

func RemoteGetPublicKey(url string, privateKey string, cryptoStorage *cryptoutils.CryptoStorage) (string, string, error) {
	fullURL := url + "/getPublicKey"
	request := &jsonutils.Request{}
	_, _, request, err := MakePostRequest(fullURL, request, false, privateKey, cryptoStorage)
	if err != nil {
		return "", "", err
	}

	return request.PublicKey, request.ID, nil
}
