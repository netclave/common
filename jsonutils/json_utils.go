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

package jsonutils

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/netclave/common/utils"

	"github.com/netclave/common/cryptoutils"
)

type Request struct {
	Response      string `json:"response"`
	Signature     string `json:"signature"`
	NonceResponse string `json:"nonceResponse"`
	ID            string `json:"id"`
	IDSignature   string `json:"idSignature"`
	NonceID       string `json:"nonceID"`
	Key           string `json:"key"`
	PublicKey     string `json:"publicKey"`
}

func (e *Request) InputValidation() error {
	/*if len(e.ConfirmationCode) == 0 {
		return ErrMissingField("confirmationCode")
	}*/

	return nil
}

func ReadAll(r io.ReadCloser) (string, error) {
	var bodyBytes []byte
	bodyBytes, err := ioutil.ReadAll(r)

	if err != nil {
		return "", err
	}

	bodyString := string(bodyBytes)

	return bodyString, nil
}

func ParseRequest(r *http.Request) (*Request, error) {
	request := &Request{}

	bodyString, err := ReadAll(r.Body)

	if err != nil {
		return nil, err
	}

	err = ParseForm(bodyString, request)

	if err != nil {
		return nil, err
	}

	return request, nil
}

func ParseResponse(resp *http.Response) (*Response, error) {
	response := &Response{}

	bodyString, err := ReadAll(resp.Body)

	if err != nil {
		return nil, err
	}

	err = ParseForm(bodyString, response)

	if err != nil {
		log.Println("Can not parse: " + bodyString)
		return nil, err
	}

	return response, nil
}

func SignAndEncryptResponse(data interface{}, id string,
	senderPrivateKeyPem string, senderPublicKeyPem string,
	recipientPublicKeyPem string, putSenderPublicKey bool) (*Request, error) {
	message, err := json.Marshal(data)

	if err != nil {
		return nil, err
	}

	senderPrivateKey, err := cryptoutils.ParseRSAPrivateKey(senderPrivateKeyPem)

	if err != nil {
		return nil, err
	}

	signature, err := cryptoutils.Sign(string(message), senderPrivateKey)

	if err != nil {
		return nil, err
	}

	idSignature, err := cryptoutils.Sign(id, senderPrivateKey)

	if err != nil {
		return nil, err
	}

	response := string(message)
	responseID := id
	aesKeyEncrypted := ""
	nonceResponse := ""
	nonceID := ""
	encryptedResponseNonce := ""
	encryptedIDNonce := ""

	if recipientPublicKeyPem != "" {
		recipientPublicKey, err := cryptoutils.ParseRSAPublicKey(recipientPublicKeyPem)

		if err != nil {
			return nil, err
		}

		aesKey, err := cryptoutils.GenerateAesKey()

		if err != nil {
			fmt.Println(err.Error())
			return nil, err
		}

		aesKeyEncrypted, err = cryptoutils.EncryptData(aesKey, recipientPublicKey)

		if err != nil {
			fmt.Println(err.Error())
			return nil, err
		}

		response, nonceResponse, err = cryptoutils.EncryptAES(response, aesKey)

		if err != nil {
			return nil, err
		}

		encryptedResponseNonce, err = cryptoutils.EncryptData(nonceResponse, recipientPublicKey)

		if err != nil {
			fmt.Println(err.Error())
			return nil, err
		}

		responseID, nonceID, err = cryptoutils.EncryptAES(id, aesKey)

		if err != nil {
			return nil, err
		}

		encryptedIDNonce, err = cryptoutils.EncryptData(nonceID, recipientPublicKey)

		if err != nil {
			fmt.Println(err.Error())
			return nil, err
		}
	}

	responseWithSignature := &Request{
		Response:      string(response),
		Signature:     signature,
		NonceResponse: encryptedResponseNonce,
		ID:            responseID,
		IDSignature:   idSignature,
		NonceID:       encryptedIDNonce,
		Key:           aesKeyEncrypted,
	}

	if putSenderPublicKey == true {
		responseWithSignature.PublicKey = senderPublicKeyPem
	}

	return responseWithSignature, nil
}

func VerifyAndDecrypt(request *Request, recipientPrivateKeyPem string, cryptoStorage *cryptoutils.CryptoStorage) (string, string, error) {
	response := request.Response
	id := request.ID

	if recipientPrivateKeyPem != "" {
		privateKey, err := cryptoutils.ParseRSAPrivateKey(recipientPrivateKeyPem)

		if err != nil {
			return "", "", err
		}

		aesKey, err := cryptoutils.DecryptData(request.Key, privateKey)

		if err != nil {
			return "", "", err
		}

		nonceResponse, err := cryptoutils.DecryptData(request.NonceResponse, privateKey)

		if err != nil {
			return "", "", err
		}

		nonceID, err := cryptoutils.DecryptData(request.NonceID, privateKey)

		if err != nil {
			return "", "", err
		}

		response, err = cryptoutils.DecryptAes(request.Response, nonceResponse, aesKey)

		if err != nil {
			return "", "", err
		}

		id, err = cryptoutils.DecryptAes(request.ID, nonceID, aesKey)

		if err != nil {
			return "", "", err
		}
	}

	senderPublicKeyPem := request.PublicKey
	err := errors.New("")

	if senderPublicKeyPem == "" {
		senderPublicKeyPem, err = cryptoStorage.RetrievePublicKey(id)
		if err != nil || senderPublicKeyPem == "" {
			senderPublicKeyPem, err = cryptoStorage.RetrieveTempPublicKey(id)
			if err != nil || senderPublicKeyPem == "" {
				return "", "", err
			}
		}
	}

	senderPublicKey, err := cryptoutils.ParseRSAPublicKey(senderPublicKeyPem)

	if err != nil {
		return "", "", err
	}

	verifyID, err := cryptoutils.Verify(id, request.IDSignature, senderPublicKey)

	if err != nil {
		return "", "", err
	}

	if verifyID == false {
		return "", "", errors.New("Can not verify id signature")
	}

	verifyResponse, err := cryptoutils.Verify(response, request.Signature, senderPublicKey)

	if err != nil {
		return "", "", err
	}

	if verifyResponse == false {
		return "", "", errors.New("Can not verify response signature")
	}

	return response, id, nil
}

type Response struct {
	Status string      `json:"status"`
	Code   string      `json:"code"`
	Data   interface{} `json:"data"`
}

func (e *Response) InputValidation() error {
	/*if len(e.ConfirmationCode) == 0 {
		return ErrMissingField("confirmationCode")
	}*/

	return nil
}

func EncodeResponse(code string, status string, data interface{}, w http.ResponseWriter, fail2banData *utils.Fail2BanData) error {
	if code != "200" {
		event, err := utils.CreateSimpleEvent(fail2banData.RemoteAddress)

		if err != nil {
			log.Println(err.Error())
			return err
		}

		err = utils.StoreBannedIP(fail2banData.DataStorage, event, fail2banData.TTL)

		if err != nil {
			log.Println(err.Error())
			return err
		}

		log.Printf("encode response code: %s, status: %s, data: %+v", code, status, data)
	}

	w.Header().Set("Content-Type", "application/json")

	response := &Response{
		Status: status,
		Code:   code,
		Data:   data,
	}

	return json.NewEncoder(w).Encode(response)
}

type HTTPForm interface {
	InputValidation() error
}

func ParseForm(body string, form HTTPForm) error {
	r := strings.NewReader(body)
	err := json.NewDecoder(r).Decode(form)
	if err != nil {
		log.Printf("Error: %v", err)
		return errors.New("Cannot decode data: " + err.Error())
	}

	err = form.InputValidation()

	if err != nil {
		log.Printf("Error: %v", err)
		return errors.New("There was an error with the input fields: " + err.Error())
	}

	return nil
}
