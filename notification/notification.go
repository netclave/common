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

package notification

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/mail"
	"net/smtp"
	"net/url"
	"regexp"
	"strings"
)

func IsEmail(email string) bool {
	Re := regexp.MustCompile(`^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,4}$`)
	return Re.MatchString(email)
}

func SendSMS(to string, text string, twilioAccount string, twilioSecret string, twilioPhone string) (string, error) {
	accountSid := twilioAccount
	authToken := twilioSecret
	phone := twilioPhone

	urlStr := "https://api.twilio.com/2010-04-01/Accounts/" + accountSid + "/Messages.json"
	// Pack up the data for our message
	msgData := url.Values{}
	msgData.Set("To", to)
	msgData.Set("From", phone)
	msgData.Set("Body", text)
	msgDataReader := *strings.NewReader(msgData.Encode())

	// Create HTTP request client
	client := &http.Client{}
	req, _ := http.NewRequest("POST", urlStr, &msgDataReader)
	req.SetBasicAuth(accountSid, authToken)
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	// Make HTTP POST request and return message SID
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		var data map[string]interface{}
		decoder := json.NewDecoder(resp.Body)
		err := decoder.Decode(&data)
		if err != nil {
			return "", err
		}

		return resp.Status, nil
	}

	return "", errors.New(resp.Status)
}

func SendSMTPEmail(toEmail string, subject string, text string, supportEmail string, SMTPHost string, SMTPPort string, SMTPUsername string, SMTPPassword string) (string, error) {
	from := mail.Address{"", supportEmail}
	to := mail.Address{"", toEmail}
	subj := subject
	body := text

	// Setup headers
	headers := make(map[string]string)
	headers["From"] = from.String()
	headers["To"] = to.String()
	headers["Subject"] = subj

	// Setup message
	message := ""
	for k, v := range headers {
		message += fmt.Sprintf("%s: %s\r\n", k, v)
	}
	message += "\r\n" + body

	// Connect to the SMTP Server
	servername := SMTPHost + ":" + SMTPPort

	auth := smtp.PlainAuth("", SMTPUsername, SMTPPassword, SMTPHost)

	// TLS config
	tlsconfig := &tls.Config{
		InsecureSkipVerify: true,
		ServerName:         SMTPHost,
	}

	// Here is the key, you need to call tls.Dial instead of smtp.Dial
	// for smtp servers running on 465 that require an ssl connection
	// from the very beginning (no starttls)
	conn, err := tls.Dial("tcp", servername, tlsconfig)
	if err != nil {
		return "", err
	}

	c, err := smtp.NewClient(conn, SMTPHost)
	if err != nil {
		return "", err
	}

	// Auth
	if err = c.Auth(auth); err != nil {
		return "", err
	}

	// To && From
	if err = c.Mail(from.Address); err != nil {
		return "", err
	}

	if err = c.Rcpt(to.Address); err != nil {
		return "", err
	}

	// Data
	w, err := c.Data()
	if err != nil {
		return "", err
	}

	_, err = w.Write([]byte(message))
	if err != nil {
		return "", err
	}

	err = w.Close()
	if err != nil {
		return "", err
	}

	c.Quit()

	return "OK", nil
}
