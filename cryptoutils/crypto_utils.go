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

package cryptoutils

import (
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io"
)

func GenerateKeyPair() (*rsa.PrivateKey, error) {
	return rsa.GenerateKey(rand.Reader, 2048)
}

func EncodePublicKeyPEM(pair *rsa.PrivateKey) (string, error) {
	pub := pair.PublicKey
	der, err := x509.MarshalPKIXPublicKey(&pub)
	if err != nil {
		return "", err
	}

	blk := pem.Block{
		Type:    "PUBLIC KEY",
		Headers: nil,
		Bytes:   der,
	}
	pem := string(pem.EncodeToMemory(&blk))
	return pem, nil
}

func EncodePrivateKeyPEM(pair *rsa.PrivateKey) (string, error) {
	der, err := x509.MarshalPKCS8PrivateKey(pair)
	if err != nil {
		return "", err
	}
	// pem.Block
	// blk pem.Block
	blk := pem.Block{
		Type:    "RSA PRIVATE KEY",
		Headers: nil,
		Bytes:   der,
	}

	// Resultant private key in PEM format.
	// priv_pem string
	pem := string(pem.EncodeToMemory(&blk))

	return pem, nil
}

func ParseRSAPublicKey(data string) (*rsa.PublicKey, error) {
	pubKeyBlock, _ := pem.Decode([]byte(data))
	pubInterface, parseErr := x509.ParsePKIXPublicKey(pubKeyBlock.Bytes)
	if parseErr != nil {
		return nil, parseErr
	}

	return pubInterface.(*rsa.PublicKey), nil
}

func ParseRSAPrivateKey(data string) (*rsa.PrivateKey, error) {
	privateKeyBlock, _ := pem.Decode([]byte(data))
	//var pri *rsa.PrivateKey
	pri, parseErr := x509.ParsePKCS8PrivateKey(privateKeyBlock.Bytes)
	if parseErr != nil {
		fmt.Println("Load private key error")
		panic(parseErr)
	}

	return pri.(*rsa.PrivateKey), nil
}

func EncryptData(data string, pub *rsa.PublicKey) (string, error) {
	plainText := []byte(data)
	hash := sha256.New()
	random := rand.Reader
	encryptedData, encryptErr := rsa.EncryptOAEP(hash, random, pub, plainText, nil)
	if encryptErr != nil {
		return "", encryptErr
	}

	encodedData := base64.StdEncoding.EncodeToString(encryptedData)

	return encodedData, nil
}

func DecryptData(data string, pri *rsa.PrivateKey) (string, error) {
	cipherText, decryptErr := base64.StdEncoding.DecodeString(data)
	if decryptErr != nil {
		return "", decryptErr
	}
	hash := sha256.New()
	random := rand.Reader
	decryptedData, decryptErr := rsa.DecryptOAEP(hash, random, pri, cipherText, nil)
	if decryptErr != nil {
		return "", decryptErr
	}

	return string(decryptedData), nil
}

func Sign(message string, pri *rsa.PrivateKey) (string, error) {
	messageBytes := []byte(message)
	var opts rsa.PSSOptions
	opts.SaltLength = 32 // for simple example
	newhash := crypto.SHA256
	pssh := newhash.New()
	pssh.Write(messageBytes)
	hashed := pssh.Sum(nil)

	signature, err := rsa.SignPSS(rand.Reader, pri, newhash, hashed, &opts)

	if err != nil {
		return "", err
	}

	encodedData := base64.StdEncoding.EncodeToString(signature)

	return encodedData, nil
}

func Verify(message string, signature string, pub *rsa.PublicKey) (bool, error) {
	signatureBytes, err := base64.StdEncoding.DecodeString(signature)

	if err != nil {
		return false, err
	}

	messageBytes := []byte(message)
	var opts rsa.PSSOptions
	opts.SaltLength = 32
	newhash := crypto.SHA256
	pssh := newhash.New()
	pssh.Write(messageBytes)
	hashed := pssh.Sum(nil)
	err = rsa.VerifyPSS(pub, newhash, hashed, signatureBytes, &opts)

	if err != nil {
		return false, err
	}

	return true, nil
}

// GenerateRandomBytes returns securely generated random bytes.
// It will return an error if the system's secure random
// number generator fails to function correctly, in which
// case the caller should not continue.
func GenerateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := rand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return nil, err
	}

	return b, nil
}

func GenerateAesKey() (string, error) {
	key, err := GenerateRandomBytes(32)
	if err != nil {
		return "", nil
	}

	return base64.StdEncoding.EncodeToString(key), nil
}

func EncryptAES(plaintext string, keyBase64 string) (string, string, error) {
	key, err := base64.StdEncoding.DecodeString(keyBase64)
	if err != nil {
		return "", "", err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", "", err
	}

	// Never use more than 2^32 random nonces with a given key because of the risk of a repeat.
	iv := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return "", "", err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", "", err
	}

	ciphertext := aesgcm.Seal(nil, iv, []byte(plaintext), nil)

	ciphertextBase64 := base64.StdEncoding.EncodeToString(ciphertext)
	ivBase64 := base64.StdEncoding.EncodeToString(iv)

	return ciphertextBase64, ivBase64, nil
}

func DecryptAes(ciphertextBase64 string, ivBase64 string, keyBase64 string) (string, error) {

	key, err := base64.StdEncoding.DecodeString(keyBase64)

	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	iv, err := base64.StdEncoding.DecodeString(ivBase64)
	if err != nil {
		return "", err
	}

	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextBase64)
	if err != nil {
		return "", err
	}

	plaintext, err := aesgcm.Open(nil, iv, ciphertext, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}
