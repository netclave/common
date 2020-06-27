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
	"github.com/netclave/common/storage"
)

var PUBLIC_KEYS = "publickeys"
var PUBLIC_KEYS_TEMP = "publickeysTemp"
var PRIVATE_KEYS = "privatekeys"
var LABEL_TO_IDENTIFICATOR_ID = "labeltoidentificatorid"
var IDENTIFICATOR_TO_PUBLIC_KEY_LABELS = "identificatortopublickeylabels"
var IDENTIFICATORS = "identificators"
var IDENTIFICATOR_TO_IDENTIFICATOR = "identificatortoidentificator"
var IDENTIFICATOR_CONFIRMATION_CODE = "identificatorscode"
var IDENTIFICATOR_TO_IDENTITY_ID = "identificatortoidentity"
var IDENTIFICATOR_TO_IDENTITY_ID_TEMP = "identificatortoidentitytemp"
var IDENTITY_ID_TO_IDENTIFICATORS = "identityidtoidentificators"

var IDENTIFICATOR_TYPE_IDENTITY_PROVIDER = "identityProvider"
var IDENTIFICATOR_TYPE_GENERATOR = "generator"
var IDENTIFICATOR_TYPE_WALLET = "wallet"
var IDENTIFICATOR_TYPE_OPENER = "opener"
var IDENTIFICATOR_TYPE_PROXY = "proxy"

type CryptoStorage struct {
	Credentials map[string]string
	StorageType string
}

func (cs *CryptoStorage) createStorage() (*storage.GenericStorage, error) {
	return &storage.GenericStorage{
		Credentials: cs.Credentials,
		StorageType: cs.StorageType,
	}, nil
}

func (cs *CryptoStorage) StorePublicKey(label string, pubKey string) error {
	storage, err := cs.createStorage()

	if err != nil {
		return err
	}

	return storage.SetKey(PUBLIC_KEYS, label, pubKey, 0)
}

func (cs *CryptoStorage) RetrievePublicKey(label string) (string, error) {
	storage, err := cs.createStorage()

	if err != nil {
		return "", err
	}

	return storage.GetKey(PUBLIC_KEYS, label)
}

func (cs *CryptoStorage) DeletePublicKey(label string) (int64, error) {
	storage, err := cs.createStorage()

	if err != nil {
		return -1, err
	}

	return storage.DelKey(PUBLIC_KEYS, label)
}

func (cs *CryptoStorage) StoreTempPublicKey(label string, pubKey string) error {
	storage, err := cs.createStorage()

	if err != nil {
		return err
	}

	return storage.SetKey(PUBLIC_KEYS_TEMP, label, pubKey, 0)
}

func (cs *CryptoStorage) RetrieveTempPublicKey(label string) (string, error) {
	storage, err := cs.createStorage()

	if err != nil {
		return "", err
	}

	return storage.GetKey(PUBLIC_KEYS_TEMP, label)
}

func (cs *CryptoStorage) DeleteTempPublicKey(label string) (int64, error) {
	storage, err := cs.createStorage()

	if err != nil {
		return 0, err
	}

	return storage.DelKey(PUBLIC_KEYS_TEMP, label)
}

func (cs *CryptoStorage) StorePrivateKey(label string, priKey string) error {
	storage, err := cs.createStorage()

	if err != nil {
		return err
	}

	return storage.SetKey(PRIVATE_KEYS, label, priKey, 0)
}

func (cs *CryptoStorage) RetrievePrivateKey(label string) (string, error) {
	storage, err := cs.createStorage()

	if err != nil {
		return "", err
	}

	return storage.GetKey(PRIVATE_KEYS, label)
}

func (cs *CryptoStorage) DeletePrivateKey(label string) (int64, error) {
	storage, err := cs.createStorage()

	if err != nil {
		return 0, err
	}

	return storage.DelKey(PRIVATE_KEYS, label)
}

func (cs *CryptoStorage) SetIdentificatorByLabel(label string, identificatorID string) error {
	storage, err := cs.createStorage()

	if err != nil {
		return err
	}

	return storage.SetKey(LABEL_TO_IDENTIFICATOR_ID, label, identificatorID, 0)
}

func (cs *CryptoStorage) GetIdentificatorByLabel(label string) (string, error) {
	storage, err := cs.createStorage()

	if err != nil {
		return "", err
	}

	return storage.GetKey(LABEL_TO_IDENTIFICATOR_ID, label)
}

func (cs *CryptoStorage) DeleteIdentificatorByLabel(label string) (int64, error) {
	storage, err := cs.createStorage()

	if err != nil {
		return 0, err
	}

	return storage.DelKey(LABEL_TO_IDENTIFICATOR_ID, label)
}

type Identificator struct {
	IdentificatorID   string
	IdentificatorType string
	IdentificatorURL  string
	IdentificatorName string
}

func (cs *CryptoStorage) AddIdentificator(identificator *Identificator) error {
	storage, err := cs.createStorage()

	if err != nil {
		return err
	}

	return storage.AddToMap(IDENTIFICATORS, "", identificator.IdentificatorID, *identificator)
}

func (cs *CryptoStorage) DeleteIdentificator(identificatorID string) error {
	storage, err := cs.createStorage()

	if err != nil {
		return err
	}

	return storage.DelFromMap(IDENTIFICATORS, "", identificatorID)
}

func (cs *CryptoStorage) GetIdentificators() (map[string]*Identificator, error) {
	storage, err := cs.createStorage()

	if err != nil {
		return nil, err
	}

	var result map[string]*Identificator

	err = storage.GetMap(IDENTIFICATORS, "", &result)

	if err != nil {
		return nil, err
	}

	return result, nil
}

func (cs *CryptoStorage) AddPublicKeyLabelToIdentificator(identificatorID string, label string) error {
	storage, err := cs.createStorage()

	if err != nil {
		return err
	}

	return storage.AddToMap(IDENTIFICATOR_TO_PUBLIC_KEY_LABELS, identificatorID, label, label)
}

func (cs *CryptoStorage) DelPublicKeyLabelToIdentificator(identificatorID string, label string) error {
	storage, err := cs.createStorage()

	if err != nil {
		return err
	}

	return storage.DelFromMap(IDENTIFICATOR_TO_PUBLIC_KEY_LABELS, identificatorID, label)
}

func (cs *CryptoStorage) GetPublicKeyLabelsForIdentificator(identificatorID string) (map[string]string, error) {
	storage, err := cs.createStorage()

	if err != nil {
		return nil, err
	}

	var labels map[string]*string

	err = storage.GetMap(IDENTIFICATOR_TO_PUBLIC_KEY_LABELS, identificatorID, &labels)

	if err != nil {
		return nil, err
	}

	result := make(map[string]string)

	for key, value := range labels {
		result[key] = *value
	}

	return result, nil
}

func (cs *CryptoStorage) AddIdentificatorToIdentificator(identificator1 *Identificator, identificator2 *Identificator) error {
	storage, err := cs.createStorage()

	if err != nil {
		return err
	}

	return storage.AddToMap(IDENTIFICATOR_TO_IDENTIFICATOR, identificator1.IdentificatorID, identificator2.IdentificatorID, *identificator2)
}

func (cs *CryptoStorage) DelIdentificatorToIdentificator(identificator1 *Identificator, identificator2 *Identificator) error {
	storage, err := cs.createStorage()

	if err != nil {
		return err
	}

	return storage.DelFromMap(IDENTIFICATOR_TO_IDENTIFICATOR, identificator1.IdentificatorID, identificator2.IdentificatorID)
}

func (cs *CryptoStorage) GetIdentificatorToIdentificatorMap(identificator1 *Identificator, identificatorType string) (map[string]*Identificator, error) {
	storage, err := cs.createStorage()

	if err != nil {
		return nil, err
	}

	var result map[string]*Identificator

	err = storage.GetMap(IDENTIFICATOR_TO_IDENTIFICATOR, identificator1.IdentificatorID, &result)

	if err != nil {
		return nil, err
	}

	filteredMap := map[string]*Identificator{}

	for key, value := range result {
		if value.IdentificatorType == identificatorType {
			filteredMap[key] = value
		}
	}

	return filteredMap, nil
}

func (cs *CryptoStorage) SetIdentityIDForIdentificator(identificatorID string, identityID string) error {
	storage, err := cs.createStorage()

	if err != nil {
		return err
	}

	return storage.SetKey(IDENTIFICATOR_TO_IDENTITY_ID, identificatorID, identityID, 0)
}

func (cs *CryptoStorage) GetIdentityIDForIdentificator(identificatorID string) (string, error) {
	storage, err := cs.createStorage()

	if err != nil {
		return "", err
	}

	return storage.GetKey(IDENTIFICATOR_TO_IDENTITY_ID, identificatorID)
}

func (cs *CryptoStorage) DelIdentityIDForIdentificator(identificatorID string) (int64, error) {
	storage, err := cs.createStorage()

	if err != nil {
		return 0, err
	}

	return storage.DelKey(IDENTIFICATOR_TO_IDENTITY_ID, identificatorID)
}

func (cs *CryptoStorage) SetTempIdentityIDForIdentificator(identificatorID string, identityID string) error {
	storage, err := cs.createStorage()

	if err != nil {
		return err
	}

	return storage.SetKey(IDENTIFICATOR_TO_IDENTITY_ID_TEMP, identificatorID, identityID, 0)
}

func (cs *CryptoStorage) GetTempIdentityIDForIdentificator(identificatorID string) (string, error) {
	storage, err := cs.createStorage()

	if err != nil {
		return "", err
	}

	return storage.GetKey(IDENTIFICATOR_TO_IDENTITY_ID_TEMP, identificatorID)
}

func (cs *CryptoStorage) DelTempIdentityIDForIdentificator(identificatorID string) (int64, error) {
	storage, err := cs.createStorage()

	if err != nil {
		return 0, err
	}

	return storage.DelKey(IDENTIFICATOR_TO_IDENTITY_ID_TEMP, identificatorID)
}

func (cs *CryptoStorage) AddIdentificatorToIdentityID(identificatorID string, identityID string) error {
	storage, err := cs.createStorage()

	if err != nil {
		return err
	}

	return storage.AddToMap(IDENTITY_ID_TO_IDENTIFICATORS, identityID, identificatorID, identificatorID)
}

func (cs *CryptoStorage) DelIdentificatorFromIdentityID(identificatorID string, identityID string) error {
	storage, err := cs.createStorage()

	if err != nil {
		return err
	}

	return storage.DelFromMap(IDENTITY_ID_TO_IDENTIFICATORS, identityID, identificatorID)
}

func (cs *CryptoStorage) GetIdentificatorsByIdentityID(identityID string) (map[string]string, error) {
	storage, err := cs.createStorage()

	if err != nil {
		return nil, err
	}

	var tmp map[string]*string

	err = storage.GetMap(IDENTITY_ID_TO_IDENTIFICATORS, identityID, &tmp)

	if err != nil {
		return nil, err
	}

	result := make(map[string]string)

	for key, value := range tmp {
		result[key] = *value
	}

	return result, nil
}
