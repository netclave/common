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

package storage

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"reflect"
	"time"
)

type GenericStorage struct {
	Credentials map[string]string
	StorageType string
}

func (gs *GenericStorage) Init() error {
	storage, err := CreateStorage(gs.Credentials, gs.StorageType, true)

	if err != nil {
		return err
	}

	defer storage.Destroy()

	return storage.Init()
}

func (gs *GenericStorage) GetKeys(table string, pattern string) ([]string, error) {
	err := CheckTableName(table)

	if err != nil {
		return nil, err
	}

	storage, err := CreateStorage(gs.Credentials, gs.StorageType, false)

	if err != nil {
		return nil, err
	}

	defer storage.Destroy()

	return storage.GetKeys(table, pattern)
}

func (gs *GenericStorage) SetKey(table string, key string, value string, expiration time.Duration) error {
	err := CheckTableName(table)

	if err != nil {
		return err
	}

	storage, err := CreateStorage(gs.Credentials, gs.StorageType, false)

	if err != nil {
		return err
	}

	defer storage.Destroy()

	return storage.SetKey(table, key, value, expiration)
}

func (gs *GenericStorage) GetFullKey(key string) (string, error) {
	storage, err := CreateStorage(gs.Credentials, gs.StorageType, false)

	if err != nil {
		return "", err
	}

	defer storage.Destroy()

	return storage.GetFullKey(key)
}

func (gs *GenericStorage) GetKey(table string, key string) (string, error) {
	err := CheckTableName(table)

	if err != nil {
		return "", err
	}

	storage, err := CreateStorage(gs.Credentials, gs.StorageType, false)

	if err != nil {
		return "", err
	}

	defer storage.Destroy()

	return storage.GetKey(table, key)
}

func (gs *GenericStorage) DelKey(table string, key string) (int64, error) {
	err := CheckTableName(table)

	if err != nil {
		return 0, err
	}

	storage, err := CreateStorage(gs.Credentials, gs.StorageType, false)

	if err != nil {
		return 0, err
	}

	defer storage.Destroy()

	return storage.DelKey(table, key)
}

func (gs *GenericStorage) AddToMap(table string, key string, objectKey string, object interface{}) error {
	err := CheckTableName(table)

	if err != nil {
		return err
	}

	storage, err := CreateStorage(gs.Credentials, gs.StorageType, false)

	if err != nil {
		return err
	}

	defer storage.Destroy()

	json, err := json.Marshal(object)

	if err != nil {
		log.Println("Can not json encode")
		return err
	}

	// Encode
	encodedString := base64.StdEncoding.EncodeToString(json)

	return storage.AddToMap(table, key, objectKey, encodedString)
}

func (gs *GenericStorage) DelFromMap(table string, key string, objectKey string) error {
	err := CheckTableName(table)

	if err != nil {
		return err
	}

	storage, err := CreateStorage(gs.Credentials, gs.StorageType, false)

	if err != nil {
		return err
	}

	defer storage.Destroy()

	return storage.DelFromMap(table, key, objectKey)
}

func (gs *GenericStorage) GetFromMap(table string, key string, objectKey string, reference interface{}) error {
	err := CheckTableName(table)

	if err != nil {
		return err
	}

	storage, err := CreateStorage(gs.Credentials, gs.StorageType, false)

	if err != nil {
		return err
	}

	defer storage.Destroy()

	value, err := storage.GetFromMap(table, key, objectKey)

	if err != nil {
		return err
	}

	bytes, err := base64.StdEncoding.DecodeString(value)

	if err != nil {
		return err
	}

	err = json.Unmarshal(bytes, &reference)

	if err != nil {
		return err
	}

	return nil
}

func (gs *GenericStorage) GetMap(table string, key string, reference interface{}) error {
	err := CheckTableName(table)

	if err != nil {
		return err
	}

	storage, err := CreateStorage(gs.Credentials, gs.StorageType, false)

	if err != nil {
		return err
	}

	defer storage.Destroy()

	data, err := storage.GetMap(table, key)

	if err != nil {
		return err
	}

	if reference == nil {
		return errors.New("reference is null")
	}

	paramType := reflect.TypeOf(reference)

	if paramType.Kind() != reflect.Ptr ||
		(paramType.Elem().Kind() != reflect.Map || paramType.Elem().Key().Kind() != reflect.String) ||
		paramType.Elem().Elem().Kind() != reflect.Ptr {

		return errors.New("fill slice: parameter is not in format *map[string]*interface{}")
	}

	containerType := paramType.Elem()
	messageType := containerType.Elem()
	elementType := messageType.Elem()
	paramValue := reflect.ValueOf(reference)
	containerValue := paramValue.Elem()

	if containerValue.IsNil() {
		containerValue.Set(reflect.MakeMapWithSize(reflect.MapOf(containerType.Key(), messageType), len(data)))
	}

	for key, element := range data {
		messageValue := reflect.New(elementType)

		bytes, err := base64.StdEncoding.DecodeString(element)

		if err != nil {
			return err
		}

		err = json.Unmarshal(bytes, messageValue.Interface())

		if err != nil {
			return err
		}
		keyValue := reflect.ValueOf(key)

		containerValue.SetMapIndex(keyValue, messageValue)
	}

	return nil
}
