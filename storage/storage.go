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
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"time"
)

var REDIS_STORAGE = "redis"
var SQLITE_STORAGE = "sqlite"
var POSTGRE_SQL_STORAGE = "postgresql"
var MY_SQL_STORAGE = "mysql"
var LastTruncate = int64(0)

const NumberOfColumns = 10
const TruncateInterval = 60 * 1000

type Storage interface {
	Setup(credentials map[string]string) error
	Init() error
	Create(credentials map[string]string) error
	Destroy() error
	GetKeys(table string, pattern string) ([]string, error)
	SetKey(table string, key string, value string, expiration time.Duration) error
	GetFullKey(key string) (string, error)
	GetKey(table string, key string) (string, error)
	DelKey(table string, key string) (int64, error)
	AddToMap(table string, key string, objectKey string, object string) error
	DelFromMap(table string, key string, objectKey string) error
	GetFromMap(table string, key string, objectKey string) (string, error)
	GetMap(table string, key string) (map[string]string, error)
}

func SplitToParts(key string) []string {
	return strings.Split(key, "/")
}

func CalculateHash(data string) uint32 {
	crc32InUint32 := crc32.ChecksumIEEE([]byte(data))

	return crc32InUint32
}

func CalculateHashesOfColumns(columns []string) []uint32 {
	result := []uint32{}

	for _, column := range columns {
		hash := CalculateHash(column)

		result = append(result, hash)
	}

	return result
}

func CreateWhereClause(columns []string, hashes []uint32) string {
	result := ""

	hashesLen := len(hashes)

	for hashesLen-1 >= 0 && columns[hashesLen-1] == "*" {
		hashesLen--
	}

	for index := 0; index < hashesLen; index++ {
		hash := hashes[index]

		if columns[index] == "*" {
			continue
		}

		hashStr := fmt.Sprint(hash)
		columnName := "column_" + strconv.Itoa(index+1) + "_hash"

		result += columnName + " = " + hashStr

		if index+1 < hashesLen {
			result += " AND "
		}
	}

	return result
}

func CheckTableName(table string) error {
	if strings.Contains(table, "/") {
		return errors.New("Table can not contains '/' in its name")
	}

	return nil
}

func CreateStorage(credentials map[string]string, storageType string, setup bool) (Storage, error) {
	switch storageType {
	case REDIS_STORAGE:
		storage := &RedisStorage{}
		if setup == true {
			storage.Setup(credentials)
		}
		err := storage.Create(credentials)

		return storage, err
	case SQLITE_STORAGE:
		storage := &SQLiteStorage{}
		if setup == true {
			storage.Setup(credentials)
		}
		err := storage.Create(credentials)

		return storage, err
	case POSTGRE_SQL_STORAGE:
		storage := &PostgreSQLStorage{}
		if setup == true {
			storage.Setup(credentials)
		}
		err := storage.Create(credentials)

		return storage, err
	case MY_SQL_STORAGE:
		storage := &MySQLStorage{}
		if setup == true {
			storage.Setup(credentials)
		}
		err := storage.Create(credentials)

		return storage, err

	default:
		return nil, errors.New("No such storage type")
	}
}
