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
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

type PostgreSQLStorage struct {
	Connection *sql.DB
}

func (pss *PostgreSQLStorage) Setup(credentials map[string]string) error {
	return nil
}

func (pss *PostgreSQLStorage) executeSingleQuery(query string) error {
	statement, err := pss.Connection.Prepare(query) // Prepare SQL Statement
	if err != nil {
		return err
	}

	_, err = statement.Exec() // Execute SQL Statements

	return err
}

func (pss *PostgreSQLStorage) Init() error {
	keysTableSQL := `CREATE TABLE IF NOT EXISTS keys (
		 "id" BIGSERIAL NOT NULL PRIMARY KEY,		
		 "table" TEXT,
		 "table_hash" BIGINT,
		 "key" TEXT,
		 `

	keysUniqueIndexValues := ""

	for i := 1; i <= NumberOfColumns; i++ {
		keysTableSQL += `"column_` + strconv.Itoa(i) + `_hash" BIGINT,
		 `

		keysUniqueIndexValues = keysUniqueIndexValues + "column_" + strconv.Itoa(i) + "_hash"

		if i != NumberOfColumns {
			keysUniqueIndexValues = keysUniqueIndexValues + ", "
		}
	}

	keysUniqueIndexValues += ", table_hash"

	keysTableSQL += `"value" TEXT,
		 "ttl" BIGINT,		
		CONSTRAINT columns_unique_key UNIQUE(` + keysUniqueIndexValues + `));`

	log.Println(keysTableSQL)

	err := pss.executeSingleQuery(keysTableSQL)

	if err != nil {
		return err
	}

	keysTableSQL = `CREATE INDEX IF NOT EXISTS table_hash_index ON keys(table_hash);`

	err = pss.executeSingleQuery(keysTableSQL)

	if err != nil {
		return err
	}

	for i := 1; i <= NumberOfColumns; i++ {
		keysTableSQL = `CREATE INDEX IF NOT EXISTS column_` + strconv.Itoa(i) + `_hash_index ON keys(column_` + strconv.Itoa(i) + `_hash);`

		err = pss.executeSingleQuery(keysTableSQL)

		if err != nil {
			return err
		}
	}

	keysTableSQL = `CREATE INDEX IF NOT EXISTS ttl_index ON keys(ttl);`

	err = pss.executeSingleQuery(keysTableSQL)

	if err != nil {
		return err
	}

	mapsTableSQL := `CREATE TABLE IF NOT EXISTS maps (
		 "id" BIGSERIAL NOT NULL PRIMARY KEY,		
		 "table" TEXT,
		 "table_hash" BIGINT,
		 "key" TEXT,
		 `

	mapKeysUniqueIndexValues := ""

	for i := 1; i <= NumberOfColumns; i++ {
		mapsTableSQL += `"column_` + strconv.Itoa(i) + `_hash" BIGINT,
		 `

		mapKeysUniqueIndexValues = mapKeysUniqueIndexValues + "column_" + strconv.Itoa(i) + "_hash"

		if i != NumberOfColumns {
			mapKeysUniqueIndexValues = mapKeysUniqueIndexValues + ", "
		}
	}

	mapKeysUniqueIndexValues += ", table_hash"
	mapKeysUniqueIndexValues += ", object_key_hash"

	mapsTableSQL += `"value" TEXT,
		 "object_key" TEXT,		
		 "object_key_hash" BIGINT,		
		CONSTRAINT map_columns_unique_key UNIQUE(` + mapKeysUniqueIndexValues + `));`

	log.Println(mapsTableSQL)

	err = pss.executeSingleQuery(mapsTableSQL)

	if err != nil {
		return err
	}

	mapsTableSQL = `CREATE INDEX IF NOT EXISTS table_hash_index ON maps(table_hash);`

	err = pss.executeSingleQuery(mapsTableSQL)

	if err != nil {
		return err
	}

	for i := 1; i <= NumberOfColumns; i++ {
		mapsTableSQL = `CREATE INDEX IF NOT EXISTS column_` + strconv.Itoa(i) + `_hash_index ON maps(column_` + strconv.Itoa(i) + `_hash);`

		err = pss.executeSingleQuery(mapsTableSQL)

		if err != nil {
			return err
		}
	}

	log.Println("Before creating an index for object key hash")

	mapsTableSQL = `CREATE INDEX IF NOT EXISTS object_key_hash_index ON maps(object_key_hash);`

	err = pss.executeSingleQuery(mapsTableSQL)

	if err != nil {
		return err
	}

	LastTruncate = 0

	return nil
}

func (pss *PostgreSQLStorage) Create(credentials map[string]string) error {
	var err error

	host := credentials["host"]
	port := credentials["port"]
	user := credentials["user"]
	password := credentials["password"]
	dbname := credentials["dbname"]
	sslmode := credentials["sslmode"]

	psqlconn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", host, port, user, password, dbname, sslmode)

	pss.Connection, err = sql.Open("postgres", psqlconn)
	if err != nil {
		return err
	}

	return nil
}

func (pss *PostgreSQLStorage) Destroy() error {
	return pss.Connection.Close()
}

func (pss *PostgreSQLStorage) GetKeys(table string, pattern string) ([]string, error) {
	err := pss.KeysCleanUp()

	if err != nil {
		return nil, err
	}

	keys := []string{}

	columns := SplitToParts(pattern)

	if len(columns) > NumberOfColumns {
		return nil, errors.New("Too many data columns")
	}

	hashes := CalculateHashesOfColumns(columns)
	tableHash := CalculateHash(table)

	clause := CreateWhereClause(columns, hashes)

	sql := "SELECT \"table\", \"key\", ttl FROM keys WHERE table_hash = " + fmt.Sprint(tableHash)

	if clause != "" {
		sql += " AND " + clause
	}

	now := time.Now().UnixNano() / int64(time.Millisecond)

	row, err := pss.Connection.Query(sql)

	if err != nil {
		return nil, err
	}

	defer row.Close()
	for row.Next() {
		var table string
		var key string
		var ttl int64
		row.Scan(&table, &key, &ttl)

		if ttl >= now {
			keys = append(keys, table+"/"+key)
		}
	}

	sql = "SELECT \"table\", \"key\" FROM maps WHERE table_hash = " + fmt.Sprint(tableHash)

	if clause != "" {
		sql += " AND " + clause
	}

	now = time.Now().UnixNano() / int64(time.Millisecond)

	row, err = pss.Connection.Query(sql)

	if err != nil {
		return nil, err
	}

	defer row.Close()
	for row.Next() { // Iterate and fetch the records from result cursor
		var table string
		var key string
		row.Scan(&table, &key)

		keys = append(keys, table+"/"+key)
	}

	return keys, nil
}

func (pss *PostgreSQLStorage) SetKey(table string, key string, value string, expiration time.Duration) error {
	err := pss.KeysCleanUp()

	if err != nil {
		return err
	}

	columns := SplitToParts(key)

	if len(columns) > NumberOfColumns {
		return errors.New("Too many data columns")
	}

	hashes := CalculateHashesOfColumns(columns)
	tableHash := CalculateHash(table)

	sql := "INSERT INTO keys (\"table\", table_hash, key,"

	keysUniqueIndexValues := ""

	for i := 1; i <= NumberOfColumns; i++ {
		sql += " column_" + strconv.Itoa(i) + "_hash,"
	}

	sql += " value, ttl) VALUES ('" + table + "', " + fmt.Sprint(tableHash) + ", '" + key + "',"

	hashesLen := len(hashes)

	for i := 1; i <= NumberOfColumns; i++ {
		if i <= hashesLen {
			sql += " " + fmt.Sprint(hashes[i-1]) + ","
		} else {
			sql += " 0,"
		}

		keysUniqueIndexValues = keysUniqueIndexValues + "column_" + strconv.Itoa(i) + "_hash"

		if i != NumberOfColumns {
			keysUniqueIndexValues = keysUniqueIndexValues + ", "
		}
	}

	keysUniqueIndexValues += ", table_hash"

	until := time.Now().Add(expiration)

	if expiration.Milliseconds() <= 0 {
		until = time.Unix(0, int64(math.MaxInt64))
	}

	untilMilliseconds := until.UnixNano() / int64(time.Millisecond)

	sql += " '" + value + "', " + strconv.FormatInt(untilMilliseconds, 10) + ")"

	sql += " ON CONFLICT(" + keysUniqueIndexValues + ") DO UPDATE SET value = EXCLUDED.value, ttl = EXCLUDED.ttl"

	//log.Println(sql)

	_, err = pss.Connection.Exec(sql)

	return err
}

func (pss *PostgreSQLStorage) GetFullKey(key string) (string, error) {
	parts := SplitToParts(key)

	table := parts[0]

	newKey := ""

	partsLen := len(parts)

	for i := 1; i < partsLen; i++ {
		newKey = newKey + parts[i]

		if i+1 < partsLen {
			newKey += "/"
		}
	}

	return pss.GetKey(table, newKey)
}

func (pss *PostgreSQLStorage) GetKey(table string, key string) (string, error) {
	err := pss.KeysCleanUp()

	if err != nil {
		return "", err
	}

	columns := SplitToParts(key)

	if len(columns) > NumberOfColumns {
		return "", errors.New("Too many data columns")
	}

	hashes := CalculateHashesOfColumns(columns)
	tableHash := CalculateHash(table)

	clause := CreateWhereClause(columns, hashes)

	sql := "SELECT \"value\", \"ttl\" FROM keys WHERE table_hash = " + fmt.Sprint(tableHash)

	if clause != "" {
		sql += " AND " + clause
	}

	now := time.Now().UnixNano() / int64(time.Millisecond)

	row, err := pss.Connection.Query(sql)

	if err != nil {
		return "", err
	}

	defer row.Close()
	for row.Next() { // Iterate and fetch the records from result cursor
		var value string
		var ttl int64
		row.Scan(&value, &ttl)

		if ttl >= now {
			return value, nil
		}
	}

	return "", nil
}

func (pss *PostgreSQLStorage) KeysCleanUp() error {
	now := time.Now().UnixNano() / int64(time.Millisecond)

	if (now - LastTruncate) > TruncateInterval {
		nowStr := fmt.Sprint(now)

		log.Println(nowStr)

		sql := "DELETE FROM keys WHERE ttl < " + nowStr

		result, err := pss.Connection.Exec(sql)

		if err != nil {
			return err
		}

		truncatedRows, err := result.RowsAffected()

		if err != nil {
			return err
		}

		log.Printf("%d rows truncated\n", truncatedRows)

		LastTruncate = now
	}

	return nil
}

func (pss *PostgreSQLStorage) DelKey(table string, key string) (int64, error) {
	err := pss.KeysCleanUp()

	if err != nil {
		return 0, err
	}

	columns := SplitToParts(key)

	if len(columns) > NumberOfColumns {
		return -1, errors.New("Too many data columns")
	}

	hashes := CalculateHashesOfColumns(columns)
	tableHash := CalculateHash(table)

	clause := CreateWhereClause(columns, hashes)

	sql := "DELETE FROM keys WHERE table_hash = " + fmt.Sprint(tableHash)

	if clause != "" {
		sql += " AND " + clause
	}

	res, err := pss.Connection.Exec(sql)

	if err != nil {
		return -1, err
	}

	return res.RowsAffected()
}

func (pss *PostgreSQLStorage) AddToMap(table string, key string, objectKey string, object string) error {
	columns := SplitToParts(key)

	if len(columns) > NumberOfColumns {
		return errors.New("Too many data columns")
	}

	hashes := CalculateHashesOfColumns(columns)
	tableHash := CalculateHash(table)
	objectKeyHash := CalculateHash(objectKey)

	sql := "INSERT INTO maps (\"table\", table_hash, key, object_key, object_key_hash,"

	keysUniqueIndexValues := ""

	for i := 1; i <= NumberOfColumns; i++ {
		sql += " column_" + strconv.Itoa(i) + "_hash,"
	}

	sql += " value) VALUES ('" + table + "', " + fmt.Sprint(tableHash) + ", '" + key + "', '" + objectKey + "', " + fmt.Sprint(objectKeyHash) + ", "

	hashesLen := len(hashes)

	for i := 1; i <= NumberOfColumns; i++ {
		if i <= hashesLen {
			sql += " " + fmt.Sprint(hashes[i-1]) + ","
		} else {
			sql += " 0,"
		}

		keysUniqueIndexValues = keysUniqueIndexValues + "column_" + strconv.Itoa(i) + "_hash"

		if i != NumberOfColumns {
			keysUniqueIndexValues = keysUniqueIndexValues + ", "
		}
	}

	keysUniqueIndexValues += ", table_hash"
	keysUniqueIndexValues += ", object_key_hash"

	sql += " '" + object + "')"

	sql += " ON CONFLICT(" + keysUniqueIndexValues + ") DO UPDATE SET value = EXCLUDED.value"

	//log.Println(sql)

	_, err := pss.Connection.Exec(sql)

	return err
}

func (pss *PostgreSQLStorage) DelFromMap(table string, key string, objectKey string) error {
	columns := SplitToParts(key)

	if len(columns) > NumberOfColumns {
		return errors.New("Too many data columns")
	}

	hashes := CalculateHashesOfColumns(columns)
	tableHash := CalculateHash(table)
	objectKeyHash := CalculateHash(objectKey)

	clause := CreateWhereClause(columns, hashes)

	sql := "DELETE FROM maps WHERE table_hash = " + fmt.Sprint(tableHash) + " AND object_key_hash = " + fmt.Sprint(objectKeyHash)

	if clause != "" {
		sql += " AND " + clause
	}

	_, err := pss.Connection.Exec(sql)

	if err != nil {
		return err
	}

	return nil
}

func (pss *PostgreSQLStorage) GetFromMap(table string, key string, objectKey string) (string, error) {
	columns := SplitToParts(key)

	if len(columns) > NumberOfColumns {
		return "", errors.New("Too many data columns")
	}

	hashes := CalculateHashesOfColumns(columns)
	tableHash := CalculateHash(table)
	objectKeyHash := CalculateHash(objectKey)

	clause := CreateWhereClause(columns, hashes)

	sql := "SELECT \"value\" FROM maps WHERE table_hash = " + fmt.Sprint(tableHash) + " AND object_key_hash = " + fmt.Sprint(objectKeyHash)

	if clause != "" {
		sql += " AND " + clause
	}

	row, err := pss.Connection.Query(sql)

	if err != nil {
		return "", err
	}

	defer row.Close()
	for row.Next() { // Iterate and fetch the records from result cursor
		var value string
		row.Scan(&value)

		return value, nil
	}

	return "", nil
}
func (pss *PostgreSQLStorage) GetMap(table string, key string) (map[string]string, error) {
	result := map[string]string{}

	columns := SplitToParts(key)

	if len(columns) > NumberOfColumns {
		return nil, errors.New("Too many data columns")
	}

	hashes := CalculateHashesOfColumns(columns)
	tableHash := CalculateHash(table)

	clause := CreateWhereClause(columns, hashes)

	sql := "SELECT object_key, \"value\" FROM maps WHERE table_hash = " + fmt.Sprint(tableHash)

	if clause != "" {
		sql += " AND " + clause
	}

	row, err := pss.Connection.Query(sql)

	if err != nil {
		return nil, err
	}

	defer row.Close()
	for row.Next() { // Iterate and fetch the records from result cursor
		var value string
		var objectKey string
		row.Scan(&objectKey, &value)

		result[objectKey] = value
	}

	return result, nil
}
