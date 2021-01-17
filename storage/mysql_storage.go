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

	_ "github.com/go-sql-driver/mysql"
)

type MySQLStorage struct {
	Connection *sql.DB
}

func (mss *MySQLStorage) Setup(credentials map[string]string) error {
	return nil
}

func (mss *MySQLStorage) executeSingleQuery(query string) error {
	statement, err := mss.Connection.Prepare(query) // Prepare SQL Statement
	if err != nil {
		return err
	}

	_, err = statement.Exec() // Execute SQL Statements

	return err
}

func (mss *MySQLStorage) Init() error {
	keysTableSQL := `CREATE TABLE IF NOT EXISTS ` + "`" + `keys` + "`" + `(
		 id BIGINT NOT NULL AUTO_INCREMENT,		
		 ` + "`" + `table` + "`" + ` LONGTEXT,
		 table_hash BIGINT,
		 ` + "`" + `key` + "`" + ` LONGTEXT,
		 `

	keysUniqueIndexValues := ""

	for i := 1; i <= NumberOfColumns; i++ {
		keysTableSQL += `column_` + strconv.Itoa(i) + `_hash BIGINT,
		 `

		keysUniqueIndexValues = keysUniqueIndexValues + "column_" + strconv.Itoa(i) + "_hash"

		if i != NumberOfColumns {
			keysUniqueIndexValues = keysUniqueIndexValues + ", "
		}
	}

	keysUniqueIndexValues += ", table_hash"

	keysTableSQL += `value LONGTEXT,
		 ttl BIGINT,
		 PRIMARY KEY (id),
		CONSTRAINT columns_unique_key UNIQUE(` + keysUniqueIndexValues + `));
	   ` // SQL Statement for Create Table

	log.Println(keysTableSQL)

	err := mss.executeSingleQuery(keysTableSQL)

	if err != nil {
		return err
	}

	keysTableSQL = `CREATE INDEX table_hash_index ON ` + "`" + `keys` + "`" + `(table_hash);`

	err = mss.executeSingleQuery(keysTableSQL)

	if err != nil {
		log.Println(err.Error())
	}

	for i := 1; i <= NumberOfColumns; i++ {
		keysTableSQL = `CREATE INDEX column_` + strconv.Itoa(i) + `_hash_index ON ` + "`" + `keys` + "`" + `(column_` + strconv.Itoa(i) + `_hash);`

		err = mss.executeSingleQuery(keysTableSQL)

		if err != nil {
			log.Println(err.Error())
		}
	}

	keysTableSQL = `CREATE INDEX ttl_index ON ` + "`" + `keys` + "`" + `(ttl);`

	err = mss.executeSingleQuery(keysTableSQL)

	if err != nil {
		log.Println(err.Error())
	}

	//og.Println("keys table created")

	mapsTableSQL := `CREATE TABLE IF NOT EXISTS maps (
		 id BIGINT NOT NULL AUTO_INCREMENT,		
		 ` + "`" + `table` + "`" + ` LONGTEXT,
		 table_hash BIGINT,
		 ` + "`" + `key` + "`" + ` LONGTEXT,
		 `

	mapKeysUniqueIndexValues := ""

	for i := 1; i <= NumberOfColumns; i++ {
		mapsTableSQL += `column_` + strconv.Itoa(i) + `_hash BIGINT,
		 `

		mapKeysUniqueIndexValues = mapKeysUniqueIndexValues + "column_" + strconv.Itoa(i) + "_hash"

		if i != NumberOfColumns {
			mapKeysUniqueIndexValues = mapKeysUniqueIndexValues + ", "
		}
	}

	mapKeysUniqueIndexValues += ", table_hash"
	mapKeysUniqueIndexValues += ", object_key_hash"

	mapsTableSQL += `value LONGTEXT,
		 object_key LONGTEXT,		
		 object_key_hash BIGINT,
		 PRIMARY KEY (id),	
		CONSTRAINT map_columns_unique_key UNIQUE(` + mapKeysUniqueIndexValues + `));
	   ` // SQL Statement for Create Table

	err = mss.executeSingleQuery(mapsTableSQL)

	if err != nil {
		return err
	}

	mapsTableSQL = `CREATE INDEX table_hash_index ON maps(table_hash);`

	err = mss.executeSingleQuery(mapsTableSQL)

	if err != nil {
		log.Println(err.Error())
	}

	for i := 1; i <= NumberOfColumns; i++ {
		mapsTableSQL = `CREATE INDEX column_` + strconv.Itoa(i) + `_hash_index ON maps(column_` + strconv.Itoa(i) + `_hash);`

		err = mss.executeSingleQuery(mapsTableSQL)

		if err != nil {
			log.Println(err.Error())
		}
	}

	log.Println("Before creating an index for object key hash")

	mapsTableSQL = `CREATE INDEX object_key_hash_index ON maps(object_key_hash);`

	err = mss.executeSingleQuery(mapsTableSQL)

	if err != nil {
		log.Println(err.Error())
	}

	LastTruncate = 0

	return nil
}

func (mss *MySQLStorage) Create(credentials map[string]string) error {
	var err error

	url := credentials["url"]
	username := credentials["username"]
	password := credentials["password"]
	dbname := credentials["dbname"]

	mss.Connection, err = sql.Open("mysql", username+":"+password+url+"/"+dbname)
	if err != nil {
		return err
	}

	return nil
}

func (mss *MySQLStorage) Destroy() error {
	return mss.Connection.Close() // Defer Closing the database
}

func (mss *MySQLStorage) GetKeys(table string, pattern string) ([]string, error) {
	err := mss.KeysCleanUp()

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

	sql := "SELECT `table`, `key`, ttl FROM `keys` WHERE table_hash = " + fmt.Sprint(tableHash)

	if clause != "" {
		sql += " AND " + clause
	}

	now := time.Now().UnixNano() / int64(time.Millisecond)

	row, err := mss.Connection.Query(sql)

	if err != nil {
		return nil, err
	}

	defer row.Close()
	for row.Next() { // Iterate and fetch the records from result cursor
		var table string
		var key string
		var ttl int64
		row.Scan(&table, &key, &ttl)

		if ttl >= now {
			keys = append(keys, table+"/"+key)
		}
	}

	sql = "SELECT `table`, `key` FROM maps WHERE table_hash = " + fmt.Sprint(tableHash)

	if clause != "" {
		sql += " AND " + clause
	}

	now = time.Now().UnixNano() / int64(time.Millisecond)

	row, err = mss.Connection.Query(sql)

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

func (mss *MySQLStorage) SetKey(table string, key string, value string, expiration time.Duration) error {
	err := mss.KeysCleanUp()

	if err != nil {
		return err
	}

	columns := SplitToParts(key)

	if len(columns) > NumberOfColumns {
		return errors.New("Too many data columns")
	}

	hashes := CalculateHashesOfColumns(columns)
	tableHash := CalculateHash(table)

	sql := "INSERT INTO `keys` (`table`, table_hash, `key`,"

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

	sql += " ON DUPLICATE KEY UPDATE value = VALUES(value), ttl = VALUES(ttl)"

	//log.Println(sql)

	_, err = mss.Connection.Exec(sql)

	return err
}

func (mss *MySQLStorage) GetFullKey(key string) (string, error) {
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

	return mss.GetKey(table, newKey)
}

func (mss *MySQLStorage) GetKey(table string, key string) (string, error) {
	err := mss.KeysCleanUp()

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

	sql := "SELECT `value`, `ttl` FROM `keys` WHERE table_hash = " + fmt.Sprint(tableHash)

	if clause != "" {
		sql += " AND " + clause
	}

	now := time.Now().UnixNano() / int64(time.Millisecond)

	row, err := mss.Connection.Query(sql)

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

func (mss *MySQLStorage) KeysCleanUp() error {
	now := time.Now().UnixNano() / int64(time.Millisecond)

	if (now - LastTruncate) > TruncateInterval {
		nowStr := fmt.Sprint(now)

		log.Println(nowStr)

		sql := "DELETE FROM `keys` WHERE ttl < " + nowStr

		result, err := mss.Connection.Exec(sql)

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

func (mss *MySQLStorage) DelKey(table string, key string) (int64, error) {
	err := mss.KeysCleanUp()

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

	sql := "DELETE FROM `keys` WHERE table_hash = " + fmt.Sprint(tableHash)

	if clause != "" {
		sql += " AND " + clause
	}

	res, err := mss.Connection.Exec(sql)

	if err != nil {
		return -1, err
	}

	return res.RowsAffected()
}

func (mss *MySQLStorage) AddToMap(table string, key string, objectKey string, object string) error {
	columns := SplitToParts(key)

	if len(columns) > NumberOfColumns {
		return errors.New("Too many data columns")
	}

	hashes := CalculateHashesOfColumns(columns)
	tableHash := CalculateHash(table)
	objectKeyHash := CalculateHash(objectKey)

	sql := "INSERT INTO maps (`table`, table_hash, `key`, object_key, object_key_hash,"

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

	sql += " ON DUPLICATE KEY UPDATE value = VALUES(value)"

	//log.Println(sql)

	_, err := mss.Connection.Exec(sql)

	return err
}

func (mss *MySQLStorage) DelFromMap(table string, key string, objectKey string) error {
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

	_, err := mss.Connection.Exec(sql)

	if err != nil {
		return err
	}

	return nil
}

func (mss *MySQLStorage) GetFromMap(table string, key string, objectKey string) (string, error) {
	columns := SplitToParts(key)

	if len(columns) > NumberOfColumns {
		return "", errors.New("Too many data columns")
	}

	hashes := CalculateHashesOfColumns(columns)
	tableHash := CalculateHash(table)
	objectKeyHash := CalculateHash(objectKey)

	clause := CreateWhereClause(columns, hashes)

	sql := "SELECT `value` FROM maps WHERE table_hash = " + fmt.Sprint(tableHash) + " AND object_key_hash = " + fmt.Sprint(objectKeyHash)

	if clause != "" {
		sql += " AND " + clause
	}

	row, err := mss.Connection.Query(sql)

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
func (mss *MySQLStorage) GetMap(table string, key string) (map[string]string, error) {
	result := map[string]string{}

	columns := SplitToParts(key)

	if len(columns) > NumberOfColumns {
		return nil, errors.New("Too many data columns")
	}

	hashes := CalculateHashesOfColumns(columns)
	tableHash := CalculateHash(table)

	clause := CreateWhereClause(columns, hashes)

	sql := "SELECT object_key, `value` FROM maps WHERE table_hash = " + fmt.Sprint(tableHash)

	if clause != "" {
		sql += " AND " + clause
	}

	row, err := mss.Connection.Query(sql)

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
