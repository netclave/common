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
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

type RedisStorage struct {
	client *redis.Client
	ctx    context.Context
}

func (rs *RedisStorage) Setup(credentials map[string]string) error {
	return nil
}

func (rs *RedisStorage) Init() error {
	return nil
}

func (rs *RedisStorage) Create(credentials map[string]string) error {

	db, err := strconv.ParseInt(credentials["db"], 10, 64)

	if err != nil {
		return err
	}

	rs.client = redis.NewClient(&redis.Options{
		Addr:     credentials["host"],
		Password: credentials["password"], // no password set
		DB:       int(db),                 // use default DB
	})

	rs.ctx = context.Background()

	return nil
}

func (rs *RedisStorage) Destroy() error {
	return rs.client.Close()
}

func (rs *RedisStorage) GetKeys(table string, pattern string) ([]string, error) {
	keysInterface, err := rs.client.Do(rs.ctx, "keys", table+"/"+pattern).Result()
	if err != nil {
		return nil, err
	}

	keysString := fmt.Sprintf("%v", keysInterface)
	keysString = keysString[1 : len(keysString)-1]
	keys := strings.Fields(keysString)

	return keys, nil
}

func (rs *RedisStorage) SetKey(table string, key string, value string, expiration time.Duration) error {
	err := rs.client.Set(rs.ctx, table+"/"+key, value, expiration).Err()
	if err != nil {
		return err
	}

	return nil
}

func (rs *RedisStorage) GetFullKey(key string) (string, error) {
	val, err := rs.client.Get(rs.ctx, key).Result()

	if err != nil {
		if err.Error() == redis.Nil.Error() {
			return "", nil
		}

		return "", err
	}
	return val, nil
}

func (rs *RedisStorage) GetKey(table string, key string) (string, error) {
	val, err := rs.client.Get(rs.ctx, table+"/"+key).Result()

	if err != nil {
		if err.Error() == redis.Nil.Error() {
			return "", nil
		}

		return "", err
	}
	return val, nil
}

func (rs *RedisStorage) DelKey(table string, key string) (int64, error) {
	deleted, err := rs.client.Del(rs.ctx, table+"/"+key).Result()

	if err != nil {
		return -1, err
	}
	return deleted, nil
}

func (rs *RedisStorage) AddToMap(table string, key string, objectKey string, object string) error {
	_, err := rs.client.HSet(rs.ctx, table+"/"+key, objectKey, object).Result()

	if err != nil {
		log.Println("Can not set map")
		return err
	}

	return nil
}

func (rs *RedisStorage) DelFromMap(table string, key string, objectKey string) error {
	_, err := rs.client.HDel(rs.ctx, table+"/"+key, objectKey).Result()

	return err
}

func (rs *RedisStorage) GetFromMap(table string, key string, objectKey string) (string, error) {
	cmdResult, err := rs.client.HGet(rs.ctx, table+"/"+key, objectKey).Result()

	if err != nil {
		return "", err
	}

	value := cmdResult

	return value, nil
}
func (rs *RedisStorage) GetMap(table string, key string) (map[string]string, error) {
	cmdResult, err := rs.client.HGetAll(rs.ctx, table+"/"+key).Result()

	if err != nil {
		return nil, err
	}

	result := map[string]string{}

	for key, value := range cmdResult {
		result[key] = value
	}

	return result, nil
}
