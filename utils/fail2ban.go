package utils

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/netclave/common/networkutils"
	"github.com/netclave/common/storage"
)

const FAILED_EVENTS_TABLE = "failedEvents"
const FAILED_IPS_TABLE = "failedIPs"

var LAST_TIME_LOGGED = map[string]int64{}

type Event struct {
	ID       string
	IP       string
	Priority string
}

type Fail2BanData struct {
	DataStorage   *storage.GenericStorage
	RemoteAddress string
	TTL           int64
}

func CreateSimpleEvent(remoteAddress string) (*Event, error) {
	uuid, err := GenerateUUID()

	if err != nil {
		return nil, err
	}

	return &Event{
		ID:       uuid,
		IP:       networkutils.ParseIP(remoteAddress),
		Priority: "1",
	}, nil
}

func StoreBannedIP(dataStorage *storage.GenericStorage, event *Event, ttl int64) error {
	err := dataStorage.SetKey(FAILED_IPS_TABLE, event.IP, event.IP, time.Duration(ttl)*time.Millisecond)

	if err != nil {
		return err
	}

	err = dataStorage.AddToMap(FAILED_EVENTS_TABLE, event.IP, event.ID, event)

	if err != nil {
		return err
	}

	return nil
}

func LogBannedIPs(dataStorage *storage.GenericStorage) error {
	eventsPerIpsKeys, err := dataStorage.GetKeys(FAILED_EVENTS_TABLE, "*")

	if err != nil {
		log.Println(err.Error())
		return err
	}

	for _, key := range eventsPerIpsKeys {
		ip := strings.ReplaceAll(key, FAILED_EVENTS_TABLE+"/", "")

		res, err := dataStorage.GetKey(FAILED_IPS_TABLE, ip)

		if err != nil {
			return err
		}

		var events map[string]*Event

		err = dataStorage.GetMap(FAILED_EVENTS_TABLE, ip, &events)

		if err != nil {
			return err
		}

		if res == "" {
			for eventKey := range events {
				err = dataStorage.DelFromMap(FAILED_EVENTS_TABLE, ip, eventKey)

				if err != nil {
					return err
				}
			}

			continue
		}

		timestamp, ok := LAST_TIME_LOGGED[ip]

		if ok == false {
			timestamp = int64(0)
		}

		now := time.Now().UnixNano() / int64(time.Millisecond)

		if now-timestamp > 60000 {
			fmt.Fprintf(os.Stderr, "Failed request for ip: %s\n", ip)

			LAST_TIME_LOGGED[ip] = now
		}
	}

	return nil
}

func RetrieveIPs(dataStorage *storage.GenericStorage) ([]string, error) {
	result := make(map[string]struct{}, 0)

	eventsPerIpsKeys, err := dataStorage.GetKeys(FAILED_EVENTS_TABLE, "*")

	if err != nil {
		log.Println(err.Error())
		return make([]string, 0), err
	}

	for _, key := range eventsPerIpsKeys {
		ip := strings.ReplaceAll(key, FAILED_EVENTS_TABLE+"/", "")
		result[ip] = struct{}{}
	}

	var resultArr []string
	for ip, _ := range result {
		resultArr = append(resultArr, ip)
	}

	return resultArr, nil
}

func RetrieveEventsForIP(dataStorage *storage.GenericStorage, ip string) (map[string]*Event, error) {
	var result map[string]*Event

	res, err := dataStorage.GetKey(FAILED_IPS_TABLE, ip)

	if err != nil {
		return result, err
	}

	var events map[string]*Event

	err = dataStorage.GetMap(FAILED_EVENTS_TABLE, ip, &events)

	if err != nil || res == "" {
		return result, err
	} else {
		return events, err
	}
}
