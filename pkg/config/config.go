package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"kafka_client/pkg/config/entity"
	"log"
)

func LoadConfigSettingsByPath(path string) (configSettings entity.Settings, err error) {
	environment := "local"
	path = fmt.Sprintf("%v/%v.json", path, environment)
	byteValue, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal(err)
	}
	err = json.Unmarshal(byteValue, &configSettings)
	if err != nil {
		log.Fatal(err)
	}
	return
}
