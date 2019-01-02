package goutils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
)

const configFileSizeLimit = 1 * 1024 * 1024 //1M

func LoadConfig(path string, cfg_obj interface{}) error {
	config_file, err := os.Open(path)
	if err != nil {
		return errors.New("FAILED to open config file: " + path)
	}

	fi, _ := config_file.Stat()
	if size := fi.Size(); size > (configFileSizeLimit) {
		return errors.New("config file too large: " + strconv.FormatInt(size, 10))
	}
	if fi.Size() == 0 {
		return errors.New("config file is empty")
	}

	buffer := make([]byte, fi.Size())
	_, err = config_file.Read(buffer)

	buffer, err = stripComments(buffer)
	if err != nil {
		return err
	}

	buffer = []byte(os.ExpandEnv(string(buffer)))
	err = json.Unmarshal(buffer, cfg_obj)
	return err
}

func PrintConfig(name string, cfg_obj interface{}) {
	data, err := json.MarshalIndent(cfg_obj, "", "    ")
	if err != nil {
		fmt.Printf("config [%s] json.Marshal FAILED! err [%v] [%v]\n", name, err, cfg_obj)
	} else {
		fmt.Printf("config [%s] ======>%s\n", name, data)
	}
}

func stripComments(data []byte) ([]byte, error) {
	data = bytes.Replace(data, []byte("\r"), []byte(""), 0) // Windows
	lines := bytes.Split(data, []byte("\n"))
	filtered := make([][]byte, 0)

	for _, line := range lines {
		match, err := regexp.Match(`^\s*/`, line)
		if err != nil {
			return nil, err
		}
		if !match {
			filtered = append(filtered, line)
		}
	}

	return bytes.Join(filtered, []byte("\n")), nil
}
