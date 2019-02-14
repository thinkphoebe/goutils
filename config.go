package goutils

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"

	"github.com/imdario/mergo"
	log "github.com/thinkphoebe/golog"
)

const configFileSizeLimit = 1 * 1024 * 1024 //1M

func LoadConfig(path string, pCfgObj interface{}) error {
	cfgFile, err := os.Open(path)
	if err != nil {
		return errors.New("FAILED to open config file: " + path)
	}

	fi, _ := cfgFile.Stat()
	if size := fi.Size(); size > (configFileSizeLimit) {
		return errors.New("config file too large: " + strconv.FormatInt(size, 10))
	}
	if fi.Size() == 0 {
		return errors.New("config file is empty")
	}

	buf := make([]byte, fi.Size())
	_, err = cfgFile.Read(buf)

	buf, err = stripComments(buf)
	if err != nil {
		return err
	}

	buf = []byte(os.ExpandEnv(string(buf)))
	err = json.Unmarshal(buf, pCfgObj)
	return err
}

func PrintConfig(name string, pCfgObj interface{}) {
	data, err := json.MarshalIndent(pCfgObj, "", "    ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "config [%s] json.Marshal FAILED! err [%v] [%v]\n", name, err, pCfgObj)
	} else {
		fmt.Fprintf(os.Stderr, "config [%s] ======>%s\n", name, data)
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

// Example:
//     type TestSubConfig struct {
//         CfgTest string
//     }
//     type TestConfig struct {
//         SubCfg  TestSubConfig
//         StrCfg  *string
//         IntCfg  *int
//         BoolCfg *bool
//     }
//     var cfgObj TestConfig{}
//     goutils.LoadConfigWithDefault(userPath, defaultPath, &cfgObj)
//
// 说明：
//     由于int、bool、string等变量无法区分空值和零值，建议将这类变量定义为指针
//     struct/interface定义为指针时会彻底覆盖，非指针时合并，即逐项覆盖src各个非空的项。一般期望合并，即struct不应定义为指针
//     map定义为指针时会彻底覆盖，非指针时会合并，合并类似于Python dict的update
//     slice定义为指针且mergo.Merge添加mergo.WithAppendSlice参数时会合并。一般期望覆盖，因此以下未添加。所以slice不必定义为指针
func LoadConfigWithDefault(userPath string, defaultPath string, pCfgObj interface{}) error {
	if userPath == "" || defaultPath == "" {
		return errors.New("no userPath or defaultPath provided")
	}

	t := reflect.TypeOf(pCfgObj).Elem()
	pCfgUser := reflect.New(t).Interface()

	if userPath != "" {
		if err := LoadConfig(userPath, &pCfgUser); err != nil {
			log.Errorf("[config] load userPath config [%s] FAILED! err [%v]", userPath, err)
			return err
		}
		PrintConfig("userPath", &pCfgUser)
	}

	if defaultPath != "" {
		if err := LoadConfig(defaultPath, pCfgObj); err != nil {
			log.Errorf("[config] load default config [%s] FAILED! err [%v]", defaultPath, err)
			return err
		}
		PrintConfig("default", pCfgObj)

		if err := mergo.Merge(pCfgObj, pCfgUser, mergo.WithOverride); err != nil {
			log.Errorf("[config] mergo.Merge FAILED! err [%v]", err)
			return err
		}
		PrintConfig("final", pCfgObj)
	}

	return nil
}
