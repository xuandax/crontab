package worker

import (
	"encoding/json"
	"io/ioutil"
)

var (
	G_config *Config
)

type Config struct {
	EtcdEndPoints         []string `json:"etcdEndPoints"`
	EtcdDialTimeout       int      `json:"etcdDialTimeout"`
	MongodbUri            string   `json:"mongodbUri"`
	MongodbConnectTimeout int      `json:"mongodbConnectTimeout"`
	LogChanLength         int      `json:"logChanLength"`
	LogBatchLength        int      `json:"logBatchLength"`
	LogCommitTime         int      `json:"logCommitTime"`
}

func InitConfig(fileName string) (err error) {
	var (
		file   []byte
		config *Config
	)

	if file, err = ioutil.ReadFile(fileName); err != nil {
		return
	}

	if err = json.Unmarshal(file, &config); err != nil {
		return
	}

	G_config = config
	return
}
