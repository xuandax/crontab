package master

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	ApiPort               int      `json:"apiPort"`
	WriteTimeout          int      `json:"writeTimeout"`
	ReadTimeout           int      `json:"readTimeout"`
	EtcdEndPoints         []string `json:"etcdEndPoints"`
	EtcdDialTimeout       int      `json:"etcdDialTimeout"`
	WebRoot               string   `json:"webroot"`
	MongodbUri            string   `json:"mongodbUri"`
	MongodbConnectTimeout int      `json:"mongodbConnectTimeout"`
}

var (
	G_config *Config
)

func InitConfig(filename string) (err error) {
	var (
		contents []byte
		config   *Config
	)
	//读取文件
	if contents, err = ioutil.ReadFile(filename); err != nil {
		return
	}

	//反序列化json
	if err = json.Unmarshal(contents, &config); err != nil {
		return
	}

	//赋值单例
	G_config = config
	return
}
