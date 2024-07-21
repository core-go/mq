package server

import (
	"crypto/tls"
	"log"
	"net/http"
	"strconv"
	"time"
)

type ServerConfig struct {
	Name              string `mapstructure:"name" json:"name,omitempty" gorm:"column:name" bson:"name,omitempty" dynamodbav:"name,omitempty" firestore:"name,omitempty"`
	Version           string `mapstructure:"version" json:"version,omitempty" gorm:"column:version" bson:"version,omitempty" dynamodbav:"version,omitempty" firestore:"version,omitempty"`
	Port              *int64 `mapstructure:"port" json:"port,omitempty" gorm:"column:port" bson:"port,omitempty" dynamodbav:"port,omitempty" firestore:"port,omitempty"`
	WriteTimeout      *int64 `mapstructure:"write_timeout" json:"writeTimeout,omitempty" gorm:"column:writetimeout" bson:"writeTimeout,omitempty" dynamodbav:"writeTimeout,omitempty" firestore:"writeTimeout,omitempty"`
	ReadTimeout       *int64 `mapstructure:"read_timeout" json:"readTimeout,omitempty" gorm:"column:readtimeout" bson:"readTimeout,omitempty" dynamodbav:"readTimeout,omitempty" firestore:"readTimeout,omitempty"`
	ReadHeaderTimeout *int64 `mapstructure:"read_header_timeout" json:"readHeaderTimeout,omitempty" gorm:"column:readheadertimeout" bson:"readHeaderTimeout,omitempty" dynamodbav:"readHeaderTimeout,omitempty" firestore:"readHeaderTimeout,omitempty"`
	IdleTimeout       *int64 `mapstructure:"idle_timeout" json:"idleTimeout,omitempty" gorm:"column:idletimeout" bson:"idleTimeout,omitempty" dynamodbav:"idleTimeout,omitempty" firestore:"idleTimeout,omitempty"`
	MaxHeaderBytes    *int   `mapstructure:"max_header_bytes" json:"maxHeaderBytes,omitempty" gorm:"column:maxheaderbytes" bson:"maxHeaderBytes,omitempty" dynamodbav:"maxHeaderBytes,omitempty" firestore:"maxHeaderBytes,omitempty"`
}

func Addr(port *int64) string {
	server := ""
	if port != nil && *port >= 0 {
		server = ":" + strconv.FormatInt(*port, 10)
	}
	return server
}
func ServerInfo(conf ServerConfig) string {
	if len(conf.Version) > 0 {
		if conf.Port != nil && *conf.Port >= 0 {
			return "Start service: " + conf.Name + " at port " + strconv.FormatInt(*conf.Port, 10) + " with version " + conf.Version
		} else {
			return "Start service: " + conf.Name + " with version " + conf.Version
		}
	} else {
		if conf.Port != nil && *conf.Port >= 0 {
			return "Start service: " + conf.Name + " at port " + strconv.FormatInt(*conf.Port, 10)
		} else {
			return "Start service: " + conf.Name
		}
	}
}
func Serve(conf ServerConfig, check func(w http.ResponseWriter, r *http.Request), options ...*tls.Config) {
	log.Println(ServerInfo(conf))
	http.HandleFunc("/health", check)
	http.HandleFunc("/", check)
	srv := CreateServer(conf, nil, options...)
	err := srv.ListenAndServe()
	if err != nil {
		log.Println(err.Error())
		panic(err)
	}
}
func CreateServer(conf ServerConfig, handler http.Handler, options ...*tls.Config) *http.Server {
	addr := Addr(conf.Port)
	srv := http.Server{
		Addr:      addr,
		Handler:   nil,
		TLSConfig: nil,
	}
	if len(options) > 0 && options[0] != nil {
		srv.TLSConfig = options[0]
	}
	if conf.ReadTimeout != nil && *conf.ReadTimeout > 0 {
		srv.ReadTimeout = time.Duration(*conf.ReadTimeout) * time.Second
	}
	if conf.ReadHeaderTimeout != nil && *conf.ReadHeaderTimeout > 0 {
		srv.ReadHeaderTimeout = time.Duration(*conf.ReadHeaderTimeout) * time.Second
	}
	if conf.WriteTimeout != nil && *conf.WriteTimeout > 0 {
		srv.WriteTimeout = time.Duration(*conf.WriteTimeout) * time.Second
	}
	if conf.IdleTimeout != nil && *conf.IdleTimeout > 0 {
		srv.IdleTimeout = time.Duration(*conf.IdleTimeout) * time.Second
	}
	if conf.MaxHeaderBytes != nil && *conf.MaxHeaderBytes > 0 {
		srv.MaxHeaderBytes = *conf.MaxHeaderBytes
	}
	srv.Handler = handler
	return &srv
}
