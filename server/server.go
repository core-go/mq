package server

import (
	"crypto/tls"
	"log"
	"net/http"
	"strconv"
	"time"
)

type ServerConfig struct {
	Name              string         `yaml:"name" mapstructure:"name" json:"name,omitempty" gorm:"column:name" bson:"name,omitempty" dynamodbav:"name,omitempty" firestore:"name,omitempty"`
	Version           string         `yaml:"version" mapstructure:"version" json:"version,omitempty" gorm:"column:version" bson:"version,omitempty" dynamodbav:"version,omitempty" firestore:"version,omitempty"`
	Port              *int64         `yaml:"port" mapstructure:"port" json:"port,omitempty" gorm:"column:port" bson:"port,omitempty" dynamodbav:"port,omitempty" firestore:"port,omitempty"`
	WriteTimeout      *time.Duration `yaml:"write_timeout" mapstructure:"write_timeout" json:"writeTimeout,omitempty" gorm:"column:writetimeout" bson:"writeTimeout,omitempty" dynamodbav:"writeTimeout,omitempty" firestore:"writeTimeout,omitempty"`
	ReadTimeout       *time.Duration `yaml:"read_timeout" mapstructure:"read_timeout" json:"readTimeout,omitempty" gorm:"column:readtimeout" bson:"readTimeout,omitempty" dynamodbav:"readTimeout,omitempty" firestore:"readTimeout,omitempty"`
	ReadHeaderTimeout *time.Duration `yaml:"read_header_timeout" mapstructure:"read_header_timeout" json:"readHeaderTimeout,omitempty" gorm:"column:readheadertimeout" bson:"readHeaderTimeout,omitempty" dynamodbav:"readHeaderTimeout,omitempty" firestore:"readHeaderTimeout,omitempty"`
	IdleTimeout       *time.Duration `yaml:"idle_timeout" mapstructure:"idle_timeout" json:"idleTimeout,omitempty" gorm:"column:idletimeout" bson:"idleTimeout,omitempty" dynamodbav:"idleTimeout,omitempty" firestore:"idleTimeout,omitempty"`
	MaxHeaderBytes    *int           `yaml:"max_header_bytes" mapstructure:"max_header_bytes" json:"maxHeaderBytes,omitempty" gorm:"column:maxheaderbytes" bson:"maxHeaderBytes,omitempty" dynamodbav:"maxHeaderBytes,omitempty" firestore:"maxHeaderBytes,omitempty"`
	Cert              string         `yaml:"cert" mapstructure:"cert" json:"cert,omitempty" gorm:"column:cert" bson:"cert,omitempty" dynamodbav:"cert,omitempty" firestore:"cert,omitempty"`
	Key               string         `yaml:"key" mapstructure:"key" json:"key,omitempty" gorm:"column:key" bson:"key,omitempty" dynamodbav:"key,omitempty" firestore:"key,omitempty"`
}

func Addr(port *int64) string {
	server := ""
	if port != nil && *port >= 0 {
		server = ":" + strconv.FormatInt(*port, 10)
	}
	return server
}
func ServerInfo(cfg ServerConfig) string {
	if len(cfg.Version) > 0 {
		if cfg.Port != nil && *cfg.Port >= 0 {
			return "Start service: " + cfg.Name + " at port " + strconv.FormatInt(*cfg.Port, 10) + " with version " + cfg.Version
		} else {
			return "Start service: " + cfg.Name + " with version " + cfg.Version
		}
	} else {
		if cfg.Port != nil && *cfg.Port >= 0 {
			return "Start service: " + cfg.Name + " at port " + strconv.FormatInt(*cfg.Port, 10)
		} else {
			return "Start service: " + cfg.Name
		}
	}
}
func Serve(cfg ServerConfig, check func(w http.ResponseWriter, r *http.Request), options ...*tls.Config) {
	log.Println(ServerInfo(cfg))
	http.HandleFunc("/health", check)
	http.HandleFunc("/", check)
	srv := CreateServer(cfg, nil, options...)
	err := srv.ListenAndServe()
	if err != nil {
		log.Println(err.Error())
		panic(err)
	}
}
func CreateServer(cfg ServerConfig, handler http.Handler, options ...*tls.Config) *http.Server {
	addr := Addr(cfg.Port)
	srv := http.Server{
		Addr:      addr,
		Handler:   nil,
		TLSConfig: nil,
	}
	if len(options) > 0 && options[0] != nil {
		srv.TLSConfig = options[0]
	}
	if cfg.ReadTimeout != nil {
		srv.ReadTimeout = *cfg.ReadTimeout
	}
	if cfg.ReadHeaderTimeout != nil {
		srv.ReadHeaderTimeout = *cfg.ReadHeaderTimeout
	}
	if cfg.WriteTimeout != nil {
		srv.WriteTimeout = *cfg.WriteTimeout
	}
	if cfg.IdleTimeout != nil {
		srv.IdleTimeout = *cfg.IdleTimeout
	}
	if cfg.MaxHeaderBytes != nil && *cfg.MaxHeaderBytes > 0 {
		srv.MaxHeaderBytes = *cfg.MaxHeaderBytes
	}
	srv.Handler = handler
	return &srv
}
