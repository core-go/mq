package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"log"
	"reflect"
	"strconv"
	"time"
)

type ClientConfig struct {
	ClientID *string `yaml:"client_id" mapstructure:"client_id" json:"clientId,omitempty" gorm:"column:clientid" bson:"clientId,omitempty" dynamodbav:"clientId,omitempty" firestore:"clientId,omitempty"`
	Username *string `yaml:"username" mapstructure:"username" json:"username,omitempty" gorm:"column:username" bson:"username,omitempty" dynamodbav:"username,omitempty" firestore:"username,omitempty"`
	Password *string `yaml:"password" mapstructure:"password" json:"password,omitempty" gorm:"column:password" bson:"password,omitempty" dynamodbav:"password,omitempty" firestore:"password,omitempty"`

	Algorithm string `yaml:"algorithm" mapstructure:"algorithm" json:"algorithm,omitempty" gorm:"column:algorithm" bson:"algorithm,omitempty" dynamodbav:"algorithm,omitempty" firestore:"algorithm,omitempty"`

	SASLEnable    *bool `yaml:"sasl_enable" mapstructure:"sasl_enable" json:"saslEnable,omitempty" gorm:"column:saslenable" bson:"saslEnable,omitempty" dynamodbav:"saslEnable,omitempty" firestore:"saslEnable,omitempty"`
	SASLHandshake *bool `yaml:"sasl_handshake" mapstructure:"sasl_handshake" json:"saslHandshake,omitempty" gorm:"column:saslhandshake" bson:"saslHandshake,omitempty" dynamodbav:"saslHandshake,omitempty" firestore:"saslHandshake,omitempty"`
	MetadataFull  *bool `yaml:"metadata_full" mapstructure:"metadata_full" json:"metadataFull,omitempty" gorm:"column:metadatafull" bson:"metadataFull,omitempty" dynamodbav:"metadataFull,omitempty" firestore:"metadataFull,omitempty"`

	TLSEnable *bool      `yaml:"tls_enable" mapstructure:"tls_enable" json:"tlsEnable,omitempty" gorm:"column:tlsenable" bson:"tlsEnable,omitempty" dynamodbav:"tlsEnable,omitempty" firestore:"tlsEnable,omitempty"`
	TLS       *TLSConfig `yaml:"tls" mapstructure:"tls" json:"tls,omitempty" gorm:"column:tls" bson:"tls,omitempty" dynamodbav:"tls,omitempty" firestore:"tls,omitempty"`

	Version *sarama.KafkaVersion `yaml:"version" mapstructure:"version" json:"version,omitempty" gorm:"column:version" bson:"version,omitempty" dynamodbav:"version,omitempty" firestore:"version,omitempty"`
	Retry   *RetryConfig         `yaml:"retry" mapstructure:"retry" json:"retry,omitempty" gorm:"column:retry" bson:"retry,omitempty" dynamodbav:"retry,omitempty" firestore:"retry,omitempty"`
}

type TLSConfig struct {
	InsecureSkipVerify *bool  `yaml:"insecure_skip_verify" mapstructure:"insecure_skip_verify" json:"insecureSkipVerify,omitempty" gorm:"column:insecureskipverify" bson:"insecureSkipVerify,omitempty" dynamodbav:"insecureSkipVerify,omitempty" firestore:"insecureSkipVerify,omitempty"` // the same with TLSSkipVerify ?
	CertFile           string `yaml:"cert_file" mapstructure:"cert_file" json:"certFile,omitempty" gorm:"column:certFile" bson:"certFile,omitempty" dynamodbav:"certFile,omitempty" firestore:"certFile,omitempty"`
	KeyFile            string `yaml:"key_file" mapstructure:"key_file" json:"keyFile,omitempty" gorm:"column:keyfile" bson:"keyFile,omitempty" dynamodbav:"keyFile,omitempty" firestore:"keyFile,omitempty"`
	CaFile             string `yaml:"ca_file" mapstructure:"ca_file" json:"caFile,omitempty" gorm:"column:cafile" bson:"caFile,omitempty" dynamodbav:"caFile,omitempty" firestore:"caFile,omitempty"`
}
type RetryConfig struct {
	Retry1 int64 `yaml:"1" mapstructure:"1" json:"retry1,omitempty" gorm:"column:retry1" bson:"retry1,omitempty" dynamodbav:"retry1,omitempty" firestore:"retry1,omitempty"`
	Retry2 int64 `yaml:"2" mapstructure:"2" json:"retry2,omitempty" gorm:"column:retry2" bson:"retry2,omitempty" dynamodbav:"retry2,omitempty" firestore:"retry2,omitempty"`
	Retry3 int64 `yaml:"3" mapstructure:"3" json:"retry3,omitempty" gorm:"column:retry3" bson:"retry3,omitempty" dynamodbav:"retry3,omitempty" firestore:"retry3,omitempty"`
	Retry4 int64 `yaml:"4" mapstructure:"4" json:"retry4,omitempty" gorm:"column:retry4" bson:"retry4,omitempty" dynamodbav:"retry4,omitempty" firestore:"retry4,omitempty"`
	Retry5 int64 `yaml:"5" mapstructure:"5" json:"retry5,omitempty" gorm:"column:retry5" bson:"retry5,omitempty" dynamodbav:"retry5,omitempty" firestore:"retry5,omitempty"`
	Retry6 int64 `yaml:"6" mapstructure:"6" json:"retry6,omitempty" gorm:"column:retry6" bson:"retry6,omitempty" dynamodbav:"retry6,omitempty" firestore:"retry6,omitempty"`
	Retry7 int64 `yaml:"7" mapstructure:"7" json:"retry7,omitempty" gorm:"column:retry7" bson:"retry7,omitempty" dynamodbav:"retry7,omitempty" firestore:"retry7,omitempty"`
	Retry8 int64 `yaml:"8" mapstructure:"8" json:"retry8,omitempty" gorm:"column:retry8" bson:"retry8,omitempty" dynamodbav:"retry8,omitempty" firestore:"retry8,omitempty"`
	Retry9 int64 `yaml:"9" mapstructure:"9" json:"retry9,omitempty" gorm:"column:retry9" bson:"retry9,omitempty" dynamodbav:"retry9,omitempty" firestore:"retry9,omitempty"`
}

func MakeDurations(vs []int64) []time.Duration {
	durations := make([]time.Duration, 0)
	for _, v := range vs {
		d := time.Duration(v) * time.Second
		durations = append(durations, d)
	}
	return durations
}
func MakeArray(v interface{}, prefix string, max int) []int64 {
	var ar []int64
	v2 := reflect.Indirect(reflect.ValueOf(v))
	for i := 1; i <= max; i++ {
		fn := prefix + strconv.Itoa(i)
		v3 := v2.FieldByName(fn).Interface().(int64)
		if v3 > 0 {
			ar = append(ar, v3)
		} else {
			return ar
		}
	}
	return ar
}
func DurationsFromValue(v interface{}, prefix string, max int) []time.Duration {
	arr := MakeArray(v, prefix, max)
	return MakeDurations(arr)
}
func Retry(sleeps []time.Duration, f func() error) (err error) {
	attempts := len(sleeps)
	for i := 0; ; i++ {
		log.Printf("Retrying %d of %d ", i+1, attempts)
		err = f()
		if err == nil {
			return
		}
		if i >= (attempts - 1) {
			break
		}
		time.Sleep(sleeps[i])
		log.Printf("Retrying %d of %d after error: %s", i+1, attempts, err.Error())
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}
