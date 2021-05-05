package pubsub

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"google.golang.org/api/option"
	"log"
	"os"
	"reflect"
	"strconv"
	"time"
)

func NewPubSubClientWithRetries(ctx context.Context, projectId string, credentials string, retries []time.Duration) (*pubsub.Client, error) {
	if len(credentials) > 0 {
		c, er1 := pubsub.NewClient(ctx, projectId, option.WithCredentialsJSON([]byte(credentials)))
		if er1 == nil {
			return c, er1
		}
		i := 0
		err := Retry(retries, func() (err error) {
			i = i + 1
			c2, er2 := pubsub.NewClient(ctx, projectId, option.WithCredentialsJSON([]byte(credentials)))
			if er2 == nil {
				c = c2
			}
			return er2
		})
		if err != nil {
			log.Printf("Failed to new pubsub client: %s.", err.Error())
		}
		return c, err
	} else {
		log.Println("empty credentials")
		return pubsub.NewClient(ctx, projectId)
	}
}
func NewPubSubClientWithFile(ctx context.Context, projectId string, keyFilename string) (*pubsub.Client, error) {
	if len(keyFilename) > 0 && existFile(keyFilename) {
		log.Println("key file exists")
		return pubsub.NewClient(ctx, projectId, option.WithCredentialsFile(keyFilename))
	} else {
		log.Println("key file doesn't exists")
		return pubsub.NewClient(ctx, projectId)
	}
}
func NewPubSubClient(ctx context.Context, projectId string, credentials string) (*pubsub.Client, error) {
	if len(credentials) > 0 {
		return pubsub.NewClient(ctx, projectId, option.WithCredentialsJSON([]byte(credentials)))
	} else {
		log.Println("empty credentials")
		return pubsub.NewClient(ctx, projectId)
	}
}

func existFile(filename string) bool {
	if _, err := os.Stat(filename); err == nil {
		return true
	} else if os.IsNotExist(err) {
		return false
	} else {
		log.Println(err.Error())
	}
	return false
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
type RetryConfig struct {
	Retry1  int64 `mapstructure:"1" json:"retry1,omitempty" gorm:"column:retry1" bson:"retry1,omitempty" dynamodbav:"retry1,omitempty" firestore:"retry1,omitempty"`
	Retry2  int64 `mapstructure:"2" json:"retry2,omitempty" gorm:"column:retry2" bson:"retry2,omitempty" dynamodbav:"retry2,omitempty" firestore:"retry2,omitempty"`
	Retry3  int64 `mapstructure:"3" json:"retry3,omitempty" gorm:"column:retry3" bson:"retry3,omitempty" dynamodbav:"retry3,omitempty" firestore:"retry3,omitempty"`
	Retry4  int64 `mapstructure:"4" json:"retry4,omitempty" gorm:"column:retry4" bson:"retry4,omitempty" dynamodbav:"retry4,omitempty" firestore:"retry4,omitempty"`
	Retry5  int64 `mapstructure:"5" json:"retry5,omitempty" gorm:"column:retry5" bson:"retry5,omitempty" dynamodbav:"retry5,omitempty" firestore:"retry5,omitempty"`
	Retry6  int64 `mapstructure:"6" json:"retry6,omitempty" gorm:"column:retry6" bson:"retry6,omitempty" dynamodbav:"retry6,omitempty" firestore:"retry6,omitempty"`
	Retry7  int64 `mapstructure:"7" json:"retry7,omitempty" gorm:"column:retry7" bson:"retry7,omitempty" dynamodbav:"retry7,omitempty" firestore:"retry7,omitempty"`
	Retry8  int64 `mapstructure:"8" json:"retry8,omitempty" gorm:"column:retry8" bson:"retry8,omitempty" dynamodbav:"retry8,omitempty" firestore:"retry8,omitempty"`
	Retry9  int64 `mapstructure:"9" json:"retry9,omitempty" gorm:"column:retry9" bson:"retry9,omitempty" dynamodbav:"retry9,omitempty" firestore:"retry9,omitempty"`
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
