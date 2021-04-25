package mq

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"time"
)

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
	Retry10 int64 `mapstructure:"10" json:"retry10,omitempty" gorm:"column:retry10" bson:"retry10,omitempty" dynamodbav:"retry10,omitempty" firestore:"retry10,omitempty"`
	Retry11 int64 `mapstructure:"11" json:"retry11,omitempty" gorm:"column:retry11" bson:"retry11,omitempty" dynamodbav:"retry11,omitempty" firestore:"retry11,omitempty"`
	Retry12 int64 `mapstructure:"12" json:"retry12,omitempty" gorm:"column:retry12" bson:"retry12,omitempty" dynamodbav:"retry12,omitempty" firestore:"retry12,omitempty"`
	Retry13 int64 `mapstructure:"13" json:"retry13,omitempty" gorm:"column:retry13" bson:"retry13,omitempty" dynamodbav:"retry13,omitempty" firestore:"retry13,omitempty"`
	Retry14 int64 `mapstructure:"14" json:"retry14,omitempty" gorm:"column:retry14" bson:"retry14,omitempty" dynamodbav:"retry14,omitempty" firestore:"retry14,omitempty"`
	Retry15 int64 `mapstructure:"15" json:"retry15,omitempty" gorm:"column:retry15" bson:"retry15,omitempty" dynamodbav:"retry15,omitempty" firestore:"retry15,omitempty"`
	Retry16 int64 `mapstructure:"16" json:"retry16,omitempty" gorm:"column:retry16" bson:"retry16,omitempty" dynamodbav:"retry16,omitempty" firestore:"retry16,omitempty"`
	Retry17 int64 `mapstructure:"17" json:"retry17,omitempty" gorm:"column:retry17" bson:"retry17,omitempty" dynamodbav:"retry17,omitempty" firestore:"retry17,omitempty"`
	Retry18 int64 `mapstructure:"18" json:"retry18,omitempty" gorm:"column:retry18" bson:"retry18,omitempty" dynamodbav:"retry18,omitempty" firestore:"retry18,omitempty"`
	Retry19 int64 `mapstructure:"19" json:"retry19,omitempty" gorm:"column:retry19" bson:"retry19,omitempty" dynamodbav:"retry19,omitempty" firestore:"retry19,omitempty"`
	Retry20 int64 `mapstructure:"20" json:"retry20,omitempty" gorm:"column:retry20" bson:"retry20,omitempty" dynamodbav:"retry20,omitempty" firestore:"retry20,omitempty"`
}
func DurationsFromValue(v interface{}, prefix string, max int) []time.Duration {
	arr := MakeArray(v, prefix, max)
	return MakeDurations(arr)
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

// Retry Copy this code from https://stackoverflow.com/questions/47606761/repeat-code-if-an-error-occured
func Retry(ctx context.Context, sleeps []time.Duration, f func() error, log func(context.Context, string)) (err error) {
	attempts := len(sleeps)
	for i := 0; ; i++ {
		err = f()
		if err == nil {
			return
		}
		if i >= (attempts - 1) {
			break
		}
		if log != nil {
			log(ctx, fmt.Sprintf("Retrying %d of %d after error: %s", i+1, attempts, err.Error()))
		}
		time.Sleep(sleeps[i])
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}
