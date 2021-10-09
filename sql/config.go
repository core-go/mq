package sql

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"time"
)

type Config struct {
	MultiStatements bool        `mapstructure:"multi_statements" json:"multiStatements,omitempty" gorm:"column:multistatements" bson:"multiStatements,omitempty" dynamodbav:"multiStatements,omitempty" firestore:"multiStatements,omitempty"`
	DataSourceName  string      `mapstructure:"data_source_name" json:"dataSourceName,omitempty" gorm:"column:datasourcename" bson:"dataSourceName,omitempty" dynamodbav:"dataSourceName,omitempty" firestore:"dataSourceName,omitempty"`
	Driver          string      `mapstructure:"driver" json:"driver,omitempty" gorm:"column:driver" bson:"driver,omitempty" dynamodbav:"driver,omitempty" firestore:"driver,omitempty"`
	Host            string      `mapstructure:"host" json:"host,omitempty" gorm:"column:host" bson:"host,omitempty" dynamodbav:"host,omitempty" firestore:"host,omitempty"`
	Port            int         `mapstructure:"port" json:"port,omitempty" gorm:"column:port" bson:"port,omitempty" dynamodbav:"port,omitempty" firestore:"port,omitempty"`
	Database        string      `mapstructure:"database" json:"database,omitempty" gorm:"column:database" bson:"database,omitempty" dynamodbav:"database,omitempty" firestore:"database,omitempty"`
	User            string      `mapstructure:"user" json:"user,omitempty" gorm:"column:user" bson:"user,omitempty" dynamodbav:"user,omitempty" firestore:"user,omitempty"`
	Password        string      `mapstructure:"password" json:"password,omitempty" gorm:"column:password" bson:"password,omitempty" dynamodbav:"password,omitempty" firestore:"password,omitempty"`
	ConnMaxLifetime int64       `mapstructure:"conn_max_lifetime" json:"connMaxLifetime,omitempty" gorm:"column:connmaxlifetime" bson:"connMaxLifetime,omitempty" dynamodbav:"connMaxLifetime,omitempty" firestore:"connMaxLifetime,omitempty"`
	MaxIdleConns    int         `mapstructure:"max_idle_conns" json:"maxIdleConns,omitempty" gorm:"column:maxidleconns" bson:"maxIdleConns,omitempty" dynamodbav:"maxIdleConns,omitempty" firestore:"maxIdleConns,omitempty"`
	MaxOpenConns    int         `mapstructure:"max_open_conns" json:"maxOpenConns,omitempty" gorm:"column:maxopenconns" bson:"maxOpenConns,omitempty" dynamodbav:"maxOpenConns,omitempty" firestore:"maxOpenConns,omitempty"`
	Retry           RetryConfig `mapstructure:"retry" json:"retry,omitempty" gorm:"column:retry" bson:"retry,omitempty" dynamodbav:"retry,omitempty" firestore:"retry,omitempty"`
	Mock            bool        `mapstructure:"mock" json:"mock,omitempty" gorm:"column:mock" bson:"mock,omitempty" dynamodbav:"mock,omitempty" firestore:"mock,omitempty"`
	Log             bool        `mapstructure:"log" json:"log,omitempty" gorm:"column:log" bson:"log,omitempty" dynamodbav:"log,omitempty" firestore:"log,omitempty"`
}
type RetryConfig struct {
	Retry1 int64 `mapstructure:"1" json:"retry1,omitempty" gorm:"column:retry1" bson:"retry1,omitempty" dynamodbav:"retry1,omitempty" firestore:"retry1,omitempty"`
	Retry2 int64 `mapstructure:"2" json:"retry2,omitempty" gorm:"column:retry2" bson:"retry2,omitempty" dynamodbav:"retry2,omitempty" firestore:"retry2,omitempty"`
	Retry3 int64 `mapstructure:"3" json:"retry3,omitempty" gorm:"column:retry3" bson:"retry3,omitempty" dynamodbav:"retry3,omitempty" firestore:"retry3,omitempty"`
	Retry4 int64 `mapstructure:"4" json:"retry4,omitempty" gorm:"column:retry4" bson:"retry4,omitempty" dynamodbav:"retry4,omitempty" firestore:"retry4,omitempty"`
	Retry5 int64 `mapstructure:"5" json:"retry5,omitempty" gorm:"column:retry5" bson:"retry5,omitempty" dynamodbav:"retry5,omitempty" firestore:"retry5,omitempty"`
	Retry6 int64 `mapstructure:"6" json:"retry6,omitempty" gorm:"column:retry6" bson:"retry6,omitempty" dynamodbav:"retry6,omitempty" firestore:"retry6,omitempty"`
	Retry7 int64 `mapstructure:"7" json:"retry7,omitempty" gorm:"column:retry7" bson:"retry7,omitempty" dynamodbav:"retry7,omitempty" firestore:"retry7,omitempty"`
	Retry8 int64 `mapstructure:"8" json:"retry8,omitempty" gorm:"column:retry8" bson:"retry8,omitempty" dynamodbav:"retry8,omitempty" firestore:"retry8,omitempty"`
	Retry9 int64 `mapstructure:"9" json:"retry9,omitempty" gorm:"column:retry9" bson:"retry9,omitempty" dynamodbav:"retry9,omitempty" firestore:"retry9,omitempty"`
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
func open(c Config) (*sql.DB, error) {
	dsn := c.DataSourceName
	if len(dsn) == 0 {
		dsn = BuildDataSourceName(c)
	}
	db, err := sql.Open(c.Driver, dsn)
	if err != nil {
		return db, err
	}
	if c.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(time.Duration(c.ConnMaxLifetime) * time.Second)
	}
	if c.MaxIdleConns > 0 {
		db.SetMaxIdleConns(c.MaxIdleConns)
	}
	if c.MaxOpenConns > 0 {
		db.SetMaxOpenConns(c.MaxOpenConns)
	}
	return db, err
}
func Open(c Config, retries ...time.Duration) (*sql.DB, error) {
	if c.Mock {
		return nil, nil
	}
	if len(retries) == 0 {
		return open(c)
	} else {
		db, er1 := open(c)
		if er1 == nil {
			return db, er1
		}
		i := 0
		err := Retry(retries, func() (err error) {
			i = i + 1
			db2, er2 := open(c)
			if er2 == nil {
				db = db2
			}
			return er2
		})
		if err != nil {
			log.Printf("Cannot conect to database: %s.", err.Error())
		}
		return db, err
	}
}
func BuildDataSourceName(c Config) string {
	if c.Driver == "postgres" {
		uri := fmt.Sprintf("user=%s dbname=%s password=%s host=%s port=%d sslmode=disable", c.User, c.Database, c.Password, c.Host, c.Port)
		return uri
	} else if c.Driver == "mysql" {
		uri := ""
		if c.MultiStatements {
			uri = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local&multiStatements=True", c.User, c.Password, c.Host, c.Port, c.Database)
			return uri
		}
		uri = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local", c.User, c.Password, c.Host, c.Port, c.Database)
		return uri
	} else if c.Driver == "mssql" { // mssql
		uri := fmt.Sprintf("sqlserver://%s:%s@%s:%d?Database=%s", c.User, c.Password, c.Host, c.Port, c.Database)
		return uri
	} else if c.Driver == "godror" || c.Driver == "oracle" {
		return fmt.Sprintf("user=\"%s\" password=\"%s\" connectString=\"%s:%d/%s\"", c.User, c.Password, c.Host, c.Port, c.Database)
	} else { //sqlite
		return c.Host // return sql.Open("sqlite3", c.Host)
	}
}
