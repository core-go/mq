package config

import (
	"fmt"
	"github.com/spf13/viper"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func Load(c interface{}, fileNames ...string) error {
	env := os.Getenv("ENV")
	if len(env) == 0 {
		env = os.Getenv("APP_ENV")
	}
	if len(env) == 0 {
		env = os.Getenv("ENVIRONMENT")
	}
	if len(env) == 0 {
		env = os.Getenv("STATE")
	}
	if len(env) == 0 {
		env = os.Getenv("APP_STATE")
	}
	return LoadConfigWithEnv("", "", env, c, fileNames...)
}
func LoadConfig(envName string, c interface{}, fileNames ...string) error {
	env := os.Getenv(envName)
	return LoadConfigWithEnv("", "", env, c, fileNames...)
}

// LoadConfigWithEnv function will read config from environment or config file.
func LoadConfigWithEnv(parentPath string, directory string, env string, c interface{}, fileNames ...string) error {
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	viper.SetConfigType("yaml")

	fileCount := len(fileNames)
	if fileCount > 0 {
		if len(parentPath) == 0 && len(directory) == 0 {
			viper.AddConfigPath("./")
		} else {
			viper.AddConfigPath("./" + directory + "/")
			if len(parentPath) > 0 {
				viper.AddConfigPath("./" + parentPath + "/" + directory + "/")
			}
		}

		viper.SetConfigName(fileNames[0])
		if er1 := viper.ReadInConfig(); er1 != nil {
			switch er1.(type) {
			case viper.ConfigFileNotFoundError:
				log.Println("config file not found")
			default:
				return er1
			}
		}

		for i := 1; i < fileCount; i++ {
			viper.SetConfigName(fileNames[i])
			if er2b := viper.MergeInConfig(); er2b != nil {
				switch er2b.(type) {
				case viper.ConfigFileNotFoundError:
					break
				default:
					return er2b
				}
			}
		}
	} else {
		return fmt.Errorf("have no config file")
	}

	if len(env) > 0 {
		env2 := strings.ToLower(env)
		for _, fileName2 := range fileNames {
			name0 := fileName2 + "." + env2
			viper.SetConfigName(name0)
			er2a := viper.MergeInConfig()
			if er2a != nil {
				switch er2a.(type) {
				case viper.ConfigFileNotFoundError:
					break
				default:
					return er2a
				}
			}
			name1 := fileName2 + "-" + env2
			viper.SetConfigName(name1)
			er2b := viper.MergeInConfig()
			if er2b != nil {
				switch er2b.(type) {
				case viper.ConfigFileNotFoundError:
					break
				default:
					return er2b
				}
			}
		}
	}
	er3 := BindEnvs(c)
	if er3 != nil {
		return er3
	}
	er4 := viper.Unmarshal(c)
	return er4
}

// BindEnvs function will bind ymal file to struc model
func BindEnvs(conf interface{}, parts ...string) error {
	ifv := reflect.Indirect(reflect.ValueOf(conf))
	ift := reflect.TypeOf(ifv)
	num := min(ift.NumField(), ifv.NumField())
	for i := 0; i < num; i++ {
		v := ifv.Field(i)
		t := ift.Field(i)
		tv, ok := t.Tag.Lookup("mapstructure")
		if !ok {
			continue
		}
		switch v.Kind() {
		case reflect.Struct:
			return BindEnvs(v.Interface(), append(parts, tv)...)
		default:
			return viper.BindEnv(strings.Join(append(parts, tv), "."))
		}
	}
	return nil
}

func min(n1 int, n2 int) int {
	if n1 < n2 {
		return n1
	}
	return n2
}

func LoadMapWithPath(parentPath string, directory string, env string, fileNames ...string) (map[string]string, error) {
	innerMap := make(map[string]string)
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	viper.SetConfigType("yaml")

	for _, fileName := range fileNames {
		viper.SetConfigName(fileName)
	}

	if len(parentPath) == 0 && len(directory) == 0 {
		viper.AddConfigPath("./")
	} else {
		viper.AddConfigPath("./" + directory + "/")
		if len(parentPath) > 0 {
			viper.AddConfigPath("./" + parentPath + "/" + directory + "/")
		}
	}

	if er1 := viper.ReadInConfig(); er1 != nil {
		switch er1.(type) {
		case viper.ConfigFileNotFoundError:
			log.Println("config file not found")
		default:
			return nil, er1
		}
	}
	if len(env) > 0 {
		env2 := strings.ToLower(env)
		for _, fileName2 := range fileNames {
			name0 := fileName2 + "." + env2
			viper.SetConfigName(name0)
			er2a := viper.MergeInConfig()
			if er2a != nil {
				switch er2a.(type) {
				case viper.ConfigFileNotFoundError:
					break
				default:
					return nil, er2a
				}
			}
			name1 := fileName2 + "-" + env2
			viper.SetConfigName(name1)
			er2b := viper.MergeInConfig()
			if er2b != nil {
				switch er2b.(type) {
				case viper.ConfigFileNotFoundError:
					break
				default:
					return nil, er2b
				}
			}
		}
	}
	er3 := viper.Unmarshal(&innerMap)
	return innerMap, er3
}
func LoadMapWithEnv(env string, fileNames ...string) (map[string]string, error) {
	return LoadMapWithPath("", "", env, fileNames...)
}
func LoadMap(fileNames ...string) (map[string]string, error) {
	env := os.Getenv("ENV")
	return LoadMapWithPath("", "", env, fileNames...)
}

func LoadFileWithPath(parentPath string, directory string, env string, filename string) ([]byte, error) {
	if len(directory) > 0 {
		if len(env) > 0 {
			indexDot := strings.LastIndex(filename, ".")
			if indexDot >= 0 {
				file := "./" + directory + "/" + filename[0:indexDot] + "-" + env + filename[indexDot:]
				if !fileExists(file) {
					file = "./" + parentPath + "/" + directory + "/" + filename[0:indexDot] + "-" + env + filename[indexDot:]
				}
				if fileExists(file) {
					return ioutil.ReadFile(file)
				}
			}
		}
		file := "./" + directory + "/" + filename
		if !fileExists(file) {
			file = "./" + parentPath + "/" + directory + "/" + filename
		}
		return ioutil.ReadFile(file)
	} else {
		if len(env) > 0 {
			indexDot := strings.LastIndex(filename, ".")
			if indexDot >= 0 {
				file := "./" + filename[0:indexDot] + "-" + env + filename[indexDot:]
				if !fileExists(file) {
					file = "./" + parentPath + "/" + filename[0:indexDot] + "-" + env + filename[indexDot:]
				}
				if fileExists(file) {
					return ioutil.ReadFile(file)
				}
			}
		}
		file := "./" + filename
		if !fileExists(file) {
			file = "./" + parentPath + "/" + filename
		}
		return ioutil.ReadFile(file)
	}
}
func LoadFileWithEnv(env string, filename string) ([]byte, error) {
	return LoadFileWithPath("", "", env, filename)
}
func LoadFile(filename string) ([]byte, error) {
	env := os.Getenv("ENV")
	return LoadFileWithPath("", "", env, filename)
}

func LoadCredentialsWithPath(parentPath string, directory string, env string, filename string) ([]byte, error) {
	return LoadFileWithPath(parentPath, directory, env, filename)
}
func LoadCredentialsWithEnv(env string, filename string) ([]byte, error) {
	return LoadFileWithPath("", "", env, filename)
}
func LoadCredentials(filename string) ([]byte, error) {
	env := os.Getenv("ENV")
	return LoadFileWithPath("", "", env, filename)
}

func LoadTextWithPath(parentPath string, directory string, env string, filename string) (string, error) {
	rs, err := LoadFileWithPath(parentPath, directory, env, filename)
	if err != nil {
		return "", err
	}
	return string(rs), nil
}
func LoadTextWithEnv(env string, filename string) (string, error) {
	return LoadTextWithPath("", "", env, filename)
}
func LoadText(filename string) (string, error) {
	env := os.Getenv("ENV")
	return LoadTextWithPath("", "", env, filename)
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
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

type Retry struct {
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
	Retry21 int64 `mapstructure:"21" json:"retry21,omitempty" gorm:"column:retry21" bson:"retry21,omitempty" dynamodbav:"retry21,omitempty" firestore:"retry21,omitempty"`
	Retry22 int64 `mapstructure:"22" json:"retry22,omitempty" gorm:"column:retry22" bson:"retry22,omitempty" dynamodbav:"retry22,omitempty" firestore:"retry22,omitempty"`
	Retry23 int64 `mapstructure:"23" json:"retry23,omitempty" gorm:"column:retry23" bson:"retry23,omitempty" dynamodbav:"retry23,omitempty" firestore:"retry23,omitempty"`
	Retry24 int64 `mapstructure:"24" json:"retry24,omitempty" gorm:"column:retry24" bson:"retry24,omitempty" dynamodbav:"retry24,omitempty" firestore:"retry24,omitempty"`
	Retry25 int64 `mapstructure:"25" json:"retry25,omitempty" gorm:"column:retry25" bson:"retry25,omitempty" dynamodbav:"retry25,omitempty" firestore:"retry25,omitempty"`
	Retry26 int64 `mapstructure:"26" json:"retry26,omitempty" gorm:"column:retry26" bson:"retry26,omitempty" dynamodbav:"retry26,omitempty" firestore:"retry26,omitempty"`
	Retry27 int64 `mapstructure:"27" json:"retry27,omitempty" gorm:"column:retry27" bson:"retry27,omitempty" dynamodbav:"retry27,omitempty" firestore:"retry27,omitempty"`
	Retry28 int64 `mapstructure:"28" json:"retry28,omitempty" gorm:"column:retry28" bson:"retry28,omitempty" dynamodbav:"retry28,omitempty" firestore:"retry28,omitempty"`
	Retry29 int64 `mapstructure:"29" json:"retry29,omitempty" gorm:"column:retry29" bson:"retry29,omitempty" dynamodbav:"retry29,omitempty" firestore:"retry29,omitempty"`
	Retry30 int64 `mapstructure:"30" json:"retry30,omitempty" gorm:"column:retry30" bson:"retry30,omitempty" dynamodbav:"retry30,omitempty" firestore:"retry30,omitempty"`
	Retry31 int64 `mapstructure:"31" json:"retry31,omitempty" gorm:"column:retry31" bson:"retry31,omitempty" dynamodbav:"retry31,omitempty" firestore:"retry31,omitempty"`
	Retry32 int64 `mapstructure:"32" json:"retry32,omitempty" gorm:"column:retry32" bson:"retry32,omitempty" dynamodbav:"retry32,omitempty" firestore:"retry32,omitempty"`
	Retry33 int64 `mapstructure:"33" json:"retry33,omitempty" gorm:"column:retry33" bson:"retry33,omitempty" dynamodbav:"retry33,omitempty" firestore:"retry33,omitempty"`
	Retry34 int64 `mapstructure:"34" json:"retry34,omitempty" gorm:"column:retry34" bson:"retry34,omitempty" dynamodbav:"retry34,omitempty" firestore:"retry34,omitempty"`
	Retry35 int64 `mapstructure:"35" json:"retry35,omitempty" gorm:"column:retry35" bson:"retry35,omitempty" dynamodbav:"retry35,omitempty" firestore:"retry35,omitempty"`
	Retry36 int64 `mapstructure:"36" json:"retry36,omitempty" gorm:"column:retry36" bson:"retry36,omitempty" dynamodbav:"retry36,omitempty" firestore:"retry36,omitempty"`
	Retry37 int64 `mapstructure:"37" json:"retry37,omitempty" gorm:"column:retry37" bson:"retry37,omitempty" dynamodbav:"retry37,omitempty" firestore:"retry37,omitempty"`
	Retry38 int64 `mapstructure:"38" json:"retry38,omitempty" gorm:"column:retry38" bson:"retry38,omitempty" dynamodbav:"retry38,omitempty" firestore:"retry38,omitempty"`
	Retry39 int64 `mapstructure:"39" json:"retry39,omitempty" gorm:"column:retry39" bson:"retry39,omitempty" dynamodbav:"retry39,omitempty" firestore:"retry39,omitempty"`
	Retry40 int64 `mapstructure:"40" json:"retry40,omitempty" gorm:"column:retry40" bson:"retry40,omitempty" dynamodbav:"retry40,omitempty" firestore:"retry40,omitempty"`
	Retry41 int64 `mapstructure:"41" json:"retry41,omitempty" gorm:"column:retry41" bson:"retry41,omitempty" dynamodbav:"retry41,omitempty" firestore:"retry41,omitempty"`
	Retry42 int64 `mapstructure:"42" json:"retry42,omitempty" gorm:"column:retry42" bson:"retry42,omitempty" dynamodbav:"retry42,omitempty" firestore:"retry42,omitempty"`
	Retry43 int64 `mapstructure:"43" json:"retry43,omitempty" gorm:"column:retry43" bson:"retry43,omitempty" dynamodbav:"retry43,omitempty" firestore:"retry43,omitempty"`
	Retry44 int64 `mapstructure:"44" json:"retry44,omitempty" gorm:"column:retry44" bson:"retry44,omitempty" dynamodbav:"retry44,omitempty" firestore:"retry44,omitempty"`
	Retry45 int64 `mapstructure:"45" json:"retry45,omitempty" gorm:"column:retry45" bson:"retry45,omitempty" dynamodbav:"retry45,omitempty" firestore:"retry45,omitempty"`
	Retry46 int64 `mapstructure:"46" json:"retry46,omitempty" gorm:"column:retry46" bson:"retry46,omitempty" dynamodbav:"retry46,omitempty" firestore:"retry46,omitempty"`
	Retry47 int64 `mapstructure:"47" json:"retry47,omitempty" gorm:"column:retry47" bson:"retry47,omitempty" dynamodbav:"retry47,omitempty" firestore:"retry47,omitempty"`
	Retry48 int64 `mapstructure:"48" json:"retry48,omitempty" gorm:"column:retry48" bson:"retry48,omitempty" dynamodbav:"retry48,omitempty" firestore:"retry48,omitempty"`
	Retry49 int64 `mapstructure:"49" json:"retry49,omitempty" gorm:"column:retry49" bson:"retry49,omitempty" dynamodbav:"retry49,omitempty" firestore:"retry49,omitempty"`
	Retry50 int64 `mapstructure:"50" json:"retry50,omitempty" gorm:"column:retry50" bson:"retry50,omitempty" dynamodbav:"retry50,omitempty" firestore:"retry50,omitempty"`
}
