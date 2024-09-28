package log

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

func GenerateFileNameLog(pathFileNameFormat string) string {
	path := pathFileNameFormat
	if strings.Contains(pathFileNameFormat, "$") {
		s := getLayoutsFormat(pathFileNameFormat)
		if len(s) > 1 {
			timeFormat := time.Now().Format(s[1])
			path = strings.ReplaceAll(pathFileNameFormat, s[0], timeFormat)
		}
	}
	CreatePath(path)
	return path
}

func CreatePath(path string) error {
	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		err2 := os.Mkdir(dir, os.ModePerm)
		if err2 != nil {
			return err2
		}
	}
	return nil
}

func getLayoutsFormat(s string) []string {
	re := regexp.MustCompile("\\$\\{(.*?)\\}")
	return re.FindStringSubmatch(s)
}
