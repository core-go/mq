package log

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
)

type SizeOfFile string

type Bytesize float64

// bytes sizes generally used for computer storage and memory
const (
	B   Bytesize = 1
	KiB Bytesize = 1024 * B
	MiB Bytesize = 1024 * KiB
	GiB Bytesize = 1024 * MiB
	TiB Bytesize = 1024 * GiB
	PiB Bytesize = 1024 * TiB
	EiB Bytesize = 1024 * PiB
)

func (s *SizeOfFile) Parse(n int64, unit string) int64 {
	unit = strings.ToUpper(unit)
	switch unit {
	case "B":
		return n
	case "MB":
		return n * int64(MiB)
	case "KB":
		return n * int64(KiB)
	case "GB":
		return n * int64(GiB)
	}
	return 0
}

func (s SizeOfFile) GetByteSize() (int64, error) {
	size, unit, err := s.parse(string(s))
	if err != nil {
		return 0, err
	}
	return s.Parse(size, unit), nil
}

func (s SizeOfFile) parse(input string) (int64, string, error) {
	var rxPattern = regexp.MustCompile(`(\d*)(B|KB|MB|GB)`)
	submatch := rxPattern.FindStringSubmatch(input)
	if len(submatch) == 3 {
		value := submatch[1]
		size, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return size, "", err
		}
		unit := submatch[2]
		return size, unit, nil
	}
	return 0, "", errors.New("Invalid format of size")
}
