package converter

import (
	"fmt"
	"time"
)
const layout = "2006-01-02"
func TimeToMilliseconds(time string) (int64, error) {
	var h, m, s int
	_, err := fmt.Sscanf(time, "%02d:%02d:%02d", &h, &m, &s)
	if err != nil {
		return 0, err
	}
	return int64(h * 3600000 + m * 60000 + s * 1000), nil
}
func DateToUnixTime(s string) (int64, error) {
	date, err := time.Parse(layout, s)
	if err != nil {
		return 0, err
	}
	return date.Unix() * 1000, nil
}
func DateToUnixNano(s string) (int64, error) {
	date, err := time.Parse(layout, s)
	if err != nil {
		return 0, err
	}
	return date.UnixNano(), nil
}
func UnixTime(m int64) string {
	dateUtc := time.Unix(0, m* 1000000)
	return dateUtc.Format("2006-01-02")
}
func MillisecondsToTimeString(milliseconds int) string {
	hourUint := 3600000 //60 * 60 * 1000 = 3600000
	minuteUint := 60000 //60 * 1000 = 60000
	secondUint := 1000
	hour := milliseconds / hourUint
	milliseconds = milliseconds % hourUint
	minute := milliseconds / minuteUint
	milliseconds = milliseconds % minuteUint
	second := milliseconds / secondUint
	return fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
}
func StringToAvroDate(date *string) (*int, error) {
	if date == nil {
		return nil, nil
	}
	d, err := time.Parse(layout, *date)
	if err != nil {
		return nil, err
	}
	i := int(d.Unix() / int64(60*60*24))
	return &i, nil
}
func ToAvroDate(date *time.Time) *int {
	if date == nil {
		return nil
	}
	i := int(date.Unix() / int64(60*60*24))
	return &i
}
