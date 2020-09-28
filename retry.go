package mq

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"time"
)

//Copy this code from https://stackoverflow.com/questions/47606761/repeat-code-if-an-error-occured
func Retry(sleeps []time.Duration, f func() error) (err error) {
	attempts := len(sleeps)
	for i := 0; ; i++ {
		if logrus.IsLevelEnabled(logrus.InfoLevel) {
			logrus.Infof("Retrying %d of %d ", i+1, attempts)
		}

		err = f()
		if err == nil {
			return
		}
		if i >= (attempts - 1) {
			break
		}
		time.Sleep(sleeps[i])
		if logrus.IsLevelEnabled(logrus.InfoLevel) {
			logrus.Infof("Retrying %d of %d after error: %s", i+1, attempts, err.Error())
		}
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}
