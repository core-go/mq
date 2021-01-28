package mq

import (
	"context"
	"fmt"
	"time"
)

//Copy this code from https://stackoverflow.com/questions/47606761/repeat-code-if-an-error-occured
func Retry(ctx context.Context, sleeps []time.Duration, f func() error, log func(context.Context, string)) (err error) {
	attempts := len(sleeps)
	for i := 0; ; i++ {
		if log != nil  {
			log(ctx, fmt.Sprintf("Retrying %d of %d ", i+1, attempts))
		}
		err = f()
		if err == nil {
			return
		}
		if i >= (attempts - 1) {
			break
		}
		time.Sleep(sleeps[i])
		if log != nil {
			log(ctx, fmt.Sprintf("Retrying %d of %d after error: %s", i+1, attempts, err.Error()))
		}
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}
