package sql

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type HealthChecker struct {
	db      *sql.DB
	name    string
	timeout time.Duration
}

func NewSqlHealthChecker(db *sql.DB, name string, timeouts ...time.Duration) *HealthChecker {
	var timeout time.Duration
	if len(timeouts) >= 1 {
		timeout = timeouts[0]
	} else {
		timeout = 4 * time.Second
	}
	return &HealthChecker{db: db, name: name, timeout: timeout}
}
func NewHealthChecker(db *sql.DB, options ...string) *HealthChecker {
	var name string
	if len(options) >= 1 && len(options[0]) > 0 {
		name = options[0]
	} else {
		name = "sql"
	}
	return NewSqlHealthChecker(db, name, 4*time.Second)
}

func (s *HealthChecker) Name() string {
	return s.name
}

func (s *HealthChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	res := make(map[string]interface{}, 0)
	if s.timeout > 0 {
		ctx, _ = context.WithTimeout(ctx, s.timeout)
	}

	checkerChan := make(chan error)
	go func() {
		err := s.db.Ping()
		checkerChan <- err
	}()
	select {
	case err := <-checkerChan:
		if err != nil {
			return res, err
		}
		return res, err
	case <-ctx.Done():
		return res, errors.New("connection timout")
	}
}

func (s *HealthChecker) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if err == nil {
		return data
	}
	if data == nil {
		data = make(map[string]interface{}, 0)
	}
	data["error"] = err.Error()
	return data
}
