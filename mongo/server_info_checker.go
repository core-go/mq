package mongo

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"time"
)

type ServerInfoChecker struct {
	db      *mongo.Database
	name    string
	timeout time.Duration
}

func NewServerInfoCheckerWithTimeout(db *mongo.Database, name string, timeout time.Duration) *ServerInfoChecker {
	return &ServerInfoChecker{db, name, timeout}
}
func NewServerInfoChecker(db *mongo.Database, options ...string) *ServerInfoChecker {
	var name string
	if len(options) >= 1 && len(options[0]) > 0 {
		name = options[0]
	} else {
		name = "mongo"
	}
	return NewServerInfoCheckerWithTimeout(db, name, 4*time.Second)
}

func (s *ServerInfoChecker) Name() string {
	return s.name
}

func (s *ServerInfoChecker) Check(ctx context.Context) (map[string]interface{}, error) {
	cancel := func() {}
	if s.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, s.timeout)
	}
	defer cancel()

	res := make(map[string]interface{})
	info := make(map[string]interface{})
	checkerChan := make(chan error)
	go func() {
		checkerChan <- s.db.RunCommand(ctx, bsonx.Doc{{"serverStatus", bsonx.Int32(1)}}).Decode(&info)
	}()
	select {
	case err := <-checkerChan:
		res["version"] = info["version"]
		return res, err
	case <-ctx.Done():
		return res, fmt.Errorf("timeout")
	}
}

func (s *ServerInfoChecker) Build(ctx context.Context, data map[string]interface{}, err error) map[string]interface{} {
	if err == nil {
		return data
	}
	if data == nil {
		data = make(map[string]interface{}, 0)
	}
	data["error"] = err.Error()
	return data
}
