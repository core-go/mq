package mongo

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

func NewClientOptions(c MongoConfig) *options.ClientOptions {
	option := options.Client().ApplyURI(c.Uri)
	if len(c.ReplicaSet) > 0 {
		option = option.SetReplicaSet(c.ReplicaSet)
	}
	if c.Credential != nil {
		cr := *c.Credential
		cred := options.Credential {
			Username: cr.Username,
			Password: cr.Password,
		}
		if cr.AuthMechanism != nil {
			cred.AuthMechanism = *cr.AuthMechanism
		}
		if cr.AuthMechanismProperties != nil && len(cr.AuthMechanismProperties) > 0 {
			cred.AuthMechanismProperties = cr.AuthMechanismProperties
		}
		if cr.AuthSource != nil {
			cred.AuthSource = *cr.AuthSource
		}
		if cr.PasswordSet != nil {
			cred.PasswordSet = *cr.PasswordSet
		}
		option = option.SetAuth(cred)
	}
	if c.Compressors != nil && len(c.Compressors) > 0 {
		option = option.SetCompressors(c.Compressors)
	}
	if c.Hosts != nil && len(c.Hosts) > 0 {
		option = option.SetHosts(c.Hosts)
	}
	if c.RetryReads != nil {
		option = option.SetRetryReads(*c.RetryReads)
	}
	if c.RetryWrites != nil {
		option = option.SetRetryWrites(*c.RetryWrites)
	}
	if len(c.AppName) > 0 {
		option = option.SetAppName(c.AppName)
	}
	if c.MaxPoolSize > 0 {
		option = option.SetMaxPoolSize(c.MaxPoolSize)
	}
	if c.MinPoolSize > 0 {
		option = option.SetMinPoolSize(c.MinPoolSize)
	}
	if c.ConnectTimeout > 0 {
		option = option.SetConnectTimeout(time.Duration(c.ConnectTimeout) * time.Second)
	}
	if c.SocketTimeout > 0 {
		option = option.SetSocketTimeout(time.Duration(c.SocketTimeout) * time.Second)
	}
	if c.ServerSelectionTimeout > 0 {
		option = option.SetServerSelectionTimeout(time.Duration(c.ServerSelectionTimeout) * time.Second)
	}
	if c.LocalThreshold > 0 {
		option = option.SetLocalThreshold(time.Duration(c.LocalThreshold) * time.Nanosecond)
	}
	if c.HeartbeatInterval > 0 {
		option = option.SetHeartbeatInterval(time.Duration(c.HeartbeatInterval) * time.Nanosecond)
	}
	if c.ZlibLevel != 0 {
		option = option.SetZlibLevel(c.ZlibLevel)
	}
	if c.MaxConnIdleTime > 0 {
		option = option.SetMaxConnIdleTime(time.Duration(c.MaxConnIdleTime) * time.Second)
	}
	if c.DisableOCSPEndpointCheck != nil {
		option = option.SetDisableOCSPEndpointCheck(*c.DisableOCSPEndpointCheck)
	}
	if c.Direct != nil {
		option = option.SetDirect(*c.Direct)
	}
	return option
}
func ConnectToDatabase(ctx context.Context, conf MongoConfig) (*mongo.Database, error) {
	return Setup(ctx, conf)
}
func Setup(ctx context.Context, conf MongoConfig) (*mongo.Database, error) {
	client, err := Connect(ctx, conf)
	if err != nil {
		return nil, err
	}
	db := client.Database(conf.Database)
	return db, nil
}
func Connect(ctx context.Context, conf MongoConfig) (*mongo.Client, error) {
	option := NewClientOptions(conf)
	return mongo.Connect(ctx, option)
}
func CreateConnection(ctx context.Context, uri string, database string) (*mongo.Database, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	db := client.Database(database)
	return db, nil
}
