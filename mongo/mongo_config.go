package mongo

type MongoConfig struct {
	Uri                      string            `mapstructure:"uri" json:"uri,omitempty" gorm:"column:uri" bson:"uri,omitempty" dynamodbav:"uri,omitempty" firestore:"uri,omitempty"`
	Database                 string            `mapstructure:"database" json:"database,omitempty" gorm:"column:database" bson:"database,omitempty" dynamodbav:"database,omitempty" firestore:"database,omitempty"`
	AuthSource               string            `mapstructure:"auth_source" json:"authSource,omitempty" gorm:"column:authSource" bson:"authSource,omitempty" dynamodbav:"authSource,omitempty" firestore:"authSource,omitempty"`
	ReplicaSet               string            `mapstructure:"replica_set" json:"replicaSet,omitempty" gorm:"column:replicaSet" bson:"replicaSet,omitempty" dynamodbav:"replicaSet,omitempty" firestore:"replicaSet,omitempty"`
	Credential               *CredentialConfig `mapstructure:"credential" json:"credential,omitempty" gorm:"column:credential" bson:"credential,omitempty" dynamodbav:"credential,omitempty" firestore:"credential,omitempty"`
	Compressors              []string          `mapstructure:"compressors" json:"compressors,omitempty" gorm:"column:compressors" bson:"compressors,omitempty" dynamodbav:"compressors,omitempty" firestore:"compressors,omitempty"`
	Hosts                    []string          `mapstructure:"hosts" json:"hosts,omitempty" gorm:"column:hosts" bson:"hosts,omitempty" dynamodbav:"hosts,omitempty" firestore:"hosts,omitempty"`
	RetryReads               *bool             `mapstructure:"retry_reads" json:"retryReads,omitempty" gorm:"column:retryReads" bson:"retryReads,omitempty" dynamodbav:"retryReads,omitempty" firestore:"retryReads,omitempty"`
	RetryWrites              *bool             `mapstructure:"retry_writes" json:"retryWrites,omitempty" gorm:"column:retrywrites" bson:"retryWrites,omitempty" dynamodbav:"retryWrites,omitempty" firestore:"retryWrites,omitempty"`
	AppName                  string            `mapstructure:"app_name" json:"appName,omitempty" gorm:"column:appname" bson:"appName,omitempty" dynamodbav:"appName,omitempty" firestore:"appName,omitempty"`
	MaxPoolSize              uint64            `mapstructure:"max_pool_size" json:"maxPoolSize,omitempty" gorm:"column:maxpoolsize" bson:"maxPoolSize,omitempty" dynamodbav:"maxPoolSize,omitempty" firestore:"maxPoolSize,omitempty"`
	MinPoolSize              uint64            `mapstructure:"min_pool_size" json:"minPoolSize,omitempty" gorm:"column:minpoolsize" bson:"minPoolSize,omitempty" dynamodbav:"minPoolSize,omitempty" firestore:"minPoolSize,omitempty"`
	ConnectTimeout           int64             `mapstructure:"connect_timeout" json:"connectTimeout,omitempty" gorm:"column:connecttimeout" bson:"connectTimeout,omitempty" dynamodbav:"connectTimeout,omitempty" firestore:"connectTimeout,omitempty"`
	SocketTimeout            int64             `mapstructure:"socket_timeout" json:"socketTimeout,omitempty" gorm:"column:sockettimeout" bson:"socketTimeout,omitempty" dynamodbav:"socketTimeout,omitempty" firestore:"socketTimeout,omitempty"`
	ServerSelectionTimeout   int64             `mapstructure:"server_selection_timeout" json:"serverSelectionTimeout,omitempty" gorm:"column:serverselectiontimeout" bson:"serverSelectionTimeout,omitempty" dynamodbav:"serverSelectionTimeout,omitempty" firestore:"serverSelectionTimeout,omitempty"`
	LocalThreshold           int64             `mapstructure:"local_threshold" json:"localThreshold,omitempty" gorm:"column:localthreshold" bson:"localThreshold,omitempty" dynamodbav:"localThreshold,omitempty" firestore:"localThreshold,omitempty"`
	HeartbeatInterval        int64             `mapstructure:"heartbeat_interval" json:"heartbeatInterval,omitempty" gorm:"column:heartbeatinterval" bson:"heartbeatInterval,omitempty" dynamodbav:"heartbeatInterval,omitempty" firestore:"heartbeatInterval,omitempty"`
	ZlibLevel                int               `mapstructure:"zlibLevel" json:"zlibLevel,omitempty" gorm:"column:zlibLevel" bson:"zlibLevel,omitempty" dynamodbav:"zlibLevel,omitempty" firestore:"zlibLevel,omitempty"`
	MaxConnIdleTime          int64             `mapstructure:"max_conn_idle_time" json:"maxConnIdleTime,omitempty" gorm:"column:maxconnidletime" bson:"maxConnIdleTime,omitempty" dynamodbav:"maxConnIdleTime,omitempty" firestore:"maxConnIdleTime,omitempty"`
	DisableOCSPEndpointCheck *bool             `mapstructure:"disable_ocsp_endpoint_check" json:"disableOCSPEndpointCheck,omitempty" gorm:"column:disableocspendpointcheck" bson:"disableOCSPEndpointCheck,omitempty" dynamodbav:"disableOCSPEndpointCheck,omitempty" firestore:"disableOCSPEndpointCheck,omitempty"`
	Direct                   *bool             `mapstructure:"direct" json:"direct,omitempty" gorm:"column:direct" bson:"direct,omitempty" dynamodbav:"direct,omitempty" firestore:"direct,omitempty"`
}

type CredentialConfig struct {
	AuthMechanism           *string           `mapstructure:"auth_mechanisms" json:"authMechanisms,omitempty" gorm:"column:authmechanisms" bson:"authMechanisms,omitempty" dynamodbav:"authMechanisms,omitempty" firestore:"authMechanisms,omitempty"`
	AuthMechanismProperties map[string]string `mapstructure:"auth_mechanism_properties" json:"authMechanismProperties,omitempty" gorm:"column:authMechanismProperties" bson:"authMechanismProperties,omitempty" dynamodbav:"authMechanismProperties,omitempty" firestore:"authMechanismProperties,omitempty"`
	AuthSource              *string           `mapstructure:"auth_source" json:"authSource,omitempty" gorm:"column:authsource" bson:"authSource,omitempty" dynamodbav:"authSource,omitempty" firestore:"authSource,omitempty"`
	Username                string            `mapstructure:"username" json:"username,omitempty" gorm:"column:username" bson:"username,omitempty" dynamodbav:"username,omitempty" firestore:"username,omitempty"`
	Password                string            `mapstructure:"password" json:"password,omitempty" gorm:"column:password" bson:"password,omitempty" dynamodbav:"password,omitempty" firestore:"password,omitempty"`
	PasswordSet             *bool             `mapstructure:"password_set" json:"passwordSet,omitempty" gorm:"column:passwordset" bson:"passwordSet,omitempty" dynamodbav:"passwordSet,omitempty" firestore:"passwordSet,omitempty"`
}
