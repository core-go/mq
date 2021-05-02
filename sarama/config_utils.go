package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"github.com/Shopify/sarama"
	"io/ioutil"
	"log"
)

func GetConfig(brokers []string, algorithm *string, config *ClientConfig, conf sarama.Config) (*sarama.Config, error) {
	if len(brokers) == 0 {
		log.Fatalln("at least one broker is required")
		return nil, errors.New("at least one broker is required")
	}
	conf.Producer.Retry.Max = 1
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Return.Successes = true
	conf.Metadata.Full = true
	if config.MetadataFull != nil {
		conf.Metadata.Full = *config.MetadataFull
	}
	if config.Version != nil {
		conf.Version = *config.Version
	} else {
		conf.Version = sarama.V2_0_1_0
	}
	conf.ClientID = "sasl_scram_client"
	if config.ClientID != nil {
		conf.ClientID = *config.ClientID
	}
	if config != nil {
		if config.Username == nil {
			log.Fatalln("SASL username is required")
		} else {
			conf.Net.SASL.User = *config.Username
		}

		if config.Password == nil {
			log.Fatalln("SASL password is required")
		} else {
			conf.Net.SASL.Password = *config.Password
		}
		if config.SASLEnable != nil {
			conf.Net.SASL.Enable = *config.SASLEnable // true
		} else {
			conf.Net.SASL.Enable = true
		}
		if config.SASLHandshake != nil {
			conf.Net.SASL.Handshake = *config.SASLHandshake // true
		} else {
			conf.Net.SASL.Handshake = true
		}

		if *algorithm == sarama.SASLTypeSCRAMSHA512 {
			conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
			conf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		} else if *algorithm == sarama.SASLTypeSCRAMSHA256 {
			conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
			conf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		} else {
			log.Fatalf("invalid SHA algorithm \"%s\": can be either \"sha256\" or \"sha512\"", *algorithm)
		}

		if config.TLSEnable != nil {
			conf.Net.TLS.Enable = *config.TLSEnable
			if *config.TLSEnable == true && config.TLS != nil {
				x := config.TLS
				conf.Net.TLS.Config = CreateTLSConfig(*x)
			}
		} else {
			conf.Net.TLS.Enable = true
			if config.TLS != nil {
				x := config.TLS
				conf.Net.TLS.Config = CreateTLSConfig(*x)
			} else {
				t := tls.Config{}
				t.InsecureSkipVerify = true
				conf.Net.TLS.Config = &t
			}
		}
	}

	return &conf, nil
}

func CreateTLSConfig(c TLSConfig) (t *tls.Config) {
	t = &tls.Config{}
	if c.CertFile != "" && c.KeyFile != "" && c.CaFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			log.Fatalf("%v", err)
		}
		caCert, err := ioutil.ReadFile(c.CaFile)
		if err != nil {
			log.Fatalf("%v", err)
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
		}
	}
	if c.InsecureSkipVerify != nil {
		t.InsecureSkipVerify = *c.InsecureSkipVerify
	} else {
		t.InsecureSkipVerify = true
	}
	return t
}
