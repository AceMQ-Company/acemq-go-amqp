// Copyright 2026 AceMQ.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package devcerts generates the certificates needed to talk to a broker over
// TLS on your own machine.
//
// Everything it writes is stamped with [security.DevelopmentMarker], and the
// library refuses a certificate carrying that mark unless
// AllowDevelopmentCertificates says otherwise. That is the point: these
// certificates cannot quietly end up in front of a real broker.
//
// The command in cmd/acemq-certs wraps this:
//
//	go install github.com/AceMQ-Company/acemq-go-amqp/cmd/acemq-certs@latest
//	acemq-certs --out certs --broker localhost --days 30
package devcerts

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/AceMQ-Company/acemq-go-amqp/security"
)

// Options control what is generated.
type Options struct {
	// Directory is where the files are written. Created if it is not there.
	Directory string

	// BrokerHost is the name the broker's certificate is issued for, and the
	// name a client must connect by for verification to succeed.
	BrokerHost string

	// Validity is how long the certificates last. Short on purpose: a
	// development certificate that lasts a year is one that ends up somewhere
	// it should not.
	Validity time.Duration

	// BrokerCertPath is the directory rabbitmq.conf points at, which is where
	// the certificates are mounted inside the broker's container rather than
	// where they are on your machine.
	BrokerCertPath string

	// NoBrokerConfig skips writing rabbitmq.conf.
	NoBrokerConfig bool
}

// Defaults are what the command uses when told nothing.
func Defaults() Options {
	return Options{
		Directory:      "certs",
		BrokerHost:     "localhost",
		Validity:       30 * 24 * time.Hour,
		BrokerCertPath: "/certs",
	}
}

// Result is what was written.
type Result struct {
	Directory   string
	Authority   *x509.Certificate
	Broker      *x509.Certificate
	Client      *x509.Certificate
	Expiry      time.Time
	Files       []string
	BrokerConf  string
	MarkerUsed  string
	BrokerHost  string
	ValidityFor time.Duration
}

// Generate writes a certificate authority, a broker certificate and a client
// certificate.
//
// Existing files are overwritten. These are development certificates with a
// short life, so regenerating them is the expected way to deal with expiry
// rather than something to guard against.
func Generate(opts Options) (*Result, error) {
	if opts.Directory == "" {
		opts.Directory = "certs"
	}
	if opts.BrokerHost == "" {
		opts.BrokerHost = "localhost"
	}
	if opts.Validity <= 0 {
		opts.Validity = 30 * 24 * time.Hour
	}
	if opts.BrokerCertPath == "" {
		opts.BrokerCertPath = "/certs"
	}

	if err := os.MkdirAll(opts.Directory, 0o755); err != nil {
		return nil, fmt.Errorf("acemq-certs: cannot create %s: %w", opts.Directory, err)
	}

	notBefore := time.Now().Add(-time.Hour)
	notAfter := time.Now().Add(opts.Validity)

	caCert, caKey, caDER, err := generateAuthority(notBefore, notAfter)
	if err != nil {
		return nil, err
	}

	brokerCert, brokerKey, brokerDER, err := generateLeaf(
		"broker", opts.BrokerHost, caCert, caKey, notBefore, notAfter,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	if err != nil {
		return nil, err
	}

	clientCert, clientKey, clientDER, err := generateLeaf(
		"client", "acemq-client", caCert, caKey, notBefore, notAfter,
		[]x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})
	if err != nil {
		return nil, err
	}

	result := &Result{
		Directory:   opts.Directory,
		Authority:   caCert,
		Broker:      brokerCert,
		Client:      clientCert,
		Expiry:      notAfter,
		MarkerUsed:  security.DevelopmentMarker,
		BrokerHost:  opts.BrokerHost,
		ValidityFor: opts.Validity,
	}

	writes := []struct {
		name string
		data []byte
		mode os.FileMode
	}{
		{"ca.crt", encodeCert(caDER), 0o644},
		{"ca.key", encodeKey(caKey), 0o600},
		{"server.crt", encodeCert(brokerDER), 0o644},
		{"server.key", encodeKey(brokerKey), 0o600},
		{"client.crt", encodeCert(clientDER), 0o644},
		{"client.key", encodeKey(clientKey), 0o600},
	}
	for _, w := range writes {
		path := filepath.Join(opts.Directory, w.name)
		// Keys are written 0600. A development key is still a key, and one
		// world-readable in a repository checkout is a habit worth not forming.
		if err := os.WriteFile(path, w.data, w.mode); err != nil {
			return nil, fmt.Errorf("acemq-certs: cannot write %s: %w", path, err)
		}
		result.Files = append(result.Files, path)
	}

	if !opts.NoBrokerConfig {
		path := filepath.Join(opts.Directory, "rabbitmq.conf")
		conf := BrokerConfig(opts.BrokerCertPath)
		if err := os.WriteFile(path, []byte(conf), 0o644); err != nil {
			return nil, fmt.Errorf("acemq-certs: cannot write %s: %w", path, err)
		}
		result.Files = append(result.Files, path)
		result.BrokerConf = path
	}

	return result, nil
}

// BrokerConfig is a rabbitmq.conf that serves TLS from certPath.
//
// verify_peer with fail_if_no_peer_cert off means the broker will use a client
// certificate if one is presented and will not insist on it, which is what
// suits a development broker that is also reached by password.
func BrokerConfig(certPath string) string {
	return fmt.Sprintf(`# Written by acemq-certs. Development only.
listeners.ssl.default = 5671

ssl_options.cacertfile = %[1]s/ca.crt
ssl_options.certfile   = %[1]s/server.crt
ssl_options.keyfile    = %[1]s/server.key
ssl_options.verify     = verify_peer
ssl_options.fail_if_no_peer_cert = false

# The plaintext listener stays on so a laptop can use either.
listeners.tcp.default = 5672
`, certPath)
}

func generateAuthority(notBefore, notAfter time.Time) (*x509.Certificate, *ecdsa.PrivateKey, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("acemq-certs: cannot generate a key: %w", err)
	}

	serial, err := serialNumber()
	if err != nil {
		return nil, nil, nil, err
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			// The marker goes in the organisation, where it is visible in every
			// tool that prints a subject, and where the library looks for it.
			Organization: []string{security.DevelopmentMarker},
			CommonName:   "AceMQ development CA",
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLen:            0,
		MaxPathLenZero:        true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("acemq-certs: cannot create the authority: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, nil, err
	}
	return cert, key, der, nil
}

func generateLeaf(
	kind, name string, ca *x509.Certificate, caKey *ecdsa.PrivateKey,
	notBefore, notAfter time.Time, usage []x509.ExtKeyUsage,
) (*x509.Certificate, *ecdsa.PrivateKey, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("acemq-certs: cannot generate the %s key: %w", kind, err)
	}

	serial, err := serialNumber()
	if err != nil {
		return nil, nil, nil, err
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			Organization: []string{security.DevelopmentMarker},
			CommonName:   name,
		},
		NotBefore:   notBefore,
		NotAfter:    notAfter,
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: usage,
	}

	if kind == "broker" {
		template.DNSNames = []string{name}
		// A broker reached as localhost is very often reached as 127.0.0.1 as
		// well, and a certificate that covers only one of them fails in a way
		// that reads like a trust problem rather than a naming one.
		if ip := net.ParseIP(name); ip != nil {
			template.IPAddresses = []net.IP{ip}
			template.DNSNames = nil
		} else if name == "localhost" {
			template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")}
		}
	}

	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("acemq-certs: cannot create the %s certificate: %w", kind, err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, nil, err
	}
	return cert, key, der, nil
}

func serialNumber() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return nil, fmt.Errorf("acemq-certs: cannot draw a serial number: %w", err)
	}
	return serial, nil
}

func encodeCert(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func encodeKey(key *ecdsa.PrivateKey) []byte {
	der, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		// The key was generated here, so this cannot fail without something
		// being very wrong indeed.
		panic("acemq-certs: cannot encode a key that was just generated: " + err.Error())
	}
	return pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: der})
}
