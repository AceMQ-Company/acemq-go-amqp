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

// Package security configures how AceMQ reaches a broker: whether the
// connection is encrypted, which authority is trusted, and what credentials are
// presented.
//
// It has no dependencies outside the standard library and does not import the
// rest of the library, so it can be built and tested on its own.
//
//	sec := security.Required().
//		TrustCertificateAuthorityFile("certs/ca.crt").
//		WithCredentials(security.EnvironmentCredentials("MQ_USER", "MQ_PASSWORD"))
//
//	mq, err := acemq.Connect(ctx, "amqps://broker:5671/", acemq.WithSecurity(sec))
//
// The three modes are deliberately named rather than being a bare boolean:
// [Required] verifies, [Insecure] does not, [Disabled] does not encrypt at all.
// Somebody reading the configuration should not have to work out which of those
// a false means.
package security

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"
)

// DevelopmentMarker appears in the subject of every certificate the AceMQ
// development tooling generates.
//
// A certificate carrying it is refused on every path, including [Insecure],
// unless [Options.AllowDevelopmentCertificates] says otherwise. The point is
// that a development certificate reaching a production broker should be an
// error rather than a thing that quietly works because somebody turned
// verification off to get past a different problem.
const DevelopmentMarker = "ACEMQ DEVELOPMENT ONLY - DO NOT TRUST"

// Mode is how much verification a connection does.
type Mode int

const (
	// ModeRequired encrypts and verifies. The default, and the only one to use
	// against a broker holding anything that matters.
	ModeRequired Mode = iota

	// ModeInsecure encrypts but accepts any certificate. Traffic cannot be read
	// by an observer, but nothing stops one from being the broker.
	ModeInsecure

	// ModeDisabled does not encrypt.
	ModeDisabled
)

// String names the mode.
func (m Mode) String() string {
	switch m {
	case ModeRequired:
		return "required"
	case ModeInsecure:
		return "insecure"
	case ModeDisabled:
		return "disabled"
	default:
		return "unknown"
	}
}

// ConfigurationError is a security setting that cannot be honoured.
type ConfigurationError struct {
	Message string
	Err     error
}

func (e *ConfigurationError) Error() string {
	if e.Err == nil {
		return e.Message
	}
	return e.Message + ": " + e.Err.Error()
}

func (e *ConfigurationError) Unwrap() error { return e.Err }

// Options describe how to reach a broker safely.
//
// Build one with [Required], [Insecure] or [Disabled] and refine it. The
// methods return the same value so they chain; an Options is not safe to modify
// from two goroutines at once, which in practice means build it at start-up.
type Options struct {
	mode        Mode
	authority   *x509.Certificate
	clientCerts []tls.Certificate
	serverName  string
	allowDev    bool
	credentials CredentialsProvider

	// err carries the first failure from a chained call, so a caller can write
	// the whole chain and check once at the end rather than after every step.
	err error
}

// Required encrypts the connection and verifies the broker's certificate
// against the system's trust store.
func Required() *Options { return &Options{mode: ModeRequired} }

// Insecure encrypts the connection and accepts any certificate.
//
// This is for a broker with a self-signed certificate that you have not got
// round to trusting properly, and it is worth being clear about what it costs:
// the traffic cannot be read by somebody watching the network, but nothing
// stops that somebody from being the broker. Prefer
// [Options.TrustCertificateAuthority], which is barely more work.
//
// Development-marked certificates are still refused.
func Insecure() *Options { return &Options{mode: ModeInsecure} }

// Disabled makes a plaintext connection.
//
// Everything, including the password used to log in, crosses the network
// readable. Reasonable on a laptop, and against a broker on the other side of a
// network you do not entirely control it is not.
func Disabled() *Options { return &Options{mode: ModeDisabled} }

// TrustCertificateAuthority verifies the broker against one authority and no
// other.
//
// The system trust store is not consulted, which is the point: a broker with a
// certificate from a public authority is not your broker, and the hundreds of
// authorities a machine trusts by default are hundreds of ways to be wrong.
func (o *Options) TrustCertificateAuthority(authority *x509.Certificate) *Options {
	if o.err != nil {
		return o
	}
	if authority == nil {
		o.err = &ConfigurationError{Message: "acemq: TrustCertificateAuthority was given no certificate"}
		return o
	}
	o.authority = authority
	return o
}

// TrustCertificateAuthorityFile reads a PEM-encoded authority from disk and
// trusts it alone.
func (o *Options) TrustCertificateAuthorityFile(path string) *Options {
	if o.err != nil {
		return o
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		o.err = &ConfigurationError{
			Message: fmt.Sprintf("acemq: cannot read the certificate authority %s", path), Err: err}
		return o
	}
	cert, err := parsePEMCertificate(raw)
	if err != nil {
		o.err = &ConfigurationError{
			Message: fmt.Sprintf("acemq: %s is not a PEM-encoded certificate", path), Err: err}
		return o
	}
	return o.TrustCertificateAuthority(cert)
}

// WithClientCertificate presents a certificate to the broker, for a broker that
// authenticates clients by certificate rather than by password.
func (o *Options) WithClientCertificate(cert tls.Certificate) *Options {
	if o.err != nil {
		return o
	}
	o.clientCerts = append(o.clientCerts, cert)
	return o
}

// WithClientCertificateFiles loads a PEM certificate and key from disk and
// presents them to the broker.
func (o *Options) WithClientCertificateFiles(certPath, keyPath string) *Options {
	if o.err != nil {
		return o
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		o.err = &ConfigurationError{
			Message: fmt.Sprintf("acemq: cannot load the client certificate %s with key %s", certPath, keyPath),
			Err:     err}
		return o
	}
	return o.WithClientCertificate(cert)
}

// WithServerName overrides the name the certificate is checked against.
//
// Needed when connecting through an address that is not the broker's name — an
// IP address, a tunnel, a container's internal host name.
func (o *Options) WithServerName(name string) *Options {
	if o.err != nil {
		return o
	}
	o.serverName = name
	return o
}

// AllowDevelopmentCertificates permits certificates carrying
// [DevelopmentMarker].
//
// Without this they are refused however the connection is otherwise configured,
// including under [Insecure]. Calling it is a deliberate statement that this
// process is not production, which is exactly the statement that should be hard
// to make by accident.
func (o *Options) AllowDevelopmentCertificates() *Options {
	if o.err != nil {
		return o
	}
	o.allowDev = true
	return o
}

// WithCredentials supplies the broker login.
//
// Credentials given here override anything in the connection URL, which is how
// a password stays out of the URL and so out of logs and process listings.
func (o *Options) WithCredentials(provider CredentialsProvider) *Options {
	if o.err != nil {
		return o
	}
	if provider == nil {
		o.err = &ConfigurationError{Message: "acemq: WithCredentials was given no provider"}
		return o
	}
	o.credentials = provider
	return o
}

// Mode is how much verification this configuration does.
func (o *Options) Mode() Mode { return o.mode }

// DevelopmentCertificatesAllowed reports whether marked certificates pass.
func (o *Options) DevelopmentCertificatesAllowed() bool { return o.allowDev }

// Err is the first configuration failure, if there was one.
//
// The chaining methods record rather than return, so a whole configuration can
// be written as one expression and checked once. [Options.TLSConfig] returns it
// too, so a caller that goes straight there need not check separately.
func (o *Options) Err() error { return o.err }

// Credentials asks the provider for the broker login, if one was configured.
func (o *Options) Credentials() (Credentials, bool, error) {
	if o.credentials == nil {
		return Credentials{}, false, nil
	}
	creds, err := o.credentials.Credentials()
	if err != nil {
		return Credentials{}, false, err
	}
	return creds, true, nil
}

// TLSConfig builds the configuration for a connection.
//
// It returns nil for [Disabled], which is the signal to connect in plaintext.
func (o *Options) TLSConfig() (*tls.Config, error) {
	if o.err != nil {
		return nil, o.err
	}
	if o.mode == ModeDisabled {
		return nil, nil
	}

	cfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		ServerName:   o.serverName,
		Certificates: o.clientCerts,
	}

	switch {
	case o.mode == ModeInsecure:
		// Go will not check the chain, so the only check left is ours.
		cfg.InsecureSkipVerify = true
		cfg.VerifyPeerCertificate = o.verifyDevelopmentMarker

	case o.authority != nil:
		// A pool holding one certificate. Verification succeeding therefore
		// means the chain reached that authority and no other — the system
		// trust store is not consulted at all. A test proves a certificate from
		// a different authority is refused, because this is the line between
		// "encrypted" and "encrypted to the right party".
		pool := x509.NewCertPool()
		pool.AddCert(o.authority)
		cfg.RootCAs = pool
		cfg.VerifyPeerCertificate = o.verifyDevelopmentMarker

	default:
		// The system trust store, and the standard verification with it.
		cfg.VerifyPeerCertificate = o.verifyDevelopmentMarker
	}

	return cfg, nil
}

// verifyDevelopmentMarker refuses a certificate generated for development.
//
// It runs on every path, including Insecure, and reads the raw certificates
// rather than the verified chains: under InsecureSkipVerify there are no
// verified chains, and that is precisely the configuration where a development
// certificate is most likely to slip through.
func (o *Options) verifyDevelopmentMarker(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	if o.allowDev {
		return nil
	}
	for _, raw := range rawCerts {
		cert, err := x509.ParseCertificate(raw)
		if err != nil {
			// Go has already parsed these to get here; a failure now is not
			// something to guess about.
			return fmt.Errorf("acemq: cannot read a certificate the broker presented: %w", err)
		}
		if IsDevelopmentCertificate(cert) {
			return &ConfigurationError{Message: fmt.Sprintf(
				"acemq: the broker presented a certificate marked %q (subject %q). "+
					"It was generated for development and is not trusted. If this really is a "+
					"development broker, say so with AllowDevelopmentCertificates()",
				DevelopmentMarker, cert.Subject.String())}
		}
	}
	return nil
}

// IsDevelopmentCertificate reports whether a certificate carries
// [DevelopmentMarker] in its subject or issuer.
func IsDevelopmentCertificate(cert *x509.Certificate) bool {
	if cert == nil {
		return false
	}
	marked := func(s string) bool {
		return strings.Contains(strings.ToUpper(s), strings.ToUpper(DevelopmentMarker))
	}
	return marked(cert.Subject.String()) || marked(cert.Issuer.String())
}

// String describes the configuration without revealing anything secret.
func (o *Options) String() string {
	parts := []string{"mode=" + o.mode.String()}
	if o.authority != nil {
		parts = append(parts, "authority="+o.authority.Subject.String())
	}
	if len(o.clientCerts) > 0 {
		parts = append(parts, fmt.Sprintf("clientCertificates=%d", len(o.clientCerts)))
	}
	if o.serverName != "" {
		parts = append(parts, "serverName="+o.serverName)
	}
	if o.allowDev {
		parts = append(parts, "developmentCertificatesAllowed")
	}
	if o.credentials != nil {
		parts = append(parts, "credentials=provided")
	}
	return "security.Options{" + strings.Join(parts, ", ") + "}"
}

// parsePEMCertificate reads the first CERTIFICATE block in a PEM file.
//
// A CA file often carries a key or a chain alongside the certificate, so the
// blocks are walked rather than assuming the first one is what is wanted.
func parsePEMCertificate(raw []byte) (*x509.Certificate, error) {
	rest := raw
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			return nil, errors.New("no CERTIFICATE block found")
		}
		if block.Type == "CERTIFICATE" {
			return x509.ParseCertificate(block.Bytes)
		}
	}
}
