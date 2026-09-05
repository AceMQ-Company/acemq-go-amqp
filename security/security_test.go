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

package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// authority is a certificate authority and the certificates it signs, built in
// memory so these tests need nothing on disk and no network.
type authority struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	der  []byte
}

func newAuthority(t *testing.T, commonName string) *authority {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: commonName},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return &authority{cert: cert, key: key, der: der}
}

// sign issues a server certificate for a host.
func (a *authority) sign(t *testing.T, commonName, host string) tls.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano() + 1),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{host},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, a.cert, &key.PublicKey, a.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der, a.der}, PrivateKey: key}
}

func (a *authority) pemFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.crt")
	encoded := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: a.der})
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// serve starts a TLS listener presenting cert, and returns its address.
func serve(t *testing.T, cert tls.Certificate) string {
	t.Helper()

	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			// The handshake is all these tests care about.
			go func() {
				_ = conn.(*tls.Conn).Handshake()
				_ = conn.Close()
			}()
		}
	}()
	return listener.Addr().String()
}

// dial performs a handshake with the given options and reports what happened.
func dial(t *testing.T, addr string, opts *Options) error {
	t.Helper()

	cfg, err := opts.TLSConfig()
	if err != nil {
		return err
	}
	if cfg == nil {
		return errors.New("TLS is disabled, so there is no handshake to make")
	}
	if cfg.ServerName == "" && !cfg.InsecureSkipVerify {
		cfg.ServerName = "broker.test"
	}

	conn, err := tls.Dial("tcp", addr, cfg)
	if err != nil {
		return err
	}
	_ = conn.Close()
	return nil
}

// ---- trust -----------------------------------------------------------

func TestABrokerSignedByTheTrustedAuthorityIsAccepted(t *testing.T) {
	ca := newAuthority(t, "AceMQ Test CA")
	addr := serve(t, ca.sign(t, "broker.test", "broker.test"))

	err := dial(t, addr, Required().TrustCertificateAuthority(ca.cert))

	if err != nil {
		t.Fatalf("a broker signed by the trusted authority was refused: %v", err)
	}
}

// TestABrokerSignedByADifferentAuthorityIsRefused is the test the whole file
// exists for.
//
// Trusting one authority has to mean trusting one authority. A configuration
// that accepts any self-consistent chain is barely better than accepting
// anything, and it looks identical from the outside until somebody is on the
// network.
func TestABrokerSignedByADifferentAuthorityIsRefused(t *testing.T) {
	ours := newAuthority(t, "AceMQ Test CA")
	theirs := newAuthority(t, "Somebody Else")
	addr := serve(t, theirs.sign(t, "broker.test", "broker.test"))

	err := dial(t, addr, Required().TrustCertificateAuthority(ours.cert))

	if err == nil {
		t.Fatal("a broker signed by an untrusted authority was accepted")
	}
	var unknown x509.UnknownAuthorityError
	var hostErr x509.HostnameError
	if !errors.As(err, &unknown) && !errors.As(err, &hostErr) {
		t.Logf("refused with: %v", err)
	}
}

func TestTheSystemTrustStoreIsNotConsultedWhenAnAuthorityIsNamed(t *testing.T) {
	// The same property from the other side: naming an authority must replace
	// the system's list rather than add to it. A broker whose certificate came
	// from a public authority is not this broker.
	ours := newAuthority(t, "AceMQ Test CA")
	cfg, err := Required().TrustCertificateAuthority(ours.cert).TLSConfig()
	if err != nil {
		t.Fatal(err)
	}

	if cfg.RootCAs == nil {
		t.Fatal("no root pool was set, so the system store is still in use")
	}
	if len(cfg.RootCAs.Subjects()) != 1 { //nolint:staticcheck // the count is the point
		t.Errorf("the root pool holds %d certificates, want exactly the one named",
			len(cfg.RootCAs.Subjects())) //nolint:staticcheck
	}
}

func TestAnAuthorityCanBeReadFromAFile(t *testing.T) {
	ca := newAuthority(t, "AceMQ Test CA")
	addr := serve(t, ca.sign(t, "broker.test", "broker.test"))

	err := dial(t, addr, Required().TrustCertificateAuthorityFile(ca.pemFile(t)))

	if err != nil {
		t.Fatalf("a broker signed by the authority read from disk was refused: %v", err)
	}
}

func TestAMissingAuthorityFileIsReportedNotIgnored(t *testing.T) {
	opts := Required().TrustCertificateAuthorityFile("/nonexistent/ca.crt")

	if opts.Err() == nil {
		t.Fatal("a missing authority file was accepted; the connection would fall back to the system store")
	}
	if _, err := opts.TLSConfig(); err == nil {
		t.Fatal("TLSConfig succeeded despite the configuration having failed")
	}
}

func TestAFileThatIsNotACertificateIsReported(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-cert.pem")
	if err := os.WriteFile(path, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}

	if Required().TrustCertificateAuthorityFile(path).Err() == nil {
		t.Fatal("a file that is not a certificate was accepted")
	}
}

// ---- development certificates ----------------------------------------

func TestADevelopmentCertificateIsRefusedEvenWhenItsAuthorityIsTrusted(t *testing.T) {
	ca := newAuthority(t, "AceMQ Test CA")
	addr := serve(t, ca.sign(t, DevelopmentMarker+" broker", "broker.test"))

	err := dial(t, addr, Required().TrustCertificateAuthority(ca.cert))

	if err == nil {
		t.Fatal("a development certificate was accepted against a production configuration")
	}
	if !strings.Contains(err.Error(), "AllowDevelopmentCertificates") {
		t.Errorf("the error does not say how to permit it deliberately: %v", err)
	}
}

// TestADevelopmentCertificateIsRefusedUnderInsecureToo is the path that matters
// most.
//
// Insecure exists for somebody who could not get verification working, which is
// exactly the person most likely to be pointing at a development broker — and,
// later, to leave the setting in place against a real one.
func TestADevelopmentCertificateIsRefusedUnderInsecureToo(t *testing.T) {
	ca := newAuthority(t, "AceMQ Test CA")
	addr := serve(t, ca.sign(t, DevelopmentMarker+" broker", "broker.test"))

	err := dial(t, addr, Insecure())

	if err == nil {
		t.Fatal("Insecure accepted a development certificate; the marker would mean nothing")
	}
}

func TestADevelopmentCertificateIsAcceptedWhenAllowedDeliberately(t *testing.T) {
	ca := newAuthority(t, "AceMQ Test CA")
	addr := serve(t, ca.sign(t, DevelopmentMarker+" broker", "broker.test"))

	err := dial(t, addr, Required().
		TrustCertificateAuthority(ca.cert).
		AllowDevelopmentCertificates())

	if err != nil {
		t.Fatalf("a development certificate was refused despite being allowed: %v", err)
	}
}

func TestTheMarkerIsFoundOnTheIssuerAsWellAsTheSubject(t *testing.T) {
	// A development authority signing an ordinarily-named broker certificate.
	// The leaf says nothing; the chain does.
	devCA := newAuthority(t, DevelopmentMarker+" CA")
	addr := serve(t, devCA.sign(t, "broker.test", "broker.test"))

	err := dial(t, addr, Required().TrustCertificateAuthority(devCA.cert))

	if err == nil {
		t.Fatal("a certificate issued by a development authority was accepted")
	}
}

func TestIsDevelopmentCertificateReadsBothNames(t *testing.T) {
	ca := newAuthority(t, DevelopmentMarker+" CA")
	ordinary := newAuthority(t, "An Ordinary CA")

	if !IsDevelopmentCertificate(ca.cert) {
		t.Error("a marked certificate was not recognised")
	}
	if IsDevelopmentCertificate(ordinary.cert) {
		t.Error("an unmarked certificate was called a development one")
	}
	if IsDevelopmentCertificate(nil) {
		t.Error("nil was called a development certificate")
	}
}

// ---- modes -----------------------------------------------------------

func TestInsecureAcceptsAnUntrustedCertificate(t *testing.T) {
	// It is what the mode is for. The test exists so that if this ever stopped
	// being true, it would be a deliberate change rather than a surprise.
	theirs := newAuthority(t, "Somebody Else")
	addr := serve(t, theirs.sign(t, "broker.test", "broker.test"))

	if err := dial(t, addr, Insecure()); err != nil {
		t.Fatalf("Insecure refused an untrusted certificate: %v", err)
	}
}

func TestDisabledProducesNoTLSConfiguration(t *testing.T) {
	cfg, err := Disabled().TLSConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg != nil {
		t.Error("Disabled produced a TLS configuration; the connection would be encrypted after all")
	}
}

func TestTheDefaultRefusesOldTLSVersions(t *testing.T) {
	for _, opts := range []*Options{Required(), Insecure()} {
		cfg, err := opts.TLSConfig()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.MinVersion < tls.VersionTLS12 {
			t.Errorf("%s allows TLS below 1.2", opts.Mode())
		}
	}
}

func TestModesAreNamedInMessages(t *testing.T) {
	for mode, want := range map[Mode]string{
		ModeRequired: "required", ModeInsecure: "insecure", ModeDisabled: "disabled",
	} {
		if got := mode.String(); got != want {
			t.Errorf("Mode(%d) = %q, want %q", mode, got, want)
		}
	}
}

func TestTheServerNameCanBeOverridden(t *testing.T) {
	// Connecting by IP address, or through a tunnel, where the address is not
	// the name on the certificate.
	ca := newAuthority(t, "AceMQ Test CA")
	addr := serve(t, ca.sign(t, "broker.test", "broker.test"))

	err := dial(t, addr, Required().
		TrustCertificateAuthority(ca.cert).
		WithServerName("broker.test"))

	if err != nil {
		t.Fatalf("overriding the server name refused a valid certificate: %v", err)
	}
}

// ---- credentials -----------------------------------------------------

func TestASecretNeverAppearsInAStringOrAnError(t *testing.T) {
	// The reason Credentials is a type. A struct printed with %v, a connection
	// logged on failure, an error wrapping the configuration — none of them
	// should put the password in a log somebody else can read.
	creds := Of("orders", "hunter2")

	if strings.Contains(creds.String(), "hunter2") {
		t.Errorf("String() leaked the secret: %s", creds.String())
	}
	if !strings.Contains(creds.String(), "orders") {
		t.Errorf("String() does not name the account: %s", creds.String())
	}
	withCreds := Required().WithCredentials(StaticCredentials("orders", "hunter2"))
	if strings.Contains(withCreds.String(), "hunter2") {
		t.Error("the options leaked the secret")
	}
}

func TestATokenHasNoUsername(t *testing.T) {
	creds := Token("ya29.abcdef")

	if creds.Username() != "" {
		t.Errorf("Username() = %q, want empty for a token", creds.Username())
	}
	if creds.Secret() != "ya29.abcdef" {
		t.Error("the token did not survive")
	}
	if strings.Contains(creds.String(), "ya29") {
		t.Errorf("String() leaked the token: %s", creds.String())
	}
}

func TestEnvironmentCredentialsAreReadEachTime(t *testing.T) {
	t.Setenv("TEST_MQ_USER", "orders")
	t.Setenv("TEST_MQ_PASSWORD", "first")

	provider := EnvironmentCredentials("TEST_MQ_USER", "TEST_MQ_PASSWORD")

	first, err := provider.Credentials()
	if err != nil {
		t.Fatal(err)
	}
	// Rotated by whatever manages the secret.
	t.Setenv("TEST_MQ_PASSWORD", "second")
	second, err := provider.Credentials()
	if err != nil {
		t.Fatal(err)
	}

	if first.Secret() != "first" || second.Secret() != "second" {
		t.Errorf("got %q then %q; a rotated password was not picked up",
			first.Secret(), second.Secret())
	}
}

func TestAMissingEnvironmentVariableSaysWhichOne(t *testing.T) {
	t.Setenv("TEST_MQ_USER", "orders")

	_, err := EnvironmentCredentials("TEST_MQ_USER", "TEST_MQ_ABSENT").Credentials()

	if err == nil {
		t.Fatal("a missing password variable was accepted")
	}
	if !strings.Contains(err.Error(), "TEST_MQ_ABSENT") {
		t.Errorf("the error does not name the variable: %v", err)
	}
}

func TestFileCredentialsTrimTheTrailingNewline(t *testing.T) {
	// Every editor and every `echo` adds one, and a password with a newline on
	// the end fails in a way nobody enjoys diagnosing.
	path := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(path, []byte("hunter2\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	creds, err := FileCredentials(path, "orders").Credentials()
	if err != nil {
		t.Fatal(err)
	}

	if creds.Secret() != "hunter2" {
		t.Errorf("Secret() = %q, want the newline trimmed", creds.Secret())
	}
	if creds.Username() != "orders" {
		t.Errorf("Username() = %q", creds.Username())
	}
}

func TestFileCredentialsCanCarryBothParts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials")
	if err := os.WriteFile(path, []byte("orders:hunter2\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	creds, err := FileCredentials(path, "").Credentials()
	if err != nil {
		t.Fatal(err)
	}

	if creds.Username() != "orders" || creds.Secret() != "hunter2" {
		t.Errorf("got %q / %q", creds.Username(), creds.Secret())
	}
}

func TestAnEmptyCredentialsFileIsReported(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty")
	if err := os.WriteFile(path, []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, err := FileCredentials(path, "orders").Credentials(); err == nil {
		t.Fatal("an empty credentials file produced an empty password rather than an error")
	}
}

func TestCredentialsReachTheOptions(t *testing.T) {
	opts := Required().WithCredentials(StaticCredentials("orders", "hunter2"))

	creds, present, err := opts.Credentials()
	if err != nil {
		t.Fatal(err)
	}
	if !present {
		t.Fatal("credentials were configured but not reported")
	}
	if creds.Username() != "orders" || creds.Secret() != "hunter2" {
		t.Errorf("got %q / %q", creds.Username(), creds.Secret())
	}

	if _, present, _ := Required().Credentials(); present {
		t.Error("credentials were reported when none were configured")
	}
}

func TestAFailingProviderStopsTheConnection(t *testing.T) {
	opts := Required().WithCredentials(CredentialsFunc(func() (Credentials, error) {
		return Credentials{}, errors.New("the secret store is unreachable")
	}))

	if _, _, err := opts.Credentials(); err == nil {
		t.Fatal("a failing credentials provider was ignored, so the connection would try anonymously")
	}
}

func TestAConfigurationFailureIsKeptRatherThanOverwritten(t *testing.T) {
	// The first failure is the useful one; later calls should not replace it
	// with something less specific.
	opts := Required().
		TrustCertificateAuthorityFile("/nonexistent/ca.crt").
		WithServerName("broker.test").
		AllowDevelopmentCertificates()

	if opts.Err() == nil {
		t.Fatal("the failure was lost by the calls after it")
	}
	if !strings.Contains(opts.Err().Error(), "ca.crt") {
		t.Errorf("the error is no longer the original one: %v", opts.Err())
	}
}
