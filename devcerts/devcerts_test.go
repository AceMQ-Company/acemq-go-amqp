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

package devcerts

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/AceMQ-Company/acemq-go-amqp/security"
)

func generate(t *testing.T, opts Options) *Result {
	t.Helper()
	if opts.Directory == "" {
		opts.Directory = filepath.Join(t.TempDir(), "certs")
	}
	result, err := Generate(opts)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// TestEverythingIsMarked is the property the whole tool exists for.
//
// A development certificate that is not marked is just a certificate, and the
// library has no way to tell it apart from a real one. Every file has to carry
// the mark, not only the authority.
func TestEverythingIsMarked(t *testing.T) {
	result := generate(t, Options{BrokerHost: "localhost"})

	for name, cert := range map[string]*x509.Certificate{
		"authority": result.Authority,
		"broker":    result.Broker,
		"client":    result.Client,
	} {
		if !security.IsDevelopmentCertificate(cert) {
			t.Errorf("the %s certificate is not marked, so the library would trust it: %s",
				name, cert.Subject)
		}
	}
}

func TestTheLibraryRefusesWhatThisToolWrites(t *testing.T) {
	// The two halves meeting: the tool writes the mark, the library reads it.
	// Testing them separately would let the string drift apart.
	result := generate(t, Options{BrokerHost: "localhost"})

	opts := security.Required().TrustCertificateAuthorityFile(
		filepath.Join(result.Directory, "ca.crt"))
	cfg, err := opts.TLSConfig()
	if err != nil {
		t.Fatal(err)
	}

	err = cfg.VerifyPeerCertificate([][]byte{result.Broker.Raw}, nil)
	if err == nil {
		t.Fatal("the library accepted a certificate this tool generated")
	}
	if !strings.Contains(err.Error(), "AllowDevelopmentCertificates") {
		t.Errorf("the refusal does not say how to permit it: %v", err)
	}
}

func TestTheBrokerCertificateChainsToTheAuthority(t *testing.T) {
	result := generate(t, Options{BrokerHost: "localhost"})

	pool := x509.NewCertPool()
	pool.AddCert(result.Authority)

	if _, err := result.Broker.Verify(x509.VerifyOptions{
		Roots:     pool,
		DNSName:   "localhost",
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Fatalf("the broker certificate does not verify against its own authority: %v", err)
	}
}

func TestLocalhostCoversTheLoopbackAddressesToo(t *testing.T) {
	// A broker reached as localhost is very often reached as 127.0.0.1 as well,
	// and a certificate covering only one fails in a way that reads like a
	// trust problem rather than a naming one.
	result := generate(t, Options{BrokerHost: "localhost"})

	pool := x509.NewCertPool()
	pool.AddCert(result.Authority)

	for _, name := range []string{"localhost", "127.0.0.1", "::1"} {
		if _, err := result.Broker.Verify(x509.VerifyOptions{
			Roots:     pool,
			DNSName:   name,
			KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		}); err != nil {
			t.Errorf("the certificate does not cover %s: %v", name, err)
		}
	}
}

func TestAnIPAddressCanBeTheBrokerName(t *testing.T) {
	result := generate(t, Options{BrokerHost: "192.168.1.50"})

	pool := x509.NewCertPool()
	pool.AddCert(result.Authority)

	if _, err := result.Broker.Verify(x509.VerifyOptions{
		Roots:     pool,
		DNSName:   "192.168.1.50",
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err != nil {
		t.Fatalf("a broker named by IP address does not verify: %v", err)
	}
}

func TestTheKeysLoadAsAPairWithTheirCertificates(t *testing.T) {
	// The pairing is what a broker and a client actually need. A certificate
	// written beside the wrong key is a failure that only appears at handshake.
	result := generate(t, Options{BrokerHost: "localhost"})

	for _, pair := range [][2]string{
		{"server.crt", "server.key"},
		{"client.crt", "client.key"},
	} {
		_, err := tls.LoadX509KeyPair(
			filepath.Join(result.Directory, pair[0]),
			filepath.Join(result.Directory, pair[1]))
		if err != nil {
			t.Errorf("%s and %s are not a pair: %v", pair[0], pair[1], err)
		}
	}
}

func TestKeysAreNotWorldReadable(t *testing.T) {
	// A development key is still a key, and one left world-readable in a
	// checkout is a habit worth not forming.
	result := generate(t, Options{BrokerHost: "localhost"})

	for _, name := range []string{"ca.key", "server.key", "client.key"} {
		info, err := os.Stat(filepath.Join(result.Directory, name))
		if err != nil {
			t.Fatal(err)
		}
		if mode := info.Mode().Perm(); mode&0o077 != 0 {
			t.Errorf("%s is mode %04o, want it readable only by its owner", name, mode)
		}
	}
}

func TestTheValidityIsWhatWasAsked(t *testing.T) {
	result := generate(t, Options{BrokerHost: "localhost", Validity: 7 * 24 * time.Hour})

	remaining := time.Until(result.Broker.NotAfter)
	if remaining > 8*24*time.Hour || remaining < 6*24*time.Hour {
		t.Errorf("the certificate expires in %s, want about seven days", remaining)
	}
}

func TestABrokerConfigIsWrittenUnlessRefused(t *testing.T) {
	with := generate(t, Options{BrokerHost: "localhost"})
	if with.BrokerConf == "" {
		t.Error("no rabbitmq.conf was written")
	}
	conf, err := os.ReadFile(with.BrokerConf)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"listeners.ssl.default", "ssl_options.cacertfile", "/certs/server.crt"} {
		if !strings.Contains(string(conf), want) {
			t.Errorf("rabbitmq.conf does not mention %s:\n%s", want, conf)
		}
	}

	without := generate(t, Options{BrokerHost: "localhost", NoBrokerConfig: true})
	if without.BrokerConf != "" {
		t.Error("rabbitmq.conf was written despite NoBrokerConfig")
	}
}

func TestTheBrokerConfigPointsWhereItIsTold(t *testing.T) {
	conf := BrokerConfig("/etc/acemq/certs")

	if !strings.Contains(conf, "/etc/acemq/certs/ca.crt") {
		t.Errorf("the configuration does not use the given path:\n%s", conf)
	}
}

func TestRegeneratingReplacesRatherThanFails(t *testing.T) {
	// These expire in weeks by design, so running the tool again is the normal
	// way to deal with that.
	dir := filepath.Join(t.TempDir(), "certs")

	first := generate(t, Options{Directory: dir, BrokerHost: "localhost"})
	second := generate(t, Options{Directory: dir, BrokerHost: "localhost"})

	if first.Authority.SerialNumber.Cmp(second.Authority.SerialNumber) == 0 {
		t.Error("the second run produced the same authority, so nothing was regenerated")
	}
}

func TestDefaultsAreUsedForAnEmptyOptions(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "certs")

	result := generate(t, Options{Directory: dir})

	if result.BrokerHost != "localhost" {
		t.Errorf("BrokerHost = %q, want localhost", result.BrokerHost)
	}
	if len(result.Files) < 6 {
		t.Errorf("wrote %d files, want the six certificates and keys", len(result.Files))
	}
}
