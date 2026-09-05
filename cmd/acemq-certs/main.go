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

// Command acemq-certs generates the certificates needed to talk to a broker
// over TLS on your own machine, so nobody has to remember an openssl
// incantation.
//
//	go install github.com/AceMQ-Company/acemq-go-amqp/cmd/acemq-certs@latest
//	acemq-certs --out certs --broker localhost --days 30
//
// Everything it writes is stamped ACEMQ DEVELOPMENT ONLY - DO NOT TRUST, and
// the library refuses a certificate carrying that mark unless
// AllowDevelopmentCertificates says otherwise. These certificates cannot
// quietly end up in front of a real broker.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/AceMQ-Company/acemq-go-amqp/devcerts"
)

func main() {
	defaults := devcerts.Defaults()

	out := flag.String("out", defaults.Directory, "where to write them")
	broker := flag.String("broker", defaults.BrokerHost, "the name the server certificate names")
	days := flag.Int("days", int(defaults.Validity.Hours()/24), "how long they are valid")
	brokerCerts := flag.String("broker-certs", defaults.BrokerCertPath,
		"the path rabbitmq.conf points at, inside the broker")
	noConfig := flag.Bool("no-broker-config", false, "do not write rabbitmq.conf")

	flag.Usage = func() {
		fmt.Fprint(os.Stderr, `acemq-certs — certificates for talking to a broker over TLS locally

Usage:
  acemq-certs [flags]

Flags:
  --out <dir>          where to write them              (default: certs)
  --broker <host>      the name the server cert names   (default: localhost)
  --days <n>           how long they are valid          (default: 30)
  --broker-certs <d>   the path rabbitmq.conf points at (default: /certs)
  --no-broker-config   do not write rabbitmq.conf

Everything written is stamped "ACEMQ DEVELOPMENT ONLY - DO NOT TRUST" and is
refused by the library unless AllowDevelopmentCertificates() is called.
`)
	}
	flag.Parse()

	if *days < 1 {
		fmt.Fprintln(os.Stderr, "acemq-certs: --days must be at least 1")
		os.Exit(2)
	}

	result, err := devcerts.Generate(devcerts.Options{
		Directory:      *out,
		BrokerHost:     *broker,
		Validity:       time.Duration(*days) * 24 * time.Hour,
		BrokerCertPath: *brokerCerts,
		NoBrokerConfig: *noConfig,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	fmt.Printf("Written to %s, valid until %s:\n\n",
		result.Directory, result.Expiry.Format(time.RFC1123))
	fmt.Println("  ca.crt       trust this: security.Required().TrustCertificateAuthorityFile(\"ca.crt\")")
	fmt.Println("  ca.key       the authority's key")
	fmt.Println("  server.crt   the broker's certificate")
	fmt.Println("  server.key   the broker's key")
	fmt.Println("  client.crt   for EXTERNAL authentication")
	fmt.Println("  client.key   the client's key")
	if result.BrokerConf != "" {
		fmt.Println("  rabbitmq.conf mount at /etc/rabbitmq/rabbitmq.conf")
	}

	// An absolute --out already names the mount; prefixing $PWD to it produces a
	// path that does not exist and a command that cannot be pasted.
	mount := result.Directory
	if !filepath.IsAbs(mount) {
		mount = "$PWD/" + mount
	}

	fmt.Printf(`
A broker using them:

  docker run -d --name acemq-tls -p 5671:5671 -p 5672:5672 \
    -v "%s:%s:ro" \
    -v "%s/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro" \
    rabbitmq:4-alpine

Connecting to it:

  mq, err := acemq.Connect(ctx, "amqps://guest:guest@%s:5671/",
      acemq.WithSecurity(security.Required().
          TrustCertificateAuthorityFile("%s/ca.crt").
          AllowDevelopmentCertificates()))

AllowDevelopmentCertificates is required. Without it these are refused, which
is what stops them reaching anything real.
`, mount, "/certs", mount, result.BrokerHost, result.Directory)
}
