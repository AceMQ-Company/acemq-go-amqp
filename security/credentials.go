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
	"fmt"
	"os"
	"strings"
)

// Credentials are a username and a secret for the broker.
//
// The secret never appears in [Credentials.String], so a credential logged by
// accident — in a struct dump, an error, a %v — prints the username and nothing
// else. That is the whole reason this is a type rather than two strings.
type Credentials struct {
	username string
	secret   string
}

// Of builds credentials from a username and password.
func Of(username, password string) Credentials {
	return Credentials{username: username, secret: password}
}

// Token builds credentials that are a bearer token with no username, which is
// how OAuth 2 against RabbitMQ is presented.
func Token(token string) Credentials { return Credentials{secret: token} }

// Username is the account name.
func (c Credentials) Username() string { return c.username }

// Secret is the password or token. Do not log the result.
func (c Credentials) Secret() string { return c.secret }

// IsZero reports whether these credentials carry nothing.
func (c Credentials) IsZero() bool { return c.username == "" && c.secret == "" }

// String never includes the secret.
func (c Credentials) String() string {
	if c.username == "" {
		return "Credentials{token}"
	}
	return "Credentials{" + c.username + "}"
}

// CredentialsProvider supplies credentials when a connection is made.
//
// It is an interface rather than a value so a secret can be re-read at the
// moment it is needed. A password rotated by a sidecar is only useful if
// something asks for it again.
type CredentialsProvider interface {
	Credentials() (Credentials, error)
}

// StaticCredentials returns the same credentials every time.
//
// Fine for a development broker. For anything else the password is now in the
// binary, or in whatever built it.
func StaticCredentials(username, password string) CredentialsProvider {
	return staticProvider{Of(username, password)}
}

type staticProvider struct{ creds Credentials }

func (p staticProvider) Credentials() (Credentials, error) { return p.creds, nil }

// EnvironmentCredentials reads two environment variables each time it is asked.
func EnvironmentCredentials(usernameVar, passwordVar string) CredentialsProvider {
	return envProvider{usernameVar: usernameVar, passwordVar: passwordVar}
}

type envProvider struct{ usernameVar, passwordVar string }

func (p envProvider) Credentials() (Credentials, error) {
	username, ok := os.LookupEnv(p.usernameVar)
	if !ok {
		return Credentials{}, fmt.Errorf(
			"acemq: the environment variable %s is not set, so there is no broker username", p.usernameVar)
	}
	password, ok := os.LookupEnv(p.passwordVar)
	if !ok {
		return Credentials{}, fmt.Errorf(
			"acemq: the environment variable %s is not set, so there is no broker password", p.passwordVar)
	}
	return Of(username, password), nil
}

// FileCredentials reads a secret from a file each time it is asked, which is
// how a mounted Kubernetes secret or a Docker secret arrives.
//
// The file holds the password alone, or "username:password" when it carries
// both. Trailing whitespace is trimmed, because a file written by an editor
// almost always ends in a newline and a password with a newline on the end
// fails in a way nobody enjoys diagnosing.
func FileCredentials(path, username string) CredentialsProvider {
	return fileProvider{path: path, username: username}
}

type fileProvider struct {
	path     string
	username string
}

func (p fileProvider) Credentials() (Credentials, error) {
	raw, err := os.ReadFile(p.path)
	if err != nil {
		return Credentials{}, fmt.Errorf("acemq: cannot read the credentials file %s: %w", p.path, err)
	}

	contents := strings.TrimRight(string(raw), " \t\r\n")
	if contents == "" {
		return Credentials{}, fmt.Errorf("acemq: the credentials file %s is empty", p.path)
	}

	if p.username != "" {
		return Of(p.username, contents), nil
	}
	username, password, found := strings.Cut(contents, ":")
	if !found {
		return Credentials{}, fmt.Errorf(
			"acemq: the credentials file %s holds no username, and none was given; "+
				"write it as username:password or pass the username to FileCredentials", p.path)
	}
	return Of(username, password), nil
}

// CredentialsFunc adapts a function to a [CredentialsProvider].
type CredentialsFunc func() (Credentials, error)

// Credentials calls the function.
func (f CredentialsFunc) Credentials() (Credentials, error) { return f() }
