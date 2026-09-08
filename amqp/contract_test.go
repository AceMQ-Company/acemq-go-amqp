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

package acemq

// The cross-language conformance suite.
//
// Every test in this file reads internal/testdata/contract-fixtures.json, which
// the Java library generates and every AceMQ library carries byte for byte, and
// holds this library to it.
//
// It exists because the alternative failed. Java shipped exponential() with a
// multiplier of five and ten percent jitter through ten releases while Go, .NET,
// Python and Ruby all doubled with twenty percent: exponential(5, 1s, 1m) gave
// 1s, 5s, 25s, 60s in Java and 1s, 2s, 4s, 8s here. Nobody noticed for months,
// because each library tested its own arithmetic against its own expectations
// and every suite was green. Three more divergences turned up the same way in a
// single day — a rung's dead-letter exchange, a source queue's dead-letter
// arguments, and a queue's type — and all four were found by a person reading
// five codebases side by side rather than by any test.
//
// So the numbers here come out of the file rather than out of this repository,
// and wherever an expectation can be derived twice it is: the doubling is
// checked by asserting each delay is twice its predecessor, the threshold table
// is recomputed from the stated rule, and the rung names are rendered a second
// time by an obvious implementation beside the real one. A test that reads a
// number out of the fixture and compares it against the same number read out of
// the code under test proves only that both can read.
//
// Two disagreements are recorded rather than resolved, each with a test of its
// own that says what this library does and what the file says:
// TestContractSubSecondRungNamesDisagreeAcrossLanguages and
// TestContractNoPolicyHasADefaultAgeLimitInAnyLanguage.

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ------------------------------------------------------------------ the file

type contractFixtures struct {
	GeneratedBy                      string             `json:"generatedBy"`
	Contract                         string             `json:"contract"`
	DefaultBrokerWaitThresholdMillis int64              `json:"defaultBrokerWaitThresholdMillis"`
	DeadLetterExchange               string             `json:"deadLetterExchange"`
	RetryExchange                    string             `json:"retryExchange"`
	RetrySchedules                   []contractSchedule `json:"retrySchedules"`
	Jitter                           contractJitter     `json:"jitter"`
	BrokerWaitThreshold              contractThreshold  `json:"brokerWaitThreshold"`
	Naming                           contractNaming     `json:"naming"`
	RungArguments                    []contractRung     `json:"rungArguments"`
	Topology                         contractTopology   `json:"topology"`
	QueueTypeDefaults                contractQueueTypes `json:"queueTypeDefaults"`
}

type contractSchedule struct {
	Name                      string             `json:"name"`
	How                       string             `json:"how"`
	MaxAttempts               int                `json:"maxAttempts"`
	MaxMessageAgeMillis       int64              `json:"maxMessageAgeMillis"`
	HasMaxMessageAge          bool               `json:"hasMaxMessageAge"`
	JitterFactor              float64            `json:"jitterFactor"`
	BrokerWaitThresholdMillis int64              `json:"brokerWaitThresholdMillis"`
	ScheduleMillis            []int64            `json:"scheduleMillis"`
	BrokerRungDelaysMillis    []int64            `json:"brokerRungDelaysMillis"`
	Decisions                 []contractDecision `json:"decisions"`
}

type contractDecision struct {
	Attempt          int   `json:"attempt"`
	MessageAgeMillis int64 `json:"messageAgeMillis"`
	Retries          bool  `json:"retries"`
}

type contractJitter struct {
	Policy                  string  `json:"policy"`
	Factor                  float64 `json:"factor"`
	AppliesInBothDirections bool    `json:"appliesInBothDirections"`
	MinMultiplier           float64 `json:"minMultiplier"`
	MaxMultiplier           float64 `json:"maxMultiplier"`
	AppliesToConsumerWaits  bool    `json:"appliesToConsumerWaits"`
	AppliesToBrokerWaits    bool    `json:"appliesToBrokerWaits"`
	FlooredAtMillis         int64   `json:"flooredAtMillis"`
	HowToAssert             string  `json:"howToAssert"`
}

type contractThreshold struct {
	DefaultMillis int64                   `json:"defaultMillis"`
	Rule          string                  `json:"rule"`
	Cases         []contractThresholdCase `json:"cases"`
}

type contractThresholdCase struct {
	ThresholdMillis int64 `json:"thresholdMillis"`
	DelayMillis     int64 `json:"delayMillis"`
	WaitsInBroker   bool  `json:"waitsInBroker"`
}

type contractNaming struct {
	Queue                 string              `json:"queue"`
	DeadLetterQueue       string              `json:"deadLetterQueue"`
	ParkedQueue           string              `json:"parkedQueue"`
	RetryQueues           []contractRetryName `json:"retryQueues"`
	SubSecondDisagreement string              `json:"subSecondDisagreement"`
}

type contractRetryName struct {
	DelayMillis int64  `json:"delayMillis"`
	Queue       string `json:"queue"`
}

type contractRung struct {
	Queue       string         `json:"queue"`
	DelayMillis int64          `json:"delayMillis"`
	Arguments   map[string]any `json:"arguments"`
}

type contractTopology struct {
	SourceQueue            string                    `json:"sourceQueue"`
	Policy                 string                    `json:"policy"`
	PolicyScheduleMillis   []int64                   `json:"policyScheduleMillis"`
	PolicyRungDelaysMillis []int64                   `json:"policyRungDelaysMillis"`
	Exchanges              []contractTopologyExchang `json:"exchanges"`
	Queues                 []contractTopologyQueue   `json:"queues"`
	Bindings               []contractTopologyBinding `json:"bindings"`
}

type contractTopologyExchang struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Durable    bool   `json:"durable"`
	DeclaredBy string `json:"declaredBy"`
}

type contractTopologyQueue struct {
	Name       string         `json:"name"`
	Type       string         `json:"type"`
	Durable    bool           `json:"durable"`
	Arguments  map[string]any `json:"arguments"`
	DeclaredBy string         `json:"declaredBy"`
}

type contractTopologyBinding struct {
	Queue      string `json:"queue"`
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routingKey"`
	DeclaredBy string `json:"declaredBy"`
}

type contractQueueTypes struct {
	SourceQueue                        string                `json:"sourceQueue"`
	SourceQueueWithDeadLetter          string                `json:"sourceQueueWithDeadLetter"`
	ExplicitClassicQueue               string                `json:"explicitClassicQueue"`
	ExplicitClassicQueueWithDeadLetter string                `json:"explicitClassicQueueWithDeadLetter"`
	DeadLetterQueue                    string                `json:"deadLetterQueue"`
	ParkedQueue                        string                `json:"parkedQueue"`
	RetryRung                          string                `json:"retryRung"`
	Durability                         contractQueueDurabile `json:"durability"`
}

type contractQueueDurabile struct {
	EveryDeclaredQueueIsDurable    bool   `json:"everyDeclaredQueueIsDurable"`
	ExclusiveAutoDeleteOrTransient string `json:"exclusiveAutoDeleteOrTransient"`
}

// loadContract reads the file every library carries a copy of.
//
// It fails rather than skips when the file is missing. A conformance suite that
// quietly passes because it could not find what it was conforming to is the
// failure mode this whole exercise exists to remove.
func loadContract(t *testing.T) contractFixtures {
	t.Helper()
	raw, err := os.ReadFile("../internal/testdata/contract-fixtures.json")
	if err != nil {
		t.Fatalf("cannot read the contract fixtures: %v", err)
	}
	var f contractFixtures
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("cannot parse the contract fixtures: %v", err)
	}
	if len(f.RetrySchedules) == 0 || len(f.RungArguments) == 0 || len(f.Topology.Queues) == 0 {
		t.Fatal("the contract fixtures are empty, so these tests prove nothing")
	}
	return f
}

// contractPolicy builds the Go policy the fixture's own "how" describes.
//
// Read rather than mapped by name, so that a policy added to the file is built
// from what the file says about it. exponential(5, 1s, 24h).giveUpAfter(2m) is
// the widest form here.
func contractPolicy(t *testing.T, how string) RetryPolicy {
	t.Helper()

	base := how
	var giveUp time.Duration
	if at := strings.Index(how, ").giveUpAfter("); at >= 0 {
		base = how[:at+1]
		giveUp = contractDuration(t, strings.TrimSuffix(how[at+len(").giveUpAfter("):], ")"))
	}

	open := strings.Index(base, "(")
	if open < 0 || !strings.HasSuffix(base, ")") {
		t.Fatalf("cannot read the policy %q out of the fixture", how)
	}
	name := base[:open]
	var args []string
	if inner := strings.TrimSpace(base[open+1 : len(base)-1]); inner != "" {
		for _, arg := range strings.Split(inner, ",") {
			args = append(args, strings.TrimSpace(arg))
		}
	}

	var policy RetryPolicy
	switch name {
	case "exponential":
		if len(args) != 3 {
			t.Fatalf("exponential takes three arguments, %q has %d", how, len(args))
		}
		policy = ExponentialRetry(
			contractInt(t, args[0]), contractDuration(t, args[1]), contractDuration(t, args[2]))
	case "fixed":
		if len(args) != 2 {
			t.Fatalf("fixed takes two arguments, %q has %d", how, len(args))
		}
		policy = FixedRetry(contractInt(t, args[0]), contractDuration(t, args[1]))
	case "none":
		policy = NoRetry()
	default:
		t.Fatalf("the fixture names a policy %q this library does not have", name)
	}
	if giveUp > 0 {
		policy = policy.GiveUpAfter(giveUp)
	}
	return policy
}

func contractDuration(t *testing.T, text string) time.Duration {
	t.Helper()
	d, err := time.ParseDuration(text)
	if err != nil {
		t.Fatalf("cannot read %q as a duration: %v", text, err)
	}
	return d
}

func contractInt(t *testing.T, text string) int {
	t.Helper()
	n, err := strconv.Atoi(text)
	if err != nil {
		t.Fatalf("cannot read %q as a number: %v", text, err)
	}
	return n
}

func contractMillis(delays []time.Duration) []int64 {
	out := make([]int64, 0, len(delays))
	for _, d := range delays {
		out = append(out, d.Milliseconds())
	}
	return out
}

func sameMillis(got, want []int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// contractArgs makes two argument tables comparable.
//
// JSON has one number type, so an x-message-ttl that is an int64 here arrives
// from the file as a float64; and x-queue-type is carried by the fixture as a
// queue's "type" field rather than as an argument, so it is dropped here and
// asserted separately. Everything else is compared exactly.
func contractArgs(args map[string]any) map[string]string {
	out := make(map[string]string, len(args))
	for name, value := range args {
		if name == ArgQueueType {
			continue
		}
		switch n := value.(type) {
		case float64:
			out[name] = strconv.FormatInt(int64(n), 10)
		case float32:
			out[name] = strconv.FormatInt(int64(n), 10)
		case int:
			out[name] = strconv.FormatInt(int64(n), 10)
		case int32:
			out[name] = strconv.FormatInt(int64(n), 10)
		case int64:
			out[name] = strconv.FormatInt(n, 10)
		default:
			out[name] = fmt.Sprint(value)
		}
	}
	return out
}

func sameArgs(got, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for name, value := range want {
		if got[name] != value {
			return false
		}
	}
	return true
}

func showArgs(args map[string]string) string {
	names := make([]string, 0, len(args))
	for name := range args {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+"="+args[name])
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// ------------------------------------------------------------- the constants

func TestContractNamesTheSameTwoExchangesAndTheSameThreshold(t *testing.T) {
	fixtures := loadContract(t)

	if RetryExchange != fixtures.RetryExchange {
		t.Errorf("the retry exchange is %q here and %q in the contract",
			RetryExchange, fixtures.RetryExchange)
	}
	if DeadLetterExchange != fixtures.DeadLetterExchange {
		t.Errorf("the dead-letter exchange is %q here and %q in the contract",
			DeadLetterExchange, fixtures.DeadLetterExchange)
	}
	if got := DefaultBrokerWaitThreshold.Milliseconds(); got != fixtures.DefaultBrokerWaitThresholdMillis {
		t.Errorf("the default broker wait threshold is %dms here and %dms in the contract",
			got, fixtures.DefaultBrokerWaitThresholdMillis)
	}
	if fixtures.BrokerWaitThreshold.DefaultMillis != fixtures.DefaultBrokerWaitThresholdMillis {
		t.Errorf("the fixture disagrees with itself about the default threshold: %d and %d",
			fixtures.BrokerWaitThreshold.DefaultMillis, fixtures.DefaultBrokerWaitThresholdMillis)
	}
}

// ------------------------------------------------------------------ schedules

func TestContractRetrySchedulesAreTheOnesEveryLibraryProduces(t *testing.T) {
	fixtures := loadContract(t)

	if len(fixtures.RetrySchedules) != 5 {
		t.Fatalf("the contract names %d policies, expected the five it was written with",
			len(fixtures.RetrySchedules))
	}

	for _, want := range fixtures.RetrySchedules {
		t.Run(want.Name, func(t *testing.T) {
			policy := contractPolicy(t, want.How)

			if policy.MaxAttempts != want.MaxAttempts {
				t.Errorf("%s allows %d attempts here and %d in the contract",
					want.How, policy.MaxAttempts, want.MaxAttempts)
			}
			if policy.JitterFactor != want.JitterFactor {
				t.Errorf("%s jitters by %v here and by %v in the contract",
					want.How, policy.JitterFactor, want.JitterFactor)
			}
			if got := policy.BrokerWaitThreshold.Milliseconds(); got != want.BrokerWaitThresholdMillis {
				t.Errorf("%s moves waiting to the broker at %dms here and at %dms in the contract",
					want.How, got, want.BrokerWaitThresholdMillis)
			}

			schedule := contractMillis(policy.Schedule())
			if !sameMillis(schedule, want.ScheduleMillis) {
				t.Errorf("%s waits %v here and %v in the contract",
					want.How, schedule, want.ScheduleMillis)
			}

			// A schedule is the delays before every attempt after the first, so
			// its length follows from the attempt count and nothing else.
			if len(schedule) != max(0, want.MaxAttempts-1) {
				t.Errorf("%s has %d delays for %d attempts",
					want.How, len(schedule), want.MaxAttempts)
			}

			rungs := contractMillis(policy.BrokerRungs())
			if !sameMillis(rungs, want.BrokerRungDelaysMillis) {
				t.Errorf("%s needs rungs for %v here and for %v in the contract",
					want.How, rungs, want.BrokerRungDelaysMillis)
			}

			// The rungs derived a second way: the distinct delays of the schedule
			// that reach the threshold, in the order they first appear. Compared
			// against the file rather than against BrokerRungs, so agreeing with
			// the file is not the same as agreeing with the code that produced it.
			var recomputed []int64
			seen := map[int64]bool{}
			for _, delay := range want.ScheduleMillis {
				if delay <= 0 || want.BrokerWaitThresholdMillis <= 0 ||
					delay < want.BrokerWaitThresholdMillis || seen[delay] {
					continue
				}
				seen[delay] = true
				recomputed = append(recomputed, delay)
			}
			if !sameMillis(recomputed, want.BrokerRungDelaysMillis) {
				t.Errorf("%s: the rungs recomputed from the schedule are %v, "+
					"the contract records %v", want.How, recomputed, want.BrokerRungDelaysMillis)
			}
		})
	}
}

// TestContractExponentialDoubles derives the shape of the schedule rather than
// reading it, which is the assertion that would have caught the divergence this
// suite was written for: Java multiplied by five where everything else doubled,
// and both libraries' own tests passed because each asserted its own numbers.
func TestContractExponentialDoublesAndFixedDoesNot(t *testing.T) {
	fixtures := loadContract(t)

	for _, want := range fixtures.RetrySchedules {
		if len(want.ScheduleMillis) < 2 {
			continue
		}
		policy := contractPolicy(t, want.How)
		ceiling := int64(0)
		for _, delay := range want.ScheduleMillis {
			if delay > ceiling {
				ceiling = delay
			}
		}

		for i := 1; i < len(want.ScheduleMillis); i++ {
			previous, current := want.ScheduleMillis[i-1], want.ScheduleMillis[i]
			switch {
			case strings.HasPrefix(want.How, "exponential"):
				// Twice the one before it, or the ceiling once doubling has run
				// into it. Nothing else is a doubling schedule.
				if current != 2*previous && current != ceiling {
					t.Errorf("%s: delay %d is %dms, neither twice the %dms before it "+
						"nor the %dms ceiling", want.How, i+1, current, previous, ceiling)
				}
			case strings.HasPrefix(want.How, "fixed"):
				if current != previous {
					t.Errorf("%s: delay %d is %dms where the one before it was %dms; "+
						"fixed does not grow", want.How, i+1, current, previous)
				}
			}
		}

		// And the same shape out of this library, not only out of the file.
		delays := policy.Schedule()
		for i := 1; i < len(delays); i++ {
			previous, current := delays[i-1], delays[i]
			if strings.HasPrefix(want.How, "exponential") &&
				current != 2*previous && current != policy.MaxDelay {
				t.Errorf("%s: this library's delay %d is %s, neither twice the %s "+
					"before it nor the %s ceiling", want.How, i+1, current, previous, policy.MaxDelay)
			}
		}
	}
}

// TestContractRetryDecisions walks the attempt and age limits.
//
// The policy is built from the fixture's own maxMessageAgeMillis rather than
// from the constructor's default, because those two differ: Java's constructors
// carry a 365-day age limit and this library's carry none. That divergence is
// pinned on its own in TestContractNoPolicyHasADefaultAgeLimitInAnyLanguage
// rather than hidden here — what this test is for is the rule itself, which is
// the same rule in both.
func TestContractRetryDecisionsMatchAtEveryLimit(t *testing.T) {
	fixtures := loadContract(t)

	for _, schedule := range fixtures.RetrySchedules {
		t.Run(schedule.Name, func(t *testing.T) {
			if len(schedule.Decisions) == 0 {
				t.Fatal("a policy with no decisions recorded proves nothing")
			}
			policy := contractPolicy(t, schedule.How).
				GiveUpAfter(time.Duration(schedule.MaxMessageAgeMillis) * time.Millisecond)

			for _, decision := range schedule.Decisions {
				age := time.Duration(decision.MessageAgeMillis) * time.Millisecond

				// The rule recomputed from the two limits the fixture states,
				// so the file is checked as well as the library.
				//
				// An age limit of zero means there is no age limit, which is why
				// the fixture carries hasMaxMessageAge beside the number: read
				// the zero as a limit and every age is past it, so a policy that
				// retries for ever looks like one that retries never. Java used
				// to sidestep this by defaulting to 365 days; it now defaults to
				// zero like everyone else, and the flag is how the file says
				// which zero it means.
				withinAge := !schedule.HasMaxMessageAge ||
					decision.MessageAgeMillis < schedule.MaxMessageAgeMillis
				expected := decision.Attempt < schedule.MaxAttempts && withinAge
				if expected != decision.Retries {
					t.Errorf("attempt %d at %dms: the contract says retries=%v, but "+
						"attempts<%d and (no age limit=%v, age<%dms) gives %v",
						decision.Attempt, decision.MessageAgeMillis, decision.Retries,
						schedule.MaxAttempts, !schedule.HasMaxMessageAge,
						schedule.MaxMessageAgeMillis, expected)
				}

				wait, again := policy.NextWait(decision.Attempt, age)
				if again != decision.Retries {
					t.Errorf("attempt %d at %dms: this library retries=%v, the contract says %v",
						decision.Attempt, decision.MessageAgeMillis, again, decision.Retries)
					continue
				}
				if !again {
					continue
				}
				// A retry that happens must be the delay the schedule names,
				// once jitter is allowed for.
				scheduled := schedule.ScheduleMillis[decision.Attempt-1]
				if wait.InBroker {
					if wait.Delay.Milliseconds() != scheduled {
						t.Errorf("attempt %d waits %s on a rung, the schedule says %dms",
							decision.Attempt, wait.Delay, scheduled)
					}
					continue
				}
				low := float64(scheduled) * (1 - schedule.JitterFactor)
				high := float64(scheduled) * (1 + schedule.JitterFactor)
				if got := float64(wait.Delay.Milliseconds()); got < low-1 || got > high+1 {
					t.Errorf("attempt %d waits %s, outside [%.0fms, %.0fms]",
						decision.Attempt, wait.Delay, low, high)
				}
			}
		})
	}
}

// TestContractNoPolicyHasADefaultAgeLimitInAnyLanguage records a real
// disagreement rather than resolving it.
//
// The fixture reports maxMessageAgeMillis as 31536000000 — 365 days — for every
// policy that never called giveUpAfter, because that is what Java's constructors
// carry. This library, and the Python, Ruby and .NET ones, treat a zero age
// limit as no limit at all, so a policy here still retries a message older than
// a year. Four libraries against one makes Java the odd one out, and the file
// records Java's number because Java generates the file.
//
// Nothing reachable turns on it: a message a year old has outlived every queue
// it could be sitting in. It is written down because an undocumented difference
// is the one that surfaces in production, and because whoever settles it should
// be changing one library rather than four.
func TestContractNoPolicyHasADefaultAgeLimitInAnyLanguage(t *testing.T) {
	fixtures := loadContract(t)

	// Java used to default every policy to a 365-day age limit while Go, .NET,
	// Python and Ruby defaulted to none, so a message exactly a year old was
	// abandoned here and retried there. Java now agrees that zero means no
	// limit, and this asserts the agreement rather than the divergence.
	//
	// The previous version of this test looped over schedules whose limit
	// equalled 365 days. There are none now, so it passed without executing its
	// body -- which is the failure this whole suite exists to catch, in the
	// suite itself.
	checked := 0
	for _, schedule := range fixtures.RetrySchedules {
		if schedule.HasMaxMessageAge {
			continue // asked for a limit of its own; covered elsewhere
		}
		if schedule.MaxMessageAgeMillis != 0 {
			t.Errorf("%s has no age limit but records %dms; zero is how the "+
				"fixture says unlimited", schedule.Name, schedule.MaxMessageAgeMillis)
		}
		policy := contractPolicy(t, schedule.How)
		if policy.MaxMessageAge != 0 {
			t.Errorf("%s carries a %s age limit here, but the contract says none",
				schedule.How, policy.MaxMessageAge)
		}
		if schedule.MaxAttempts < 2 {
			continue // none() stops on attempts before age can matter
		}
		aYear := 365 * 24 * time.Hour
		if _, again := policy.NextWait(1, aYear); !again {
			t.Errorf("%s gave up on a year-old message; with no age limit it "+
				"should still retry", schedule.How)
		}
		checked++
	}

	if checked == 0 {
		t.Fatal("no unlimited policy was exercised, so this test proved nothing")
	}
}

// --------------------------------------------------------------------- jitter

// TestContractJitter samples this library rather than reading the numbers back.
//
// A port cannot compare one random number with another, so what the fixture
// pins is the factor, the two multipliers it implies, the floor, and the fact
// that a wait held on a rung is not jittered at all. Every one of those is
// checked here by rolling this library's own jitter a great many times and
// looking at where the results landed.
func TestContractJitterStaysInsideTheContractedBounds(t *testing.T) {
	fixtures := loadContract(t)
	jitter := fixtures.Jitter

	if jitter.MinMultiplier != 1-jitter.Factor || jitter.MaxMultiplier != 1+jitter.Factor {
		t.Fatalf("the contract's multipliers [%v, %v] are not 1±%v",
			jitter.MinMultiplier, jitter.MaxMultiplier, jitter.Factor)
	}
	if !jitter.AppliesInBothDirections || !jitter.AppliesToConsumerWaits || jitter.AppliesToBrokerWaits {
		t.Fatalf("this test is written for jitter that moves consumer waits both ways "+
			"and leaves broker waits alone; the contract now says %+v", jitter)
	}

	var named *contractSchedule
	for i := range fixtures.RetrySchedules {
		if fixtures.RetrySchedules[i].Name == jitter.Policy {
			named = &fixtures.RetrySchedules[i]
		}
	}
	if named == nil {
		t.Fatalf("the contract jitters %q, which is not one of its policies", jitter.Policy)
	}

	policy := contractPolicy(t, named.How)
	if policy.JitterFactor != jitter.Factor {
		t.Fatalf("%s jitters by %v here and by %v in the contract",
			named.How, policy.JitterFactor, jitter.Factor)
	}

	// Attempt one of this policy waits a second, well under the threshold, so
	// the wait is spent here and is jittered.
	scheduled := float64(named.ScheduleMillis[0])
	low := scheduled * jitter.MinMultiplier
	high := scheduled * jitter.MaxMultiplier

	const rolls = 20000
	observedLow, observedHigh := scheduled*10, 0.0
	below, above := 0, 0
	for i := 0; i < rolls; i++ {
		wait, again := policy.NextWait(1, 0)
		if !again {
			t.Fatalf("%s did not retry a first failure", named.How)
		}
		if wait.InBroker {
			t.Fatalf("a %s wait was sent to the broker; the threshold is %s",
				wait.Delay, policy.BrokerWaitThreshold)
		}
		got := float64(wait.Delay.Milliseconds())
		if got < low-1 || got > high+1 {
			t.Fatalf("a jittered wait of %s is outside [%.0fms, %.0fms]", wait.Delay, low, high)
		}
		if got < float64(jitter.FlooredAtMillis) {
			t.Fatalf("a jittered wait of %s is below the contracted floor of %dms",
				wait.Delay, jitter.FlooredAtMillis)
		}
		switch {
		case got < scheduled:
			below++
		case got > scheduled:
			above++
		}
		observedLow = minFloat(observedLow, got)
		observedHigh = maxFloat(observedHigh, got)
	}

	// Both directions, sampled. One-sided jitter only ever delays, which turns a
	// thundering herd into a slower thundering herd, and would show up here as
	// nothing at all below the scheduled delay.
	if below == 0 || above == 0 {
		t.Errorf("of %d rolls, %d landed short and %d long; the contract says jitter "+
			"moves a wait both ways", rolls, below, above)
	}

	// And the factor itself, derived from where the rolls landed rather than
	// read back. Twenty thousand uniform samples reach within a fiftieth of
	// each end of the range with a probability indistinguishable from one, so a
	// library jittering by a tenth would fail here rather than pass quietly.
	const slack = 0.02
	if observedLow > scheduled*(jitter.MinMultiplier+slack) {
		t.Errorf("the shortest of %d jittered waits was %.0fms; a factor of %v should "+
			"reach %.0fms", rolls, observedLow, jitter.Factor, low)
	}
	if observedHigh < scheduled*(jitter.MaxMultiplier-slack) {
		t.Errorf("the longest of %d jittered waits was %.0fms; a factor of %v should "+
			"reach %.0fms", rolls, observedHigh, jitter.Factor, high)
	}
}

func TestContractABrokerWaitIsNeverJittered(t *testing.T) {
	fixtures := loadContract(t)

	if fixtures.Jitter.AppliesToBrokerWaits {
		t.Fatal("the contract now says broker waits are jittered; a jittered delay names " +
			"no rung queue, so this needs more than a test change")
	}

	checked := 0
	for _, schedule := range fixtures.RetrySchedules {
		if len(schedule.BrokerRungDelaysMillis) == 0 || schedule.JitterFactor == 0 {
			continue
		}
		policy := contractPolicy(t, schedule.How).
			GiveUpAfter(time.Duration(schedule.MaxMessageAgeMillis) * time.Millisecond)

		for attempt := 1; attempt < schedule.MaxAttempts; attempt++ {
			scheduled := schedule.ScheduleMillis[attempt-1]
			if scheduled < schedule.BrokerWaitThresholdMillis {
				continue
			}
			for i := 0; i < 500; i++ {
				wait, again := policy.NextWait(attempt, 0)
				if !again {
					t.Fatalf("%s did not retry attempt %d", schedule.How, attempt)
				}
				if !wait.InBroker {
					t.Fatalf("%s attempt %d waits %s here rather than on a rung, and the "+
						"threshold is %dms", schedule.How, attempt, wait.Delay,
						schedule.BrokerWaitThresholdMillis)
				}
				if wait.Delay.Milliseconds() != scheduled {
					t.Fatalf("%s attempt %d waits %s on a rung; a rung's time-to-live is "+
						"fixed at %dms and a moved delay names a queue that does not exist",
						schedule.How, attempt, wait.Delay, scheduled)
				}
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("no policy in the contract has a jittered rung, so this proves nothing")
	}
}

// TestContractTheJitterFloorIsAMillisecondInJavaAndNoneHere records the second
// place the two libraries part company, and it is the same kind of place as the
// rung naming: below a millisecond, where nothing real happens.
//
// Java holds a downward roll at one millisecond, because a zero-length wait is a
// hot loop rather than a retry, and writes that floor into the fixture. This
// library floors at zero, so a one-millisecond delay jittered by a full factor
// can come back as eight hundred microseconds. Every delay the contract's own
// policies produce is a second or more, where a downward roll of a fifth lands
// at eight hundred milliseconds and the floor is nowhere near — which is why the
// jitter test above passes the contract's floor check honestly.
func TestContractTheJitterFloorIsAMillisecondInJavaAndNoneHere(t *testing.T) {
	fixtures := loadContract(t)

	if fixtures.Jitter.FlooredAtMillis != 1 {
		t.Fatalf("the contract now floors jitter at %dms; this test is written for the "+
			"one-millisecond floor it recorded", fixtures.Jitter.FlooredAtMillis)
	}

	// The widest jitter there is, on the smallest delay that can be jittered.
	tiny := FixedRetry(2, time.Millisecond).WithJitter(1.0)
	shortest := time.Hour
	for i := 0; i < 5000; i++ {
		wait, again := tiny.NextWait(1, 0)
		if !again {
			t.Fatal("a two-attempt policy did not retry its first failure")
		}
		if wait.Delay < 0 {
			t.Fatalf("a jittered wait of %s is negative", wait.Delay)
		}
		if wait.Delay < shortest {
			shortest = wait.Delay
		}
	}
	if shortest >= time.Millisecond {
		t.Errorf("the shortest of 5000 fully jittered one-millisecond waits was %s; "+
			"this library floors at zero, so it should fall below the millisecond the "+
			"contract records, and the divergence noted here has been resolved without "+
			"this test being updated", shortest)
	}
}

// --------------------------------------------------------- the broker threshold

func TestContractTheThresholdTableIsTheSameEverywhere(t *testing.T) {
	fixtures := loadContract(t)
	cases := fixtures.BrokerWaitThreshold.Cases

	if len(cases) != 30 {
		t.Fatalf("the contract records %d threshold rows, expected the 30 it was written "+
			"with", len(cases))
	}

	for _, row := range cases {
		threshold := time.Duration(row.ThresholdMillis) * time.Millisecond
		delay := time.Duration(row.DelayMillis) * time.Millisecond

		// The rule the fixture states, applied here rather than read back: at or
		// above the threshold goes to the broker, anything below it and anything
		// zero or negative stays here, and a threshold of zero sends nothing.
		expected := row.ThresholdMillis > 0 && row.DelayMillis > 0 &&
			row.DelayMillis >= row.ThresholdMillis
		if expected != row.WaitsInBroker {
			t.Errorf("delay %dms against threshold %dms: the contract says %v, the rule "+
				"it states gives %v", row.DelayMillis, row.ThresholdMillis,
				row.WaitsInBroker, expected)
		}

		policy := ExponentialRetry(5, time.Second, time.Hour).WaitInBrokerFrom(threshold)
		if got := policy.WaitsInBroker(delay); got != row.WaitsInBroker {
			t.Errorf("delay %s against threshold %s waits in the broker=%v here and =%v "+
				"in the contract", delay, threshold, got, row.WaitsInBroker)
		}
	}

	// A threshold of zero declares no rung queue at all, which is the half of
	// the rule that is about the topology rather than about one delay.
	off := ExponentialRetry(6, 10*time.Second, 5*time.Minute).WaitInBrokerFrom(0)
	if rungs := off.BrokerRungs(); len(rungs) != 0 {
		t.Errorf("a policy with the threshold turned off still wants rungs for %v", rungs)
	}
	if ladder := LadderFor("orders.new", off); !ladder.Empty() {
		t.Errorf("a policy with the threshold turned off still builds the ladder %s", ladder)
	}
}

// --------------------------------------------------------------------- naming

// contractShortDelay renders a delay the obvious way, as a second opinion on
// shortDuration.
//
// Written out longhand and deliberately not sharing a line of code with the
// implementation. A name is half of a cross-language contract — two services
// declaring the same rung under different names give the second one a
// PRECONDITION_FAILED — so it is worth spelling twice.
func contractShortDelay(delay time.Duration) string {
	seconds := int64(delay / time.Second)
	if seconds <= 0 {
		return "0s"
	}
	if seconds%3600 == 0 {
		return strconv.FormatInt(seconds/3600, 10) + "h"
	}
	if seconds%60 == 0 {
		return strconv.FormatInt(seconds/60, 10) + "m"
	}
	return strconv.FormatInt(seconds, 10) + "s"
}

func TestContractQueueNames(t *testing.T) {
	fixtures := loadContract(t)
	naming := fixtures.Naming

	if got := DeadLetterQueue(naming.Queue); got != naming.DeadLetterQueue {
		t.Errorf("dead letters go to %q here and to %q in the contract",
			got, naming.DeadLetterQueue)
	}
	if got := ParkedQueue(naming.Queue); got != naming.ParkedQueue {
		t.Errorf("parked messages go to %q here and to %q in the contract",
			got, naming.ParkedQueue)
	}
	if naming.DeadLetterQueue != naming.Queue+".dlq" {
		t.Errorf("the contract's dead-letter queue %q is not %q plus .dlq",
			naming.DeadLetterQueue, naming.Queue)
	}
	if naming.ParkedQueue != naming.Queue+".parked" {
		t.Errorf("the contract's parked queue %q is not %q plus .parked",
			naming.ParkedQueue, naming.Queue)
	}

	for _, want := range naming.RetryQueues {
		delay := time.Duration(want.DelayMillis) * time.Millisecond
		got := RetryQueue(naming.Queue, delay)

		if delay < time.Second {
			// Recorded, not resolved. See the test below.
			continue
		}
		if got != want.Queue {
			t.Errorf("a %s rung is named %q here and %q in the contract",
				delay, got, want.Queue)
		}
		if second := naming.Queue + RetryInfix + contractShortDelay(delay); second != got {
			t.Errorf("a %s rung renders as %q one way and %q the other",
				delay, got, second)
		}
	}

	// A wider sweep than the contract carries, against the second renderer, so
	// that a change to the real one is caught between the delays the file names
	// as well as at them.
	for _, delay := range []time.Duration{
		time.Second, 2 * time.Second, 45 * time.Second, 59 * time.Second, time.Minute,
		90 * time.Second, 2 * time.Minute, 59 * time.Minute, time.Hour, 90 * time.Minute,
		24 * time.Hour, 1500 * time.Millisecond,
	} {
		want := naming.Queue + RetryInfix + contractShortDelay(delay)
		if got := RetryQueue(naming.Queue, delay); got != want {
			t.Errorf("a %s rung renders as %q one way and %q the other", delay, got, want)
		}
	}
}

// TestContractSubSecondRungNamesDisagreeAcrossLanguages is the disagreement the
// fixture records rather than settles.
//
// Java renders a sub-second rung in milliseconds — orders.new.retry.500ms —
// where this library and the Python and Ruby ones render orders.new.retry.0s.
// Three against one, and not settled, because the two names are not the same
// queue: whichever service declares it second is answered PRECONDITION_FAILED
// and cannot consume.
//
// It is unreachable through the default thirty-second threshold. Only a policy
// that has deliberately lowered the threshold below a second can produce one,
// and such a policy is asking the broker to hold a message for less time than
// it takes to publish it. This test asserts what this library actually does, and
// fails if the file ever stops recording the difference.
func TestContractSubSecondRungNamesDisagreeAcrossLanguages(t *testing.T) {
	fixtures := loadContract(t)
	naming := fixtures.Naming

	if naming.SubSecondDisagreement == "" {
		t.Fatal("the contract no longer records the sub-second naming disagreement; " +
			"either it has been settled, in which case this library's rendering needs " +
			"to change with it, or the note has been dropped by accident")
	}

	found := 0
	for _, want := range naming.RetryQueues {
		delay := time.Duration(want.DelayMillis) * time.Millisecond
		if delay >= time.Second {
			continue
		}
		found++

		got := RetryQueue(naming.Queue, delay)
		if got != naming.Queue+RetryInfix+"0s" {
			t.Errorf("a %s rung is named %q here; this library renders every sub-second "+
				"delay as 0s, as Python and Ruby do", delay, got)
		}
		if got == want.Queue {
			t.Errorf("a %s rung is named %q in both, so the disagreement the contract "+
				"records has been settled and the note in the file is now wrong", delay, got)
		}
		if !strings.Contains(naming.SubSecondDisagreement, want.Queue) ||
			!strings.Contains(naming.SubSecondDisagreement, got) {
			t.Errorf("the recorded disagreement names neither %q nor %q: %q",
				want.Queue, got, naming.SubSecondDisagreement)
		}
	}
	if found == 0 {
		t.Fatal("the contract carries no sub-second rung, so this proves nothing")
	}
}

// ------------------------------------------------------------ rung arguments

func TestContractARungIsDeclaredWithExactlyThreeArguments(t *testing.T) {
	fixtures := loadContract(t)
	source := fixtures.Naming.Queue

	if len(fixtures.RungArguments) == 0 {
		t.Fatal("the contract records no rung arguments")
	}

	for _, want := range fixtures.RungArguments {
		delay := time.Duration(want.DelayMillis) * time.Millisecond
		got := RungArgs(source, delay)

		// The count as well as the values. A fourth argument is not a richer
		// declaration; it is a PRECONDITION_FAILED for the second service to
		// declare the queue, and no way for it to consume at all.
		if len(got) != 3 {
			t.Errorf("a %s rung is declared with %d arguments %s, and the contract says "+
				"exactly three", delay, len(got), showArgs(contractArgs(got)))
		}
		if len(want.Arguments) != 3 {
			t.Errorf("the contract records %d arguments for a %s rung, and says exactly "+
				"three", len(want.Arguments), delay)
		}

		if !sameArgs(contractArgs(got), contractArgs(want.Arguments)) {
			t.Errorf("a %s rung is declared %s here and %s in the contract", delay,
				showArgs(contractArgs(got)), showArgs(contractArgs(want.Arguments)))
		}

		// And each one named, so a failure says which argument moved rather than
		// only that the tables differ.
		if ttl, ok := got[ArgMessageTTL].(int64); !ok || ttl != want.DelayMillis {
			t.Errorf("a %s rung holds a message for %v, and the contract says %dms",
				delay, got[ArgMessageTTL], want.DelayMillis)
		}
		if got[ArgDeadLetterExchange] != fixtures.RetryExchange {
			t.Errorf("a %s rung returns through %v, and the contract says %q",
				delay, got[ArgDeadLetterExchange], fixtures.RetryExchange)
		}
		if got[ArgDeadLetterRoutingKey] != source {
			t.Errorf("a %s rung returns under %v, and the contract says %q",
				delay, got[ArgDeadLetterRoutingKey], source)
		}
		if _, set := got[ArgQueueType]; set {
			t.Errorf("a %s rung carries %s among its arguments; the contract's three do "+
				"not include it, and a rung's type is said with OfType", delay, ArgQueueType)
		}

		// The queue the arguments belong to, except where the languages disagree
		// about its name — which is the sub-second case, and only that.
		if delay >= time.Second {
			if name := RetryQueue(source, delay); name != want.Queue {
				t.Errorf("the arguments for %q here belong to %q in the contract",
					name, want.Queue)
			}
		}
	}
}

// ------------------------------------------------------------------- topology

// contractRecorder is a transport that writes down what it was asked to declare
// instead of declaring it.
//
// The consumer's half of the topology is a sequence of declaration calls rather
// than a description, so the way to compare it against the contract is to make
// the calls and keep them. Java's fixture generator records its half exactly
// this way.
type contractRecorder struct {
	exchanges []struct {
		Name string
		Spec ExchangeSpec
	}
	queues []struct {
		Name string
		Spec QueueSpec
	}
	bindings []BindingSpec
}

func (r *contractRecorder) DeclareQueue(_ context.Context, name string, spec QueueSpec) error {
	r.queues = append(r.queues, struct {
		Name string
		Spec QueueSpec
	}{name, spec})
	return nil
}

func (r *contractRecorder) DeclareExchange(_ context.Context, name string, spec ExchangeSpec) error {
	r.exchanges = append(r.exchanges, struct {
		Name string
		Spec ExchangeSpec
	}{name, spec})
	return nil
}

func (r *contractRecorder) Bind(_ context.Context, queue, exchange, routingKey string) error {
	r.bindings = append(r.bindings, BindingSpec{
		Queue: queue, Exchange: exchange, RoutingKey: routingKey})
	return nil
}

func (r *contractRecorder) Publish(
	context.Context, string, string, Outbound,
) (PublishResult, error) {
	return PublishResult{}, nil
}

func (r *contractRecorder) Consume(
	context.Context, string, ConsumeSpec, func(Delivery),
) (Subscription, error) {
	return nil, fmt.Errorf("acemq: the contract recorder does not consume")
}

func (r *contractRecorder) Close() error { return nil }

// contractHalves is the whole declared topology, and which half declared each
// part of it.
//
// The operator declares the source queue and everything around it through
// Topology; the consumer declares the rungs and the dead-letter queues it will
// itself publish into, through RetryLadder.Declare. Neither half is the whole
// topology and the two overlap, which is exactly what the contract's declaredBy
// field is for: "topology", "consumer" or "both".
type contractHalves struct {
	exchangeSpec map[string]ExchangeSpec
	queueSpec    map[string]QueueSpec
	bindings     map[string]bool
	declaredBy   map[string]string
}

func contractBindingKey(queue, exchange, routingKey string) string {
	return queue + " " + exchange + " " + routingKey
}

func (h *contractHalves) note(kind, name, half string) {
	key := kind + " " + name
	switch existing := h.declaredBy[key]; existing {
	case "", half:
		h.declaredBy[key] = half
	default:
		h.declaredBy[key] = "both"
	}
}

func contractDeclaredTopology(t *testing.T, source string, policy RetryPolicy) *contractHalves {
	t.Helper()

	halves := &contractHalves{
		exchangeSpec: map[string]ExchangeSpec{},
		queueSpec:    map[string]QueueSpec{},
		bindings:     map[string]bool{},
		declaredBy:   map[string]string{},
	}

	// The operator's half, exactly as the contract describes it: a topic
	// exchange, the source queue with its dead letters, and one binding.
	operator := NewTopology().
		Exchange("orders", "topic").
		Queue(source).
		DeadLetters(source).
		Binding(source, "orders", "order.*")
	if err := operator.Validate(); err != nil {
		t.Fatalf("the operator's half of the topology is invalid: %v", err)
	}
	for _, exchange := range operator.exchanges {
		halves.exchangeSpec[exchange.Name] = exchange.Spec
		halves.note("exchange", exchange.Name, "topology")
	}
	for _, queue := range operator.queues {
		halves.queueSpec[queue.Name] = queue.Spec
		halves.note("queue", queue.Name, "topology")
	}
	for _, binding := range operator.bindings {
		key := contractBindingKey(binding.Queue, binding.Exchange, binding.RoutingKey)
		halves.bindings[key] = true
		halves.note("binding", key, "topology")
	}

	// The consumer's half, recorded from the calls it makes rather than
	// described a second time.
	recorder := &contractRecorder{}
	if err := LadderFor(source, policy).Declare(context.Background(), &Conn{transport: recorder}); err != nil {
		t.Fatalf("the consumer's half of the topology would not declare: %v", err)
	}
	for _, exchange := range recorder.exchanges {
		halves.exchangeSpec[exchange.Name] = exchange.Spec
		halves.note("exchange", exchange.Name, "consumer")
	}
	for _, queue := range recorder.queues {
		halves.queueSpec[queue.Name] = queue.Spec
		halves.note("queue", queue.Name, "consumer")
	}
	for _, binding := range recorder.bindings {
		key := contractBindingKey(binding.Queue, binding.Exchange, binding.RoutingKey)
		halves.bindings[key] = true
		halves.note("binding", key, "consumer")
	}
	return halves
}

func TestContractTheDeclaredTopologyIsTheSameInEveryLanguage(t *testing.T) {
	fixtures := loadContract(t)
	plan := fixtures.Topology

	policy := ExponentialRetry(6, 10*time.Second, 5*time.Minute)
	if schedule := contractMillis(policy.Schedule()); !sameMillis(schedule, plan.PolicyScheduleMillis) {
		t.Fatalf("the topology's policy waits %v here and %v in the contract",
			schedule, plan.PolicyScheduleMillis)
	}
	if rungs := contractMillis(policy.BrokerRungs()); !sameMillis(rungs, plan.PolicyRungDelaysMillis) {
		t.Fatalf("the topology's policy needs rungs for %v here and %v in the contract",
			rungs, plan.PolicyRungDelaysMillis)
	}

	halves := contractDeclaredTopology(t, plan.SourceQueue, policy)

	seenExchanges := map[string]bool{}
	for _, want := range plan.Exchanges {
		seenExchanges[want.Name] = true
		spec, declared := halves.exchangeSpec[want.Name]
		if !declared {
			t.Errorf("nothing declares the exchange %q, which the contract says %s does",
				want.Name, want.DeclaredBy)
			continue
		}
		if spec.Kind != want.Type {
			t.Errorf("exchange %q is %s here and %s in the contract",
				want.Name, spec.Kind, want.Type)
		}
		if spec.Durable != want.Durable {
			t.Errorf("exchange %q is durable=%v here and =%v in the contract",
				want.Name, spec.Durable, want.Durable)
		}
		contractCheckHalf(t, "exchange", want.Name, halves, want.DeclaredBy)
	}
	for name := range halves.exchangeSpec {
		if !seenExchanges[name] {
			t.Errorf("this library declares the exchange %q, which the contract does not "+
				"list", name)
		}
	}

	seenQueues := map[string]bool{}
	for _, want := range plan.Queues {
		seenQueues[want.Name] = true
		spec, declared := halves.queueSpec[want.Name]
		if !declared {
			t.Errorf("nothing declares the queue %q, which the contract says %s does",
				want.Name, want.DeclaredBy)
			continue
		}
		if got := string(queueTypeOf(spec)); got != want.Type {
			t.Errorf("queue %q is %s here and %s in the contract", want.Name, got, want.Type)
		}
		if spec.Durable != want.Durable {
			t.Errorf("queue %q is durable=%v here and =%v in the contract",
				want.Name, spec.Durable, want.Durable)
		}
		if !sameArgs(contractArgs(spec.Args), contractArgs(want.Arguments)) {
			t.Errorf("queue %q is declared %s here and %s in the contract", want.Name,
				showArgs(contractArgs(spec.Args)), showArgs(contractArgs(want.Arguments)))
		}
		contractCheckHalf(t, "queue", want.Name, halves, want.DeclaredBy)
	}
	for name := range halves.queueSpec {
		if !seenQueues[name] {
			t.Errorf("this library declares the queue %q, which the contract does not list",
				name)
		}
	}

	seenBindings := map[string]bool{}
	for _, want := range plan.Bindings {
		key := contractBindingKey(want.Queue, want.Exchange, want.RoutingKey)
		seenBindings[key] = true
		if !halves.bindings[key] {
			t.Errorf("nothing binds %q to %q on %q, which the contract says %s does",
				want.Queue, want.Exchange, want.RoutingKey, want.DeclaredBy)
			continue
		}
		contractCheckHalf(t, "binding", key, halves, want.DeclaredBy)
	}
	for key := range halves.bindings {
		if !seenBindings[key] {
			t.Errorf("this library declares the binding %q, which the contract does not "+
				"list", key)
		}
	}
}

// contractCheckHalf compares which half declared something against the contract,
// with nothing exempted.
//
// There used to be a list of five exemptions here — acemq.dlx, {queue}.dlq,
// {queue}.parked and their two bindings, which the contract says both halves
// declare and which this library declared from the topology alone. ADR-032
// closed it: the consumer declares the dead-letter half at start-up too, so
// there is no difference left to record and nothing here to allow for.
func contractCheckHalf(t *testing.T, kind, name string, halves *contractHalves, want string) {
	t.Helper()
	key := kind + " " + name
	if got := halves.declaredBy[key]; got != want {
		t.Errorf("%s is declared by %s here and by %s in the contract", key, got, want)
	}
}

// TestContractTheConsumerHalfDeclaresTheDeadLetterQueues pins ADR-032 from the
// other direction: it asserts exactly what this library's consumer half
// declares, so that the split which used to be recorded here cannot reopen
// without a test saying so.
//
// The half used to stop at the rungs. A Go service that never applied a Topology
// would then republish a message it had given up on into a queue nobody had
// declared, and an unroutable publish is discarded by the broker without a
// trace: the one message anybody wanted kept, lost in silence. Java's consumer
// half declared the dead-letter queues from the start; this one now does too.
func TestContractTheConsumerHalfDeclaresTheDeadLetterQueues(t *testing.T) {
	fixtures := loadContract(t)
	source := fixtures.Topology.SourceQueue

	recorder := &contractRecorder{}
	policy := ExponentialRetry(6, 10*time.Second, 5*time.Minute)
	if err := LadderFor(source, policy).Declare(context.Background(), &Conn{transport: recorder}); err != nil {
		t.Fatalf("declaring the ladder failed: %v", err)
	}

	gotExchanges := map[string]bool{}
	for _, e := range recorder.exchanges {
		gotExchanges[e.Name] = true
		if e.Spec.Kind != "direct" || !e.Spec.Durable {
			t.Errorf("the consumer half declares %q as %s durable=%v; both exchanges are "+
				"direct and durable in every language", e.Name, e.Spec.Kind, e.Spec.Durable)
		}
	}
	wantExchanges := map[string]bool{RetryExchange: true, DeadLetterExchange: true}
	if !maps.Equal(gotExchanges, wantExchanges) {
		t.Errorf("the consumer half declares the exchanges %v, and the contract says %v",
			sortedKeys(gotExchanges), sortedKeys(wantExchanges))
	}

	gotQueues := map[string]bool{}
	for _, q := range recorder.queues {
		gotQueues[q.Name] = true
	}
	wantQueues := map[string]bool{
		DeadLetterQueue(source): true,
		ParkedQueue(source):     true,
	}
	for _, rung := range LadderFor(source, policy).Queues() {
		wantQueues[rung] = true
	}
	if !maps.Equal(gotQueues, wantQueues) {
		t.Errorf("the consumer half declares the queues %v, and the contract says %v",
			sortedKeys(gotQueues), sortedKeys(wantQueues))
	}

	// The two the split used to leave out, spelled again on their own, so that a
	// regression here reads as what it is rather than as a set that differs.
	for _, name := range []string{DeadLetterQueue(source), ParkedQueue(source)} {
		if !gotQueues[name] {
			t.Errorf("the consumer half no longer declares %q; a consumer that gives up "+
				"would republish into a queue nobody declared, and the broker discards "+
				"an unroutable message without a trace", name)
			continue
		}
		for _, q := range recorder.queues {
			if q.Name != name {
				continue
			}
			// Classic and durable: the same declaration Topology.DeadLetters
			// makes, so whichever runs second agrees with the first instead of
			// being answered PRECONDITION_FAILED.
			if got := queueTypeOf(q.Spec); got != QueueClassic {
				t.Errorf("the consumer half declares %q as %s, and Topology declares it "+
					"classic; the second of the two to run would be refused", name, got)
			}
			if !q.Spec.Durable {
				t.Errorf("the consumer half declares %q transient", name)
			}
			if len(contractArgs(q.Spec.Args)) != 0 {
				t.Errorf("the consumer half declares %q with %s, and Topology declares it "+
					"with no arguments at all", name, showArgs(contractArgs(q.Spec.Args)))
			}
		}
	}

	gotBindings := map[string]bool{}
	for _, b := range recorder.bindings {
		gotBindings[contractBindingKey(b.Queue, b.Exchange, b.RoutingKey)] = true
	}
	wantBindings := map[string]bool{
		contractBindingKey(source, RetryExchange, source): true,
		contractBindingKey(DeadLetterQueue(source), DeadLetterExchange,
			DeadLetterQueue(source)): true,
		contractBindingKey(ParkedQueue(source), DeadLetterExchange,
			ParkedQueue(source)): true,
	}
	if !maps.Equal(gotBindings, wantBindings) {
		t.Errorf("the consumer half declares the bindings %v, and the contract says %v",
			sortedKeys(gotBindings), sortedKeys(wantBindings))
	}
}

// sortedKeys is the set as a list an error message can be read in one order.
func sortedKeys(set map[string]bool) []string {
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// -------------------------------------------------------------- queue types

func TestContractQueueTypeDefaults(t *testing.T) {
	fixtures := loadContract(t)
	defaults := fixtures.QueueTypeDefaults

	// A durable queue nobody said anything else about is quorum. It is the
	// default in all five libraries, and a library that made it classic would
	// give the second service to declare a shared queue a PRECONDITION_FAILED.
	source := NewTopology().Queue("orders.new")
	contractAssertType(t, source, "orders.new", defaults.SourceQueue)

	withDeadLetter := NewTopology().Queue("orders.new").DeadLetters("orders.new")
	contractAssertType(t, withDeadLetter, "orders.new", defaults.SourceQueueWithDeadLetter)
	contractAssertType(t, withDeadLetter, "orders.new.dlq", defaults.DeadLetterQueue)
	contractAssertType(t, withDeadLetter, "orders.new.parked", defaults.ParkedQueue)

	classic := NewTopology().Queue("orders.new", OfType(QueueClassic))
	contractAssertType(t, classic, "orders.new", defaults.ExplicitClassicQueue)

	classicWithDeadLetter := NewTopology().
		Queue("orders.new", OfType(QueueClassic)).DeadLetters("orders.new")
	contractAssertType(t, classicWithDeadLetter, "orders.new",
		defaults.ExplicitClassicQueueWithDeadLetter)

	rungs := NewTopology().
		Queue("orders.new").
		Retries("orders.new", ExponentialRetry(6, 10*time.Second, 5*time.Minute))
	contractAssertType(t, rungs, "orders.new.retry.40s", defaults.RetryRung)
	contractAssertType(t, rungs, "orders.new.retry.80s", defaults.RetryRung)
	contractAssertType(t, rungs, "orders.new.retry.160s", defaults.RetryRung)

	if !defaults.Durability.EveryDeclaredQueueIsDurable {
		t.Fatal("the contract no longer says every declared queue is durable")
	}
	whole := NewTopology().
		Exchange("orders", "topic").
		Queue("orders.new").
		DeadLetters("orders.new").
		Binding("orders.new", "orders", "order.*").
		Retries("orders.new", ExponentialRetry(6, 10*time.Second, 5*time.Minute))
	if err := whole.Validate(); err != nil {
		t.Fatalf("the whole topology is invalid: %v", err)
	}
	for _, queue := range whole.queues {
		if !queue.Spec.Durable {
			t.Errorf("queue %q is declared transient, and the contract says every "+
				"declared queue is durable", queue.Name)
		}
	}

	// The rule Java cannot be asked about, because its API offers no way to
	// declare an exclusive, auto-delete or transient queue at all. The contract
	// says the ports that do offer those flags must keep such a queue classic,
	// which is not a preference: RabbitMQ allows a quorum queue to be none of
	// the three.
	if defaults.Durability.ExclusiveAutoDeleteOrTransient == "" {
		t.Fatal("the contract no longer says anything about exclusive, auto-delete or " +
			"transient queues")
	}
	for name, opt := range map[string]QueueOption{
		"transient":   Transient(),
		"exclusive":   Exclusive(),
		"auto-delete": AutoDelete(),
	} {
		spec := QueueSpec{Durable: true}
		opt(&spec)
		quorumByDefault(&spec)
		if got := queueTypeOf(spec); got != QueueClassic {
			t.Errorf("a %s queue is declared %s; RabbitMQ allows a quorum queue to be "+
				"none of the three, so it has to stay %s", name, got, QueueClassic)
		}
	}
}

func contractAssertType(t *testing.T, topology *Topology, queue, want string) {
	t.Helper()
	if err := topology.Validate(); err != nil {
		t.Fatalf("the topology carrying %q is invalid: %v", queue, err)
	}
	for _, declared := range topology.queues {
		if declared.Name != queue {
			continue
		}
		if got := string(queueTypeOf(declared.Spec)); got != want {
			t.Errorf("queue %q is declared %s here and %s in the contract", queue, got, want)
		}
		return
	}
	t.Errorf("the topology does not declare %q at all", queue)
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
