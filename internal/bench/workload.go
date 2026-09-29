// Package bench implements the load generator and the offline
// key-redistribution experiment.
//
// # What lives in this package
//
//   - workload.go (this file): WHICH key each simulated request touches. A
//     KeyGen turns random numbers into key indexes, either uniformly or with a
//     Zipfian (power-law) skew, and KeyName turns an index into the key string.
//   - runner.go: the load generator itself (Run). It opens one TCP connection
//     per simulated client, preloads the keyspace, warms up, runs the measured
//     phase, and in parallel samples cluster health, probes one watched key and
//     fires fault-injection commands at fixed times.
//   - result.go: the JSON-serialisable Result, exact percentile computation
//     (summarize) and the human-readable report.
//   - rebalance.go: an offline experiment (no network at all) that counts how
//     many keys change owner when nodes join or leave, under consistent hashing
//     versus naive `hash % N` sharding.
//
// # Why the key distribution matters
//
// Real workloads are rarely uniform: a few keys (a celebrity's profile, the
// front page) receive most of the traffic. That skew is what makes caches
// useful and what creates "hot spots" on individual nodes. A benchmark that
// only ever used uniform keys would under-sell a cache (every key is equally
// cold) and would never exercise hot-key contention. So the generator offers
// both: uniform as the neutral baseline, Zipf to model realistic skew. The
// README's Test B shows the effect: with Zipf θ=0.99 the LRU hit rate is
// 90.9%, with uniform keys it is 40.0% for the same cache size.
package bench

import (
	"fmt"
	"math"
	"math/rand/v2"
)

// KeyGen picks key indexes in [0, n).
//
// It is an interface so the worker loop in runPhase does not care which
// distribution is in use: it just calls gen.Next(rng) for every request.
//
// The random source is passed IN rather than stored in the generator. That is
// deliberate: every benchmark worker goroutine owns its own *rand.Rand, so the
// generator itself can be shared read-only by all workers without any lock.
// A generator that owned a single shared RNG would need a mutex (or would be a
// data race), and that mutex would become a point of contention that distorts
// the very latencies we are trying to measure.
type KeyGen interface {
	// Next returns one key index in [0, n) drawn from the distribution,
	// using r as the only source of randomness.
	Next(r *rand.Rand) int
}

// Uniform picks every key with equal probability.
//
// N is the keyspace size. With uniform keys every key is equally likely, so a
// cache holding C of N keys can expect a hit rate of roughly C/N once warm;
// this is the "worst case" for caching and the neutral baseline for sharding.
type Uniform struct{ N int }

// Next implements KeyGen.
//
// IntN returns a uniformly distributed integer in [0, N) without modulo bias
// (it does not compute rand % N, which would slightly favour small values).
func (u Uniform) Next(r *rand.Rand) int { return r.IntN(u.N) }

// Zipf draws key ranks from a Zipfian distribution with exponent theta,
// P(rank i) ∝ 1/i^theta, using the rejection-free method of Gray et al.,
// "Quickly Generating Billion-Record Synthetic Databases" (SIGMOD '94) — the
// same generator YCSB uses. Unlike math/rand's Zipf it supports theta < 1;
// YCSB's default theta of 0.99 makes a small set of keys very hot.
//
// Rank 0 is the hottest key. Keys are hashed onto the ring, so hot keys land
// on arbitrary nodes rather than all on one.
//
// # The math, step by step
//
// Number the ranks 1..n (the code returns them shifted to 0..n-1). Zipf's law
// says the probability of rank i is
//
//	P(i) = (1/i^θ) / ζ(n, θ),   where ζ(n, θ) = Σ_{j=1..n} 1/j^θ
//
// ζ(n, θ) (the generalised harmonic number, "zeta") is just the normalising
// constant that makes the probabilities sum to 1.
//
// The textbook way to sample a discrete distribution is inverse-transform
// sampling: draw u uniformly in [0,1), then walk the cumulative distribution
// function (CDF) until it exceeds u. For 100,000 keys that walk costs up to
// 100,000 additions PER REQUEST, which would make the load generator slower
// than the system it measures. Rejection sampling (math/rand.Zipf's approach)
// avoids that, but only works for θ > 1, while the interesting, YCSB-standard
// skew is θ = 0.99.
//
// Gray et al.'s trick: treat the tail of the distribution as CONTINUOUS. For a
// continuous power law, the CDF integral ∫ x^-θ dx has a closed form, so it can
// be inverted analytically in O(1). The discrete head (ranks 1 and 2, which
// carry the largest individual probabilities and where the continuous
// approximation is worst) is handled exactly with two comparisons, and the
// rest of the ranks use the closed-form inverse:
//
//	rank = n * (η·u − η + 1)^α,   with α = 1/(1−θ)
//	η    = (1 − (2/n)^(1−θ)) / (1 − ζ(2,θ)/ζ(n,θ))
//
// η is chosen so that the continuous part of the curve joins the exact head
// smoothly: at the u where the head ends (u = ζ(2)/ζ(n)) the formula yields exactly
// rank 2, and as u → 1 it approaches rank n.
//
// Why θ must be strictly below 1: α = 1/(1−θ) is infinite at θ = 1, and for
// θ > 1 the exponent 1−θ turns negative and the closed form no longer
// describes the distribution. Why θ > 0: θ = 0 would be uniform (use Uniform).
//
// Cost: NewZipf computes ζ(n, θ) once, O(n) calls to math.Pow (for 100k keys
// that is a few milliseconds, paid before any measurement). Next is O(1): one
// random float, a multiply, at most two comparisons and one Pow.
//
// # How skewed is θ = 0.99?
//
// Very. TestZipfIsSkewed checks that the 100 hottest of 100,000 keys (0.1% of
// the keyspace) receive at least 30% of all requests, versus 0.1% under a
// uniform distribution.
//
// # Note on scrambling
//
// YCSB also offers a "scrambled" Zipfian that hashes the rank so that hot
// ranks are not neighbours in key order. That is unnecessary here: KeyName
// produces the key string and the coordinator hashes that string onto the
// consistent-hash ring, so rank 0, 1, 2... already land on unrelated nodes.
//
// # Fields
//
//   - n: the keyspace size; Next returns values in [0, n).
//   - theta: the skew exponent θ in (0,1).
//   - alpha: 1/(1−θ), the exponent of the closed-form inverse CDF.
//   - zetan: ζ(n, θ), the normalising constant.
//   - eta: η, the constant that stitches the continuous tail onto the exact
//     head (formula above).
//   - halfPowTheta: holds 1 + 0.5^θ, which equals ζ(2, θ) = 1/1^θ + 1/2^θ.
//     Despite the name it is NOT just 0.5^θ: it is the CUMULATIVE weight of
//     ranks 1 and 2, used as the second threshold in Next.
type Zipf struct {
	n                        int
	theta, alpha, zetan, eta float64
	halfPowTheta             float64
}

// NewZipf precomputes the generator's constants (O(n) once).
//
// All expensive work happens here, before the benchmark starts timing, so
// that the per-request cost in Next is constant. The returned *Zipf is
// immutable afterwards and therefore safe to share between goroutines.
func NewZipf(n int, theta float64) (*Zipf, error) {
	// An empty keyspace has no ranks to draw from.
	if n < 1 {
		return nil, fmt.Errorf("zipf: keyspace must be >= 1")
	}
	// The closed-form inverse only exists for 0 < θ < 1 (α = 1/(1−θ) must be
	// finite and positive). Reject anything else instead of silently producing
	// NaN or out-of-range ranks.
	if theta <= 0 || theta >= 1 {
		return nil, fmt.Errorf("zipf: theta must be in (0,1), got %v", theta)
	}
	// zeta(n) = Σ_{i=1..n} 1/i^θ, the generalised harmonic number. It is a
	// closure so the same code computes both ζ(n, θ) and ζ(2, θ).
	zeta := func(n int) float64 {
		s := 0.0
		for i := 1; i <= n; i++ {
			s += 1 / math.Pow(float64(i), theta)
		}
		return s
	}
	// α = 1/(1−θ): the exponent of the inverse of the continuous power-law
	// CDF. For θ = 0.99, α = 100, which is what makes the curve so steep.
	z := &Zipf{n: n, theta: theta, alpha: 1 / (1 - theta), zetan: zeta(n)}
	// ζ(2, θ) = 1 + 0.5^θ: the cumulative (unnormalised) weight of the two
	// head ranks that Next handles exactly.
	zeta2 := zeta(2)
	// η from Gray et al.: numerator (1 − (2/n)^(1−θ)) is the continuous CDF's
	// share of the range above rank 2; denominator (1 − ζ(2)/ζ(n)) is the
	// probability mass NOT covered by the two exact head ranks. Their ratio
	// rescales u so the tail formula starts where the head leaves off.
	z.eta = (1 - math.Pow(2/float64(n), 1-theta)) / (1 - zeta2/z.zetan)
	// Cache 1 + 0.5^θ (= ζ(2, θ)) so Next does not call Pow for the head test.
	z.halfPowTheta = 1 + math.Pow(0.5, theta)
	return z, nil
}

// Next implements KeyGen.
//
// It maps one uniform random number u in [0,1) to a rank, in three cases:
//
//  1. u·ζ(n) < 1          → rank 0 (the hottest key). Probability 1/ζ(n),
//     exactly Zipf's P(rank 1).
//  2. u·ζ(n) < 1 + 0.5^θ  → rank 1. Probability 0.5^θ/ζ(n), exactly P(rank 2).
//  3. otherwise           → the closed-form inverse CDF of the continuous tail.
//
// Scaling u by ζ(n) turns "is u below the normalised CDF?" into "is u·ζ(n)
// below the UNnormalised cumulative weight?", which avoids dividing on every
// call.
func (z *Zipf) Next(r *rand.Rand) int {
	// u is uniform in [0,1); it plays the role of a probability level in
	// inverse-transform sampling.
	u := r.Float64()
	// uz is u rescaled into the unnormalised weight space [0, ζ(n)).
	uz := u * z.zetan
	// Head case 1: the first rank's weight is 1/1^θ = 1.
	if uz < 1 {
		return 0
	}
	// Head case 2: cumulative weight of ranks 1 and 2 is 1 + 1/2^θ.
	if uz < z.halfPowTheta {
		return 1
	}
	// Tail: rank = n·(η·u − η + 1)^α. As u → 1 the base → 1 and the rank → n;
	// for u just above the head threshold the base is small and, raised to the
	// large power α, gives a small rank. Truncation to int floors the result.
	i := int(float64(z.n) * math.Pow(z.eta*u-z.eta+1, z.alpha))
	// Floating-point rounding can in rare cases produce exactly n; clamp so the
	// result is always a valid index in [0, n).
	return min(i, z.n-1)
}

// KeyName formats key index i.
//
// The zero-padded fixed width ("bench-00000042") keeps every key the same
// length, so key size never varies between requests and never skews latency,
// and it makes keys sort in index order in any listing. The failure test
// (README Test D, scripts/bench_failure.sh) watches "bench-00000042" by
// default: a name in this same format, i.e. one of the keys the workload
// itself reads and writes, so the probe observes an ordinary benchmark key.
func KeyName(i int) string { return fmt.Sprintf("bench-%08d", i) }
