// Package bench implements the load generator and the offline
// key-redistribution experiment.
package bench

import (
	"fmt"
	"math"
	"math/rand/v2"
)

// KeyGen picks key indexes in [0, n).
type KeyGen interface {
	Next(r *rand.Rand) int
}

// Uniform picks every key with equal probability.
type Uniform struct{ N int }

// Next implements KeyGen.
func (u Uniform) Next(r *rand.Rand) int { return r.IntN(u.N) }

// Zipf draws key ranks from a Zipfian distribution with exponent theta,
// P(rank i) ∝ 1/i^theta, using the rejection-free method of Gray et al.,
// "Quickly Generating Billion-Record Synthetic Databases" (SIGMOD '94) — the
// same generator YCSB uses. Unlike math/rand's Zipf it supports theta < 1;
// YCSB's default theta of 0.99 makes a small set of keys very hot.
//
// Rank 0 is the hottest key. Keys are hashed onto the ring, so hot keys land
// on arbitrary nodes rather than all on one.
type Zipf struct {
	n                        int
	theta, alpha, zetan, eta float64
	halfPowTheta             float64
}

// NewZipf precomputes the generator's constants (O(n) once).
func NewZipf(n int, theta float64) (*Zipf, error) {
	if n < 1 {
		return nil, fmt.Errorf("zipf: keyspace must be >= 1")
	}
	if theta <= 0 || theta >= 1 {
		return nil, fmt.Errorf("zipf: theta must be in (0,1), got %v", theta)
	}
	zeta := func(n int) float64 {
		s := 0.0
		for i := 1; i <= n; i++ {
			s += 1 / math.Pow(float64(i), theta)
		}
		return s
	}
	z := &Zipf{n: n, theta: theta, alpha: 1 / (1 - theta), zetan: zeta(n)}
	zeta2 := zeta(2)
	z.eta = (1 - math.Pow(2/float64(n), 1-theta)) / (1 - zeta2/z.zetan)
	z.halfPowTheta = 1 + math.Pow(0.5, theta)
	return z, nil
}

// Next implements KeyGen.
func (z *Zipf) Next(r *rand.Rand) int {
	u := r.Float64()
	uz := u * z.zetan
	if uz < 1 {
		return 0
	}
	if uz < z.halfPowTheta {
		return 1
	}
	i := int(float64(z.n) * math.Pow(z.eta*u-z.eta+1, z.alpha))
	return min(i, z.n-1)
}

// KeyName formats key index i.
func KeyName(i int) string { return fmt.Sprintf("bench-%08d", i) }
