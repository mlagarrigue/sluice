// Package bench établit le plafond de performance des primitives d'itération
// Go 1.26, préalable à toute décision d'architecture (cf. docs/ARCHITECTURE.md §13,
// étape 0).
//
// Quatre questions :
//  1. Quel est le plafond ? (boucle native = dénominateur de toutes les mesures)
//  2. Combien coûte un étage d'opérateur, et le coût est-il linéaire ?
//  3. Combien coûte réellement iter.Pull par valeur ?
//  4. Le tout-batch tient-il sa promesse face au tuple-à-tuple ?
package bench

import (
	"fmt"
	"iter"
	"testing"
)

// N est la taille du jeu de données pour tous les benchmarks à volume fixe.
// Assez grand pour sortir du cache L2 et mesurer un régime réaliste.
const N = 1 << 20 // 1 048 576 éléments

func data(n int) []int64 {
	s := make([]int64, n)
	for i := range s {
		s[i] = int64(i)
	}
	return s
}

// sink empêche le compilateur d'éliminer les calculs mesurés.
var sink int64

// ---------------------------------------------------------------------------
// Q1 — Le plafond : la boucle native.
// Toutes les autres mesures s'expriment en pourcentage de celle-ci.
// ---------------------------------------------------------------------------

func BenchmarkBaselineLoop(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var acc int64
		for _, v := range src {
			acc += v
		}
		sink = acc
	}
	reportPerElem(b, N)
}

// ---------------------------------------------------------------------------
// Q2 — Coût d'un étage : iter.Seq élément par élément, 0 à 4 étages de Map.
// ---------------------------------------------------------------------------

type Seq[T any] iter.Seq[T]

func seqOf[T any](s []T) Seq[T] {
	return func(yield func(T) bool) {
		for _, v := range s {
			if !yield(v) {
				return
			}
		}
	}
}

func mapSeq[A, B any](s Seq[A], f func(A) B) Seq[B] {
	return func(yield func(B) bool) {
		s(func(a A) bool { return yield(f(a)) })
	}
}

func filterSeq[T any](s Seq[T], pred func(T) bool) Seq[T] {
	return func(yield func(T) bool) {
		s(func(v T) bool {
			if !pred(v) {
				return true
			}
			return yield(v)
		})
	}
}

func inc(v int64) int64 { return v + 1 }

// BenchmarkSeqStages mesure le surcoût marginal de chaque étage ajouté.
// La pente de la droite est le coût d'un étage ; l'ordonnée à l'origine
// est le coût d'entrée dans iter.Seq.
func BenchmarkSeqStages(b *testing.B) {
	src := data(N)
	for _, stages := range []int{0, 1, 2, 4} {
		b.Run(fmt.Sprintf("stages=%d", stages), func(b *testing.B) {
			b.SetBytes(N * 8)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				s := seqOf(src)
				for i := 0; i < stages; i++ {
					s = mapSeq(s, inc)
				}
				var acc int64
				s(func(v int64) bool {
					acc += v
					return true
				})
				sink = acc
			}
			reportPerElem(b, N)
		})
	}
}

// ---------------------------------------------------------------------------
// Q4 — Le cœur du sujet : lot contre élément, à travail utile identique.
// ---------------------------------------------------------------------------

type Batch[T any] struct {
	Items []T
}

type BatchSeq[T any] iter.Seq[Batch[T]]

func batchesOf[T any](s []T, size int) BatchSeq[T] {
	return func(yield func(Batch[T]) bool) {
		for i := 0; i < len(s); i += size {
			end := min(i+size, len(s))
			if !yield(Batch[T]{Items: s[i:end]}) {
				return
			}
		}
	}
}

// mapBatch applique f à tout le lot. C'est le point clé du modèle :
// une seule indirection de closure par lot, et une boucle interne serrée
// que le compilateur peut optimiser.
func mapBatch[T any](s BatchSeq[T], f func(T) T) BatchSeq[T] {
	return func(yield func(Batch[T]) bool) {
		s(func(b Batch[T]) bool {
			items := b.Items
			for i := range items {
				items[i] = f(items[i])
			}
			return yield(b)
		})
	}
}

// BenchmarkBatchVsElement compare les deux modèles à nombre d'étages égal.
// C'est la mesure qui valide ou invalide la §2.1 de la spec.
func BenchmarkBatchVsElement(b *testing.B) {
	for _, stages := range []int{1, 2, 4} {
		b.Run(fmt.Sprintf("element/stages=%d", stages), func(b *testing.B) {
			src := data(N)
			b.SetBytes(N * 8)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				s := seqOf(src)
				for i := 0; i < stages; i++ {
					s = mapSeq(s, inc)
				}
				var acc int64
				s(func(v int64) bool {
					acc += v
					return true
				})
				sink = acc
			}
			reportPerElem(b, N)
		})

		b.Run(fmt.Sprintf("batch1024/stages=%d", stages), func(b *testing.B) {
			src := data(N)
			b.SetBytes(N * 8)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				s := batchesOf(src, 1024)
				for i := 0; i < stages; i++ {
					s = mapBatch(s, inc)
				}
				var acc int64
				s(func(bt Batch[int64]) bool {
					for _, v := range bt.Items {
						acc += v
					}
					return true
				})
				sink = acc
			}
			reportPerElem(b, N)
		})
	}
}

// BenchmarkBatchSize balaie la taille de lot pour situer le plateau optimal.
// La spec retient 1024 ; DuckDB utilise 2048, DataFusion 8192.
func BenchmarkBatchSize(b *testing.B) {
	sizes := []int{1, 8, 64, 256, 512, 1024, 2048, 4096, 8192, 65536, N}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			src := data(N)
			b.SetBytes(N * 8)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				s := batchesOf(src, size)
				s = mapBatch(s, inc)
				s = mapBatch(s, inc)
				var acc int64
				s(func(bt Batch[int64]) bool {
					for _, v := range bt.Items {
						acc += v
					}
					return true
				})
				sink = acc
			}
			reportPerElem(b, N)
		})
	}
}

// ---------------------------------------------------------------------------
// Q3 — iter.Pull : le coût réel par valeur, et son amortissement par lot.
// La spec cite ~20ns/next() de source communautaire, non documenté par Go.
// ---------------------------------------------------------------------------

func BenchmarkPullPerElement(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		next, stop := iter.Pull(iter.Seq[int64](seqOf(src)))
		var acc int64
		for {
			v, ok := next()
			if !ok {
				break
			}
			acc += v
		}
		stop()
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkPullPerBatch : le même Pull, mais tiré par lots.
// C'est l'argument de la §7.3 — le coût de la coroutine amorti sur 1024.
func BenchmarkPullPerBatch(b *testing.B) {
	src := data(N)
	b.SetBytes(N * 8)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		next, stop := iter.Pull(iter.Seq[Batch[int64]](batchesOf(src, 1024)))
		var acc int64
		for {
			bt, ok := next()
			if !ok {
				break
			}
			for _, v := range bt.Items {
				acc += v
			}
		}
		stop()
		sink = acc
	}
	reportPerElem(b, N)
}

// BenchmarkMergeTwoStreams mesure le coût réel d'un opérateur N->1 dans les
// deux modèles. C'est la validation directe de la §7.3 : le merge par lot
// est-il vraiment négligeable ?
func BenchmarkMergeTwoStreams(b *testing.B) {
	b.Run("element", func(b *testing.B) {
		a, c := data(N/2), data(N/2)
		b.SetBytes(N * 8)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			nextA, stopA := iter.Pull(iter.Seq[int64](seqOf(a)))
			nextC, stopC := iter.Pull(iter.Seq[int64](seqOf(c)))
			var acc int64
			okA, okC := true, true
			for okA || okC {
				if okA {
					var v int64
					if v, okA = nextA(); okA {
						acc += v
					}
				}
				if okC {
					var v int64
					if v, okC = nextC(); okC {
						acc += v
					}
				}
			}
			stopA()
			stopC()
			sink = acc
		}
		reportPerElem(b, N)
	})

	b.Run("batch1024", func(b *testing.B) {
		a, c := data(N/2), data(N/2)
		b.SetBytes(N * 8)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			nextA, stopA := iter.Pull(iter.Seq[Batch[int64]](batchesOf(a, 1024)))
			nextC, stopC := iter.Pull(iter.Seq[Batch[int64]](batchesOf(c, 1024)))
			var acc int64
			okA, okC := true, true
			for okA || okC {
				if okA {
					var bt Batch[int64]
					if bt, okA = nextA(); okA {
						for _, v := range bt.Items {
							acc += v
						}
					}
				}
				if okC {
					var bt Batch[int64]
					if bt, okC = nextC(); okC {
						for _, v := range bt.Items {
							acc += v
						}
					}
				}
			}
			stopA()
			stopC()
			sink = acc
		}
		reportPerElem(b, N)
	})
}

// ---------------------------------------------------------------------------
// Cas réaliste : filtre sélectif suivi d'une transformation.
// Vérifie que le modèle batch résiste quand les lots se vident (cf. Coalesce, §7.5).
// ---------------------------------------------------------------------------

func BenchmarkSelectiveFilter(b *testing.B) {
	// keep=1 sur 100 : le cas qui motive Coalesce.
	keep := func(v int64) bool { return v%100 == 0 }

	b.Run("element", func(b *testing.B) {
		src := data(N)
		b.SetBytes(N * 8)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			s := filterSeq(seqOf(src), keep)
			s = mapSeq(s, inc)
			var acc int64
			s(func(v int64) bool {
				acc += v
				return true
			})
			sink = acc
		}
		reportPerElem(b, N)
	})

	b.Run("batch1024_nocoalesce", func(b *testing.B) {
		src := data(N)
		b.SetBytes(N * 8)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			// Filtre par lot : produit des lots creux (10 éléments sur 1024).
			out := make([]int64, 0, 1024)
			var acc int64
			batchesOf(src, 1024)(func(bt Batch[int64]) bool {
				out = out[:0]
				for _, v := range bt.Items {
					if keep(v) {
						out = append(out, v+1)
					}
				}
				for _, v := range out {
					acc += v
				}
				return true
			})
			sink = acc
		}
		reportPerElem(b, N)
	})
}

// reportPerElem exprime le résultat en nanosecondes par élément — l'unité
// qui permet de comparer des benchmarks de volumes différents.
func reportPerElem(b *testing.B, elems int) {
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*elems), "ns/elem")
}
