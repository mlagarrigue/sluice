# Étape 0 — Plafond de performance mesuré

> Projet : **sluice** (`github.com/mlagarrigue/sluice`)

> Exécuté le 2026-08-17 · Go 1.26.6 · linux/amd64
> AMD Ryzen AI 9 465 (15 cœurs visibles) · L1d 48 KiB/cœur · L2 1 MiB/cœur · L3 16 MiB
> `-benchtime=300ms -count=3`, médiane de 3 · code : [internal/bench/](../internal/bench/)

Sans dénominateur mesuré, « hyperperformant » n'est pas falsifiable
([ARCHITECTURE.md §13](ARCHITECTURE.md)). Ce document établit ce dénominateur et
valide — ou infirme — les hypothèses de la spec.

---

## Le plafond

**Boucle native `for range` sur `[]int64` : 0,310 ns/élément, 0 allocation.**

Toutes les mesures ci-dessous s'expriment en multiple de ce plafond.

---

## Résultat 1 — Le tout-batch est validé

Hypothèse de la §2.1, empruntée à la littérature BDD. **Confirmée**, à travail utile
strictement identique :

| Étages | Élément par élément | Lot de 1024 | Gain |
|---|---|---|---|
| 1 | 3,78 ns (×12,2) | 2,19 ns (×7,1) | **×1,72** |
| 2 | 7,19 ns (×23,2) | 3,66 ns (×11,8) | **×1,97** |
| 4 | 12,92 ns (×41,6) | 6,65 ns (×21,4) | **×1,94** |

Le gain se stabilise autour de **×1,9–2,0** dès deux étages. C'est moins que
l'ordre de grandeur annoncé par X100 (~50× sur MySQL), mais la comparaison n'est pas
la même : X100 mesurait un moteur SQL complet où 62 % du temps partait en navigation
de tuples, pas une boucle `Map` sur des entiers déjà en mémoire. **Sur le cas le plus
défavorable au batch — un travail utile quasi nul — le gain est déjà de ×2.**

### Coût marginal par étage

| Modèle | Pente |
|---|---|
| Élément par élément | **~2,6 ns par étage et par élément** |
| Lot de 1024 | **~1,5 ns par étage et par élément** |

Le coût par étage est **linéaire** dans les deux modèles : pas d'effondrement en
profondeur, un pipeline de 10 étages reste prévisible. Le batch réduit la pente de
~42 %.

> À retenir : l'entrée dans `iter.Seq` coûte déjà ×5,5 le plafond (1,72 ns) avant tout
> opérateur. Le premier étage est le plus cher ; les suivants sont marginaux.

---

## Résultat 2 — `iter.Pull` coûte 3,5× plus cher qu'annoncé

**La spec citait ~20 ns par valeur, de source communautaire. C'est faux ici.**

| Mesure | Coût |
|---|---|
| `iter.Pull`, tiré élément par élément | **68,6 ns/élément** (×221) |
| Vérification sur harnais minimal | **70,0 ns/élément** |
| Même source en push direct | 1,44 ns/élément |
| `iter.Pull`, tiré **par lot de 1024** | **0,49 ns/élément** (×1,6) |

Le surcoût du pull par rapport au push est d'environ **×48** sur la même source. Le
chiffre a été vérifié sur un second harnais dépouillé de toute indirection pour
écarter un artefact de mesure.

> **Conséquence pour la spec.** La §2.4 doit porter 68 ns, pas 20 ns. La règle
> « `iter.Pull` uniquement sur des lots » passe de recommandation à **obligation
> absolue** : à 68,6 ns/élément, un `Pull` tuple-à-tuple est 221× le plafond et
> disqualifie tout chemin chaud qui l'emploierait.

---

## Résultat 3 — Le merge par lot est effectivement négligeable

Validation directe de la §7.3, qui était une projection arithmétique :

| Merge de 2 flux | Coût |
|---|---|
| Élément par élément (2 × `iter.Pull`) | **69,8 ns/élément** (×225) |
| Par lot de 1024 | **0,39 ns/élément** (×1,3) |

**×179 d'écart.** L'opérateur `Merge` est à 1,3× le plafond en batch — c'est-à-dire
essentiellement gratuit — alors qu'il est inutilisable en tuple-à-tuple.

La §7.3 affirmait « c'est le tout-batch qui rend l'opérateur possible, pas seulement
plus rapide ». **C'est mesuré, et l'affirmation est plus forte que prévu** : le calcul
de la spec projetait ~0,02 ns/élément à partir d'un coût de 20 ns ; le coût réel étant
de 68,6 ns, on obtient 0,39 ns — toujours négligeable, mais 20× plus que projeté.

---

## Résultat 4 — La taille de lot : le plateau commence à 8

C'est le résultat le plus inattendu.

| Taille | ns/élément |
|---|---|
| 1 | 7,005 |
| **8** | **3,515** |
| 64 | 3,790 |
| 256 | 3,687 |
| 512 | 3,604 |
| **1024** | **3,558** |
| 2048 | 3,561 |
| 8192 | 3,555 |
| 65536 | 3,551 |
| 1048576 (tout) | 3,509 |

**Le plateau est atteint dès 8 éléments, et il est parfaitement plat jusqu'à 1M.**
L'écart entre 8 et 1024 est de 1,2 % — dans le bruit. Entre 1024 et 8192, 0,1 %.

Deux conséquences :

1. **Le choix de 1024 est sûr, mais pas critique.** Aucune raison de le changer pour
   2048 (DuckDB) ou 8192 (DataFusion), et aucune raison de s'inquiéter d'un lot qui
   descendrait à 64 ou 128 après un filtre.
2. **L'effondrement à droite prédit par X100 ne se produit pas ici** — parce que le
   travail mesuré est un `Map` en place sur un seul tableau, sans matérialisation
   intermédiaire. La courbe de X100 mesurait des vecteurs *multiples* vivants
   simultanément. **L'hypothèse « l'ensemble des lots vivants doit tenir en cache »
   n'est donc ni validée ni infirmée par ce test** — il faudra la remesurer sur un
   pipeline à plusieurs colonnes.

> **Réserve honnête.** Ce benchmark ne teste qu'un seul flux de `int64`. Le
> dimensionnement par le cache reste à vérifier sur un cas réaliste (jointure,
> plusieurs colonnes vivantes).

---

## Résultat 5 — Le filtre sélectif ne dégrade pas le batch

Cas à 1 % de sélectivité, celui qui motive `Coalesce` (§7.5) :

| Modèle | Coût |
|---|---|
| Élément par élément | 2,58 ns (×8,3) |
| Lot de 1024, lots creux, **sans** `Coalesce` | **0,71 ns (×2,3)** |

Le batch reste **3,6× plus rapide** même avec des lots réduits à ~10 éléments utiles,
et **sans aucune allocation**. Le filtrage par lot vers un tampon réutilisé évite le
coût d'indirection par élément.

> `Coalesce` reste justifié pour éviter la propagation de lots creux **en aval sur
> plusieurs étages**, mais ce n'est pas une urgence de performance sur un étage isolé.

---

## Synthèse — ce que les mesures changent dans la spec

| # | Constat | Effet |
|---|---|---|
| 1 | Tout-batch : **×1,9 à ×2,0** dès 2 étages | §2.1 **validée** |
| 2 | `iter.Pull` : **68,6 ns**, pas 20 ns | §2.4 et §7.3 à **corriger** |
| 3 | `Merge` par lot : **0,39 ns** (×1,3) | §7.3 **validée**, plus fortement que prévu |
| 4 | Plateau de taille de lot dès **8** éléments | §2.3 : 1024 confirmé, mais **non critique** |
| 5 | Filtre 1 % : batch reste ×3,6 plus rapide, 0 alloc | §7.5 : `Coalesce` **utile, non urgent** |
| 6 | Coût par étage **linéaire** (2,6 ns élément / 1,5 ns lot) | Pipelines profonds **prévisibles** |

### Le point le plus important pour la suite

L'écart entre le plafond (0,31 ns) et un pipeline batch de 4 étages (6,65 ns) est de
**×21**. Ce n'est pas « zéro-coût » — la spec a raison de ne rien promettre de tel.
Mais l'essentiel de cet écart est le travail utile lui-même (4 additions et 4
réécritures mémoire), pas la machinerie du stream.

**La machinerie coûte ~1,5 ns par étage et par élément.** C'est le chiffre à retenir,
et le budget à ne pas dépasser pour tout nouvel opérateur du noyau.

---

## Ce qui n'est pas encore mesuré

1. **Le dimensionnement par le cache** avec plusieurs lots vivants (cf. Résultat 4).
2. **Le coût des diagnostics** portés par élément (§4) — probablement le plus gros
   risque de régression du modèle.
3. **`Split` et le lock-step** (§6.3), dont la promesse O(1) reste théorique.
4. **Le hash join borné** (§9.1) et son coût de matérialisation.
5. **Le décodage colonnaire** contre le décodage ligne à ligne (§8.6).
