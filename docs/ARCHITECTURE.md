# Architecture — sluice

> Statut : **spec v0.3**
> Module : `github.com/mlagarrigue/sluice`
> Go 1.26 · 0 dépendance de production · pull-based, tout-batch
>
> La v0.3 intègre sept recherches et les retours de relecture de la v0.2.
> Décisions révisées et sources en §14.

---

## 1. Principe fondateur

Le framework a **un seul concept** : le `Stream`, une séquence potentiellement
infinie de **lots** de valeurs, dont le consommateur contrôle le débit.

**Tout est opération de stream.** Ce n'est pas une façon de parler : le parsing
HTTP, le routage, la sécurité, les règles métier, la lecture SQL, l'hydratation
d'objets et la sérialisation sont **tous** des opérateurs de la même algèbre. Il
n'y a pas de « handler » au milieu du pipeline qui serait d'une autre nature — le
traitement métier est un opérateur comme `Map` ou `Filter`.

### 1.1 Exemple web complet — un PATCH

Chaque flèche est un opérateur de stream, sans exception :

```
Stream[Batch[byte]]        ← lecture socket, tampons poolés
  → Stream[Batch[Frame]]      parsing protocole
  → Stream[Batch[Request]]    décodage, limites
  → Stream[Batch[Route]]      routage
  → Stream[Batch[Params]]     extraction + validation des paramètres
  → Stream[Batch[Params]]     sécurité (authn/authz)          ← opérateur
  → Stream[Batch[Params]]     middleware métier de sécurité   ← opérateur
  → Stream[Batch[Entity]]     lecture DB à partir du batch de params
  → Stream[Batch[Entity]]     amendement de la donnée         ← opérateur
  → Stream[Batch[Entity]]     règles métier (ligne.qteVente > 0)
  → Stream[Batch[Entity]]     sauvegarde
  → Stream[Batch[Response]]   projection du résultat
  → Stream[Batch[byte]]       sérialisation
```

Les diagnostics (§4) circulent **avec** les entités tout au long de ce pipeline —
un élément peut être valide, porter trois avertissements, et continuer d'avancer.

### 1.2 Exemple base de données

Le connecteur suit exactement la même forme :

```
Stream[Batch[byte]]     ← protocole wire PostgreSQL
  → Stream[Batch[Row]]      décodage des DataRow, format binaire
  → Stream[Batch[Entity]]   hydratation générée, colonne par colonne
```

Cela implique de **réimplémenter les connecteurs** : `database/sql` ne sait pas
batcher (§8.1). C'est un coût assumé et budgété.

| Domaine | Lecture en termes de stream |
|---|---|
| Web | `byte → Frame → Request → … → Response → byte` |
| Base de données | `byte → Row → Entity`, jointures = opérateurs |
| ETL | source → transformations → sink, la définition littérale |
| Messaging | `Message` avec jointure par intervalle temporel |

**Positionnement vérifié.** Aucun framework web Go n'est bâti sur une abstraction
stream de bout en bout ; aucun framework, dans aucun langage, n'unifie web + BDD +
ETL sur un stream unique. Akka Streams est le précédent le plus proche sans couvrir
les trois. Le terrain est libre — ce n'est ni une garantie ni une preuve que l'idée
est bonne.

---

## 2. Le type central — tout-batch

### 2.1 Décision : une seule forme, le lot

```go
// Stream est une séquence de lots dont le consommateur contrôle le débit.
type Stream[T any] iter.Seq[Batch[T]]

type Batch[T any] struct {
    Items []T          // longueur variable, capacité stable (pool)
    Ctx   *Context     // contexte porté AU NIVEAU DU LOT, pas de l'élément
}
```

**La v0.2 proposait deux formes (`Stream[T]` et `BatchStream[T]`). C'était une
erreur**, pour trois raisons :

1. **Un lot de taille 1 est un cas dégénéré, pas un type distinct.** Si une
   fonction traite N éléments efficacement, elle en traite 1 sans effort.
2. **Deux formes = chaque opérateur écrit deux fois**, une frontière à arbitrer
   partout, et la tentation permanente de rester sur la forme lente « pour la
   lisibilité ».
3. **L'algorithme est bien plus optimisable en lot** : décodage colonnaire,
   vérification du contexte amortie, pushdown à granularité du lot, un aller-retour
   réseau par lot.

C'est aussi ce que fait X100 : `next()` retourne **toujours** un vecteur.

### 2.2 Effet de bord bénéfique : le signal d'arrêt cesse d'être ambigu

Avec `iter.Seq[T]`, la signature `func(yield func(T) bool)` mélange deux canaux :
la valeur transportée et le signal de continuation. Sur un `Stream[bool]`, on ne
les distingue plus, et un opérateur générique inspectant le retour de `yield` ne
peut pas savoir s'il lit une donnée ou un signal.

En tout-batch, la signature devient `func(yield func(Batch[T]) bool)` : **le `bool`
de contrôle n'est jamais du même type que les éléments transportés.** L'ambiguïté
disparaît par construction.

### 2.3 Taille de lot : ~1024 par défaut

Critère : *l'ensemble des lots vivants simultanément* doit tenir en cache CPU.

**Mesuré** (étape 0) : le plateau est atteint dès **8 éléments** et reste plat jusqu'à
1M — 1,2 % d'écart entre 8 et 1024. Le choix de 1024 est sûr mais **non critique**, et
un lot qui descend à 64 après un filtre ne dégrade rien.

*Réserve : ce test ne mesure qu'un seul flux d'`int64`. Le dimensionnement par le cache
reste à vérifier avec plusieurs lots vivants simultanément.*

### 2.4 Coût réel — ce qui est promis et ce qui ne l'est pas

- `iter.Seq` est **push-style côté machine** : le compilateur Go inline la closure
  `yield`, rendant `for v := range seq` proche d'une boucle native.
- Go n'a **pas** de monomorphisation. Rust est un modèle conceptuel, **pas un
  modèle de coût** — aucune promesse de « zéro-coût » n'est faite ici.
- `iter.Pull` coûte une coroutine runtime et un changement de contexte **par valeur
  tirée** : **68,6 ns/élément mesurés** (×221 le plafond), contre 1,4 ns en push
  direct. Il est réservé aux jointures en lockstep et au merge, et alors **sur des
  lots** — jamais sur des éléments. Ce n'est pas une recommandation mais une
  **obligation** : voir [BENCHMARK-ETAPE-0.md](BENCHMARK-ETAPE-0.md).

Le tout-batch amortit l'appel indirect sur ~1024 éléments : le coût par étage
devient négligeable devant le travail utile.

---

## 3. Contexte, annulation et arrêt

### 3.1 Correction de la v0.2

La v0.2 affirmait qu'un `Stream` « n'embarque ni runtime, ni contexte, ni registre
global ». **C'était mal écrit et faux tel quel.** Ce qui est vrai : il n'y a ni
registre global ni conteneur d'injection à configurer. Mais un contexte HTTP, un
contexte utilisateur, un tenant — évidemment que ça circule.

**Le contexte est porté au niveau du lot** (`Batch.Ctx`), pas répété sur chaque
élément. C'est un bénéfice direct du tout-batch.

Les contextes sont **stratifiés** : le contexte HTTP existe jusqu'à la couche HTTP,
le contexte utilisateur/métier au-delà. Un opérateur ne voit que la strate qui le
concerne.

### 3.2 Trois causes d'arrêt, à ne pas confondre

| Cause | Origine | Mécanisme |
|---|---|---|
| Fin normale | la source est épuisée, ou LIMIT atteinte | la génératrice retourne |
| Arrêt consommateur | l'aval n'a plus besoin de rien | `yield` → `false` |
| **Arrêt interne** | un étage échoue (parsing HTTP KO) | `yield` → `false` **+ cause** |
| **Annulation externe** | l'utilisateur annule | `ctx` capturé à la source |

**Le patron retenu est celui de `context`** : signal binaire rapide dans la boucle
chaude, **cause consultée après coup** via un accesseur `Err() error`. `errors.Is`
suffit à discriminer `nil` (fin normale) / erreur métier / `context.Canceled`.

C'est nécessaire parce qu'un étage du **milieu** qui décide d'arrêter peut couper
l'amont (en retournant), mais ne peut signaler l'aval qu'en terminant sa séquence —
ce qui est indistinguable d'une fin normale. Aucun des systèmes étudiés ne mélange
erreur et annulation dans le même signal.

> **Garantie du runtime.** Le contrat `iter` impose que `yield` **panique** s'il est
> appelé après avoir renvoyé `false`. La propagation d'arrêt n'est donc pas une
> convention : elle est vérifiée par le runtime.

### 3.3 Fréquence de vérification

**Une vérification de `ctx` par lot, jamais par élément.** C'est le pattern
universel des moteurs de BDD (PostgreSQL sème ses `CHECK_FOR_INTERRUPTS()` « at safe
places »). Hoister `done := ctx.Done()` hors de la boucle.

`context.Context` est un **paramètre de la fonction constructrice**, jamais un champ
caché non lu.

### 3.4 Signal d'arrêt enrichi — le pushdown

Un consommateur peut vouloir dire mieux que « stop » : *« saute jusqu'à la clé X »*,
*« je n'ai plus besoin de la colonne Y »*. Ce mécanisme existe et porte un nom :
**sideways information passing**.

L'implémentation moderne de référence est les *dynamic filters* de DataFusion : un
opérateur `TopK` en aval transmet au scan en amont le seuil courant, **qui se
resserre au fil de l'exécution** ; le scan saute alors des lignes, puis des fichiers
entiers. Gains mesurés jusqu'à **22×** (ClickBench Q23) et **25×** sur jointures.

Mécanisme transposable et compatible 0dep : un objet de demande partagé plus un
**compteur de génération atomique** que le lecteur compare sans verrou. Le chemin
chaud ne paie qu'une lecture atomique **par lot**.

```go
type Demand struct {
    generation atomic.Uint64   // le scan compare, sans verrou
    // seuils, colonnes requises, limite restante…
}
```

> Voir §13 pour le rattachement à l'étape d'implémentation. C'est la piste de
> performance à plus fort levier identifiée par les recherches.

---

## 4. Diagnostics — erreurs, avertissements et affordances

C'est le sujet le plus riche de la v0.3, et le besoin réel dépasse largement un
`Result[T]`.

### 4.1 Le besoin

Dans un flux PATCH, on veut pouvoir produire, **sans interrompre le flux** :

```
Error    1000  QteMustBeOverZero  commande[42].ligne[7].qteVente
Critical 0001  ClientMustExists   commande[42].idClient
Warning  2010  PrixInhabituel     commande[42].ligne[3].prixUnitaire
```

avec, dans les logs, la trace lisible permettant de remonter de
`Critical 0001 ClientMustExists` jusqu'à son origine réelle — une contrainte SQL
`NOT NULL` à la lecture, ou un échec d'hydratation.

### 4.2 Ce modèle existe déjà — deux fois

**FHIR OperationOutcome** (HL7) porte exactement cette forme : `severity`
(`fatal | error | warning | information`), `code`, `details`, et `expression` — un
chemin avec index, `Patient.identifier[2].value`. Point qui résout l'hésitation
« code 1000 » contre « classification Vente\Commande » : FHIR utilise **les deux**,
un code issu d'un vocabulaire fermé *et* un code applicatif dans `details`.

**SARIF 2.1.0** (OASIS) apporte les deux pièces manquantes :

- **i18n** : un objet `message` avec `id` (la clé) et `arguments` (les valeurs) ; le
  catalogue de règles est séparé des occurrences. **La clé voyage, pas le texte rendu.**
- **traçabilité causale** : `codeFlows` / `threadFlows` avec ordre d'exécution et
  niveau d'importance.

Contre-exemple utile : `ValidationProblemDetails` (.NET) a la bonne forme de chemin
(`Order.Lines[3].Quantity`) mais une charge trop pauvre — un tableau de messages
déjà rendus, sans niveau, sans code, sans clé. **N'en reprendre que le chemin.**

### 4.3 Le modèle retenu

```go
type Severity uint8   // Info, Warning, Error, Critical

type Diagnostic struct {
    Severity  Severity
    Code      string          // "Vente.Commande.QteInvalide" — hiérarchique
    RuleID    string          // clé dans le catalogue de règles
    MessageID string          // clé i18n : "QteMustBeOverZero"
    Args      map[string]any  // arguments NOMMÉS, pas positionnels
    Path      Path            // structuré, PAS une chaîne
    Origin    Origin          // SQL | Hydratation | RègleMétier | Protocole
    Affords   []Affordance    // ce qui est modifiable, par quelle action
    cause     error           // NON exporté — jamais sérialisé
}
```

Quatre décisions, chacune motivée :

**`Path` est une structure typée, pas une chaîne.** Concaténer des chaînes piège sur
l'échappement (JSON Pointer impose `~0`/`~1`) et interdit le regroupement par
sous-arbre. On rend ensuite en FHIRPath **ou** en JSON Pointer selon le consommateur.
Le chemin est celui du **modèle métier**, pas du document JSON entrant — deux espaces
de noms à ne pas confondre. FHIR a d'ailleurs déprécié son champ `location`, lié au
format de sérialisation, au profit d'`expression`, lié au modèle.

**Arguments nommés.** C'est la faiblesse reconnue de SARIF avec ses `{0}`/`{1}` : les
langues réordonnent, et un traducteur ne s'en sort pas sans ambiguïté.

**La cause reste non exportée**, avec `Unwrap() error`. Le sérialiseur HTTP ne peut
alors **structurellement pas** fuiter l'interne : la garantie S7 devient impossible à
violer au lieu d'être recommandée. À la frontière réseau, la cause est remplacée par
un identifiant de corrélation que seul le log résout.

**La gravité n'est pas la décision de flux.** FHIR distingue `fatal` et `error` ;
SARIF sépare `level` et `kind`. « À quel point c'est grave » et « faut-il s'arrêter »
sont deux axes orthogonaux.

### 4.4 Affordances — le trou à combler

Aucun format ne fusionne validation et affordances. Les briques existent séparément :
HAL-FORMS a `readOnly`, `regex`, `required` par propriété ; Siren a `method`, `href`
et `fields` pour l'action.

**La jointure que personne n'a standardisée : attacher l'affordance au même `Path`
que le diagnostic.** Le même `commande[42].ligne[7].qteVente` porte à la fois « voici
pourquoi c'est invalide » et « voici comment le corriger, par quelle action, avec quel
type ». C'est la valeur ajoutée du modèle, et c'est un pari — aucun précédent à copier.

### 4.5 Diagnostics dans un stream : ce qu'aucun standard ne fait

SARIF, FHIR et Problem Details sont tous des **documents finis**. Sur 1000 entités ×
N diagnostics, la mémoire explose.

> **Invariant.** Plafond de diagnostics par élément **et** par lot, avec ordre stable.
> Au-delà, un compteur de troncature. Sans cela, un lot pathologique noie le signal.

**Capture de trace conditionnelle** : `runtime.Callers` est coûteux et le package
`errors` de Go ne capture rien. Trace uniquement si `Severity >= Error` ou en mode
debug.

### 4.6 Articulation avec `Result[T]`

`Diagnostic` **ne remplace pas** la gestion d'erreur du flux, il la complète :

| Mécanisme | Rôle |
|---|---|
| `[]Diagnostic` porté par l'élément | diagnostic **métier** — n'interrompt jamais |
| `Result[T]` dans le flux | l'élément n'a **pas pu** être produit |
| `Source.Err()` hors flux | échec de **setup / teardown** (§4.7) |
| `Stream.Err()` | **cause d'arrêt** du flux (§3.2) |

Ne pas réutiliser `error` pour un diagnostic métier : un avertissement n'est pas une
erreur de traitement.

### 4.7 Erreurs hors flux

Un `Result[T]` **dans** le flux ne peut porter ni l'échec d'ouverture d'un fichier
(aucun élément n'a existé) ni l'échec d'un `Close()` (après le dernier élément).

```go
type Source[T any] struct {
    Stream Stream[T]
    Err    func() error   // setup + teardown ; consulté APRÈS consommation
}
```

> **Invariant de terminalité.** Un `Result` porteur d'erreur **n'arrête pas** le flux.
> Un opérateur qui doit s'arrêter à la première erreur le déclare via `OrFail`. Cette
> règle n'étant pas exprimable dans la signature, elle est **vérifiée par test**.

---

## 5. Taxonomie des opérateurs

Classer par **comportement mémoire** est le socle : c'est ce qui détermine si un
pipeline peut traiter un flux infini.

**Sans état — O(1).** `Map` · `Filter` · `FlatMap` · `Peek` · `Scan` · `Take` ·
`Drop` · `TakeWhile` · `DropWhile` · `Concat` · `Merge` · `Interleave` ·
`MergeJoinBy` · `ZipLongest`. Composables à l'infini.

Les cinq derniers sont les opérateurs N→1 (§7) : ils fusionnent plusieurs flux **sans
état**, `MergeJoinBy` et `ZipLongest` sous condition d'entrées triées ou consommées
en lockstep.

**État borné — O(k), k fixé à la construction.** `Rebatch(n)` · `Coalesce(n)` ·
`Window(durée)` · `Distinct(n)` · `Buffer(n)` · `Split` · `Parallel`. Chacun **déclare sa politique de
débordement** (bloquer, jeter l'ancien, jeter le récent, erreur) — la back-pressure ne
supprime pas la pression, elle la remonte jusqu'à un point où on peut la traiter, et
ces opérateurs *sont* ce point.

**Bloquants — O(n).** `Sort` · `GroupBy` · `Collect` · `Join` (côté build) ·
`Materialize`. **Incompatibles avec un flux infini.**

> **Leçon Java 8.** Ne jamais laisser une contrainte de parallélisme amputer l'API
> séquentielle : Java s'est privé de `zip`, `foldLeft` et `takeWhile` pour préserver
> la parallélisabilité, et une étude sur 5,5 MLOC montre que le parallélisme y est
> très peu utilisé. Tout le monde a payé pour presque personne.

---

## 6. Split — une primitive unique

### 6.1 Trois usages, une fonction

Partition (succès/erreurs), fan-out parallèle et clonage sont **la même opération**
paramétrée par une fonction de routage. C'est le modèle Akka Streams :

| Mode | Qui reçoit | Ordre | Route |
|---|---|---|---|
| **Partition** | une branche, choisie | préservé par branche | `[i]` |
| **Balance** | une branche, la première libre | **aucune garantie** | round-robin |
| **Broadcast** | toutes les branches | préservé par branche | `[0..N-1]` |

```go
func Split[T any](s Stream[T], n int, route func(Batch[T]) []int) []Stream[T]
```

> **Réserve à documenter.** Partition et Broadcast préservent l'ordre par branche ;
> **Balance ne garantit aucun ordre** et est non déterministe. Unifier les trois est
> justifié, mais les garanties **diffèrent** et doivent être annoncées par mode.

### 6.2 Le théorème d'impossibilité

Avec une source non rejouable, on ne peut pas avoir simultanément :
**branches indépendantes**, **source lue une seule fois**, **mémoire bornée**.
Il faut en sacrifier un.

| Stratégie | Sacrifice | Qui |
|---|---|---|
| Buffer non borné | **mémoire** | Web Streams, Python `tee`, Rust `itertools::tee` |
| Buffer borné + blocage | **indépendance** | Akka `Broadcast` |
| Buffer borné + drop | **complétude** | Akka `OverflowStrategy` |
| Re-exécution de la source | **CPU/IO ×N** | rejeu de `iter.Seq` |
| Matérialisation O(n) | **mémoire = total** | Python recommande `list()` |

MDN est explicite sur le choix de Web Streams : *« unread data is enqueued internally
on the slower consumed ReadableStream without any limit or backpressure »*. **C'est
l'anti-modèle** au regard de notre garantie S1.

### 6.3 La cinquième option — celle qu'on retient

**Le buffer n'est pas le prix de la duplication, c'est le prix du désalignement.**

En mono-goroutine, si `Split` **pousse** vers N handlers en lock-step au lieu
d'exposer N `iter.Seq` indépendants, le backlog reste **O(1)** sans le couplage
pathologique d'Akka. C'est le levier qui réconcilie nos trois objectifs, et il n'est
disponible que parce qu'on est pull et mono-goroutine.

Corollaire pour le clonage (§10.1) : si la source est rejouable, la re-exécuter ; sinon
`Materialize` **explicite**. Ne jamais construire un tee non borné implicite.

### 6.4 `Parallel` — limite structurelle

Rayon (Rust) parallélise parce que ses sources sont *splittables* (`split_at`). Une
`iter.Seq` opaque **ne l'est pas**. Notre seule option est le fan-out par lot, d'où :

- soit la perte de l'ordre (`Parallel`), soit un tampon de réordonnancement O(k) ;
- perte locale des traces natives → enrichir le contexte d'erreur au franchissement ;
- `Parallel(1)` doit être un no-op explicite, jamais un chemin concurrent silencieux.

Le lot est ici un avantage : le fan-out se fait par lot de ~1024, donc le coût de
coordination est amorti.

---

## 7. Opérateurs N→1 — fusionner plusieurs flux

`Split` (§6) décompose un flux en N. Cette section couvre l'opération inverse. Ce sont
deux familles distinctes, et **la fusion n'est pas la jointure** : joindre apparie des
éléments sur une clé, fusionner combine des séquences.

### 7.1 Taxonomie

| Opérateur | Sémantique | Mémoire | Ordre |
|---|---|---|---|
| `Concat` | A entier, puis B entier | O(1) | déterministe |
| `Merge` | entrelacement, par lot | O(1) | non déterministe |
| `Interleave` | alternance stricte N de A, N de B | O(1) | strictement alterné |
| `MergeJoinBy` | fusion ordonnée de flux triés | **O(1)** | trié |
| `ZipLongest` | paires positionnelles, va au plus long | O(1) | positionnel |
| `Coalesce` | recombine les lots à une taille cible | O(k) | préservé |
| ~~`Union`/`Intersect`/`Except` non triés~~ | déduplication par HashSet | **O(distinct)** | — |

Les trois derniers **n'existent pas** comme opérateurs de flux : un HashSet non borné
viole S1. Ils sont fournis uniquement en variante triée, dérivée de `MergeJoinBy`
(§7.2) — c'est la stratégie sort-merge des moteurs SQL.

### 7.2 `MergeJoinBy` — la primitive centrale

Un seul opérateur subsume six sémantiques, en mémoire O(1), sur des flux
**potentiellement infinis** à condition qu'ils soient triés sur la clé.

```go
type EitherOrBoth[L, R any] struct {
    Left  *L   // nil si absent
    Right *R   // nil si absent
}

func MergeJoinBy[L, R any](
    left  Stream[L],
    right Stream[R],
    cmp   func(L, R) int,
) Stream[EitherOrBoth[L, R]]
```

Chaque sémantique est un **filtrage en aval**, pas un opérateur distinct :

| Sémantique | Filtre |
|---|---|
| Inner join | garder `Both` |
| Left join | garder `Both` + `Left` |
| Full outer join | tout garder |
| Intersect | garder `Both` |
| Except (A \ B) | garder `Left` |
| Union | tout garder |

C'est le meilleur rapport puissance/code de toute la taxonomie, et le seul opérateur
binaire qui fusionne deux flux infinis sans état.

> **Articulation avec §8.** `MergeJoinBy` **est** la primitive du merge join ; l'entrée
> « merge join » de la table des stratégies (§8.2) y renvoie et n'est pas une
> implémentation séparée. Le hash join (§8.1) reste une primitive distincte : il ne
> suppose pas d'entrées triées, mais matérialise un côté.

> **Piège documenté.** Sur des entrées **non réellement triées**, `MergeJoinBy` produit
> un résultat faux **sans aucune erreur**. Un mode debug doit vérifier la monotonie des
> clés. C'est un échec silencieux, du même registre que le co-partitionnement Kafka
> (§8.1) — inacceptable en l'état.

### 7.3 `Merge` — pourquoi le lot rachète le coût

En pull mono-goroutine, un merge équitable **à l'élément** est impossible sans buffer
ni `iter.Pull`. Au grain du **lot**, il devient trivial : un `iter.Pull` par source, et
on alterne.

**Mesuré** (étape 0) : le merge de deux flux coûte **69,8 ns/élément** en tuple-à-tuple
contre **0,39 ns/élément** par lot de 1024 — un facteur **179**. À 1,3× le plafond,
l'opérateur est essentiellement gratuit en batch, et inutilisable sans lui.

> En tuple-à-tuple, ce coût est rédhibitoire et `Merge` exigerait une inversion de
> contrôle. **C'est le tout-batch qui rend l'opérateur possible**, pas seulement plus
> rapide — mesuré, pas projeté.

**Condition de complétion : paramètre explicite.** Le flux fusionné se termine-t-il
quand *une* source se termine, ou quand *toutes* le font ? Akka en fait un paramètre
(`eagerComplete`, défaut « toutes »). Jamais un choix implicite.

### 7.4 `ZipLongest` primitif, `Zip` dérivé

`Zip` qui s'arrête silencieusement au plus court **masque des bugs**. Rust a jugé
nécessaire d'ajouter `zip_eq`, qui **panique** sur des longueurs inégales — le signe
que la sémantique par défaut est tenue pour dangereuse.

**Décision : `ZipLongest` est la primitive**, `Zip` un filtrage de `Both`. L'utilisateur
qui veut l'arrêt au plus court le demande explicitement.

> **Avantage structurel à documenter.** En push, `Zip` est une bombe mémoire : RxJava a
> un ticket dédié montrant qu'une file non bornée par source est nécessaire, et qu'**un
> seul producteur lent force tous les autres à bufferiser**. Notre modèle pull en est
> immunisé par construction.

### 7.5 Rebatching — les lots ne s'alignent pas

Deux flux produisent des lots de tailles différentes et non alignées. **Personne ne
cherche à les aligner** — ni DuckDB, ni DataFusion, ni Arrow.

Le modèle établi : les opérateurs binaires travaillent sur un **curseur `(lot, offset)`
par entrée**, consomment les lots partiellement, et produisent une sortie de taille
cible.

DataFusion a un opérateur physique dédié à la recomposition, `CoalesceBatchesExec`,
visible dans les plans d'exécution, dont le rôle documenté est de recombiner les petits
lots après un filtre sélectif ou une jointure.

```go
func Coalesce[T any](s Stream[T], targetSize int) Stream[T]
```

> **Règle.** Placer un `Coalesce` après tout opérateur sélectif — `Filter`, inner join,
> `Split`/Partition — sous peine de propager des lots minuscules qui annulent le
> bénéfice de la vectorisation.

**Taille cible : mesurée.** Le plateau commence à 8 éléments et ne se dégrade pas
jusqu'à 1M (§2.3). L'écart avec DuckDB (2048) et DataFusion (8192) est sans effet ici.
Sur un filtre à 1 % de sélectivité, le batch reste **×3,6 plus rapide** que l'élément
même sans `Coalesce`, et sans allocation — `Coalesce` est donc utile pour éviter la
propagation de lots creux sur plusieurs étages, mais **ce n'est pas une urgence**.

### 7.6 Fusion des contextes

Il n'existe pas de `context.Merge` dans la stdlib Go. La proposition
[golang/go#36503](https://github.com/golang/go/issues/36503) a été **fermée**.

*Réserve : le fil de discussion n'a pas pu être lu, le motif exact du rejet n'est donc
pas établi.*

Le point dur est connu : fusionner **annulation et deadline** est bien défini (le plus
tôt gagne), mais fusionner les **valeurs** ne l'est pas — les implémentations tierces
prennent arbitrairement celles d'un seul parent.

**Notre `Batch.Ctx` (§3.1) rend la question sans objet** : chaque lot sortant d'un merge
**conserve le contexte de son lot d'origine**. Rien à fusionner, O(1), sémantiquement
correct.

| Aspect | Règle |
|---|---|
| Contexte de lot | conservé tel quel — jamais fusionné |
| Annulation du pipeline | canal séparé ; union des sources, via `context.AfterFunc` (sans goroutine) |
| Deadline | le minimum |
| Valeurs | **jamais fusionnées** |

### 7.7 Erreurs et diagnostics à la fusion

Rx distingue explicitement deux stratégies, et la distinction est la bonne :

| Stratégie | Comportement |
|---|---|
| `merge` | la première erreur propage immédiatement, les autres sources sont abandonnées |
| `mergeDelayError` | les autres sources vont à leur terme, les erreurs sont agrégées |

**Défaut retenu : `mergeDelayError`.** Nos diagnostics par élément (§4) rendent
l'agrégation naturelle — une source en échec produit des `Diagnostic` de sévérité
`Critical` sans interrompre les autres, ce qui est cohérent avec l'invariant de
terminalité (§4.7).

---

## 8. Connecteurs de base de données

### 8.1 `database/sql` ne convient pas — constat vérifié

- **Aucun batching** : chaque statement est exécuté sérialement, un aller-retour
  réseau chacun.
- `rows.Next()` est **intrinsèquement ligne à ligne**.
- Boxing en `interface{}` par valeur scannée — c'est le vrai coût, plus encore que
  la réflexion.

Incompatible avec le modèle vectorisé. **Le connecteur parle donc le protocole
PostgreSQL v3 directement.**

### 8.2 Budget honnête du 0dep

Le framing est trivial : ~20 types de messages utiles, 1 octet de type + longueur
int32. **Le vrai budget est ailleurs** :

- **SCRAM-SHA-256** (PBKDF2-HMAC-SHA256 + HMAC) — faisable en `crypto/*` stdlib ;
- **les codecs binaires par type** (numeric, timestamptz, arrays) — travail long et
  piégeux.

Budgéter les codecs, pas le parsing.

### 8.3 Le « truc à la LINQ » : `= ANY($1)`

`IN` exige une liste d'expressions scalaires : N valeurs → N placeholders → **un plan
distinct par valeur de N**. Sur des lots de 1 à 1024, c'est jusqu'à 1024 plans pour
une seule requête logique.

`= ANY($1)` prend **un tableau en un seul paramètre** : forme de requête unique quel
que soit N, donc **un seul plan préparé**.

> **Règle.** Le builder produit **une forme canonique par requête logique, jamais
> paramétrée par N.**

| Backend | Stratégie |
|---|---|
| PostgreSQL | `= ANY($1)` — un paramètre tableau, un plan |
| SQL Server | TVP, avec réserves (§8.4) |
| MySQL | `JSON_TABLE` — un placeholder, joignable avec index |
| Sans tableaux | padding à la puissance de 2 (~11 variantes pour N ≤ 1024) |

Le padding duplique la **dernière valeur liée** (`(1,2,3,3)`), pas du texte : la
sécurité est intacte. À proscrire sur les SGBD sans plan cache, où c'est un surcoût net.

### 8.4 Réserve sur les TVP SQL Server

Estimation de cardinalité **10 % pour l'égalité, 30 % pour l'inégalité, 9 % pour un
range**, indépendamment du nombre réel de lignes ; aucune statistique par colonne ; et
sur les **jointures**, parameter sniffing bidirectionnel — un premier plan bâti sur
100k lignes reste jusqu'à recompilation. Mitigations : `OPTION (RECOMPILE)` ou trace
flag 2453.

### 8.5 Pipelining et COPY

**Pipelining** : envoyer Parse/Bind/Execute pour tout le lot, puis **un seul `Sync`**.
En cas d'erreur le backend saute tous les messages jusqu'au Sync suivant — d'où une
**frontière d'erreur naturelle par lot**. Gain documenté côté pgx : 11 allers-retours
réduits à 2.

**COPY binaire** : signature 11 octets, puis par tuple un compteur de champs 16 bits et
par champ une longueur 32 bits (`-1` = NULL), trailer `-1`. Les frontières de messages
**n'ont pas à coïncider avec les lignes** : un `Batch[Row]` mappe sur un `CopyData`.
~200 lignes de code pour le meilleur ratio effort/gain du projet.

### 8.6 Hydratation

L'argument décisif pour le codegen n'est **pas** « `reflect` est lent » : c'est que la
réflexion travaille **ligne par ligne et champ par champ**, ce qui casse la
vectorisation.

- une fonction d'hydratation générée **par entité**, bouclant sur les 1024 lignes en
  interne — une indirection par lot, pas par ligne ;
- **décodage colonnaire** : la boucle sur 1024 valeurs d'un même type est monomorphe et
  prédictible — le principe X100 appliqué à l'hydratation ;
- **zéro `interface{}`** dans le chemin chaud ; format **binaire** obligatoire ;
- pré-allocation à la taille du lot, réutilisation des tampons entre lots.

### 8.7 Collections : jamais de produit cartésien

Deux `Include` de collections **au même niveau** produisent un produit croisé (10 posts
× 10 contributeurs = 100 lignes pour un blog). En moteur batch, on a déjà les clés
parentes : **requêtes séparées + jointure en mémoire**. L'ordre de tri doit être rendu
déterministe par construction.

### 8.8 Sécurité par construction

1. **Les valeurs ne transitent jamais par le texte SQL** — uniquement en Bind. Avec
   `= ANY($1)`, 1024 valeurs restent **un paramètre** : surface d'injection nulle.
2. **Les identifiants viennent d'un catalogue généré** à la compilation — un nom de
   colonne est un *symbole Go*, pas une chaîne.
3. **Builder typé** : un état invalide n'est pas représentable. Aucun `Raw(string)`
   public dans le chemin nominal.

---

## 9. Jointures

### 9.1 Hash join — le build est une table

Le côté build **est** une table, le côté probe pilote la jointure et produit seul les
sorties (dualité stream/table de Kafka Streams). Nommer les deux côtés dans l'API lève
l'ambiguïté sur lequel est matérialisé.

```go
func JoinStreamTable[A, B any, K comparable, R any](
    probe Stream[A],        // streamé — peut être infini
    build Stream[B],        // matérialisé — DOIT être fini
    keyA  func(A) K,
    keyB  func(B) K,
    merge func(A, B) R,
    limit BuildLimit,       // OBLIGATOIRE — paramètre positionnel requis
) Stream[R]
```

**`limit` est requis, pas une option.** Rendre l'état non borné *inexprimable* vaut
mieux que le détecter à l'exécution. Spark **refuse à la planification** un outer join
flux-flux sans watermark ; Kafka a **déprécié** son grace period implicite de 24 h
(KIP-633). Le défaut sûr est zéro tolérance, élargie sur demande.

**Le dépassement est bruyant** : `Diagnostic` de sévérité `Critical` et compteur
observable — jamais un silence, jamais un OOM. Rompre le co-partitionnement dans Kafka
Streams ne lève aucune exception et ne produit aucune sortie : c'est le pire mode de
défaillance, et le contre-exemple à ne pas reproduire.

**La limite est globale au plan**, pas locale à l'opérateur : quand plusieurs jointures
cohabitent, la somme des tables doit tenir. Un budget partagé.

**Le stockage est une interface** — map mémoire par défaut, spill optionnel. Ne jamais
câbler une politique de persistance dans un opérateur : Kafka Streams a fini par imposer
RocksDB dans son foreign-key join, en contradiction avec son propre objectif
d'agnosticisme. Pour le spill, la référence est le partitionnement radix avec
augmentation récursive des bits (DuckDB), avec son garde-fou connu : sur données très
biaisées, le sous-partitionnement récursif provoque du *I/O thrashing*.

### 9.2 Stratégies

| Stratégie | Condition | Mémoire |
|---|---|---|
| Hash join | clés `comparable` | O(build) |
| Merge join (§7.2) | les deux flux triés sur la clé | O(1) |
| Interval join | flux temporels ordonnés | O(débit × intervalle) |
| Nested loop | dernier recours, côté droit rejouable | O(1), CPU O(n·m) |

**Clés de type identique, imposé à la compilation.** Materialize documente que les casts
implicites dans les contraintes de jointure sont très coûteux en mémoire ; le typage
générique rend le problème inexistant — avantage net sur les moteurs SQL.

### 9.3 Flux infinis : interval join

L'état est borné par un **prédicat temporel**, pas par un découpage — une fenêtre
glissante duplique chaque élément dans N fenêtres.

```go
// a se joint à b ssi : a.ts + lower <= b.ts <= a.ts + upper
func IntervalJoin[A, B any, K comparable, R any](
    left, right Stream[A], /* ... */
    lower, upper time.Duration,
) Stream[R]
```

**Restrictions assumées, reprises de Flink : inner join et temps d'événement
uniquement.** Un outer join fenêtré exige de savoir *quand renoncer à trouver un
partenaire*, donc un watermark. Sans lui la sémantique est **indéfinie**, et l'exposer
serait mentir à l'utilisateur.

> Si des watermarks sont introduits : prévoir dès la conception un **idle timeout par
> source**. Une source inactive fige le watermark, les fenêtres ne ferment jamais,
> l'état croît sans fin.

### 9.4 Jointures n-aires

**Ne jamais matérialiser les résultats binaires intermédiaires** — c'est l'amplification
d'état qui contraint Materialize et RisingWave au *delta join*. Un chaînage
`.Join().Join()` **ment sur le coût réel** : soit jointure n-aire, soit coût documenté.

### 9.5 Portée : pas de rétractations

Trois modes d'accumulation existent (Dataflow Model) ; *accumulating & retracting* — la
révision d'un résultat déjà émis — est de loin le plus coûteux. **Ce framework fait du
`discarding` uniquement.** C'est ce qui fait exploser la complexité de Beam, et c'est
hors de portée d'un moteur mono-processus sans état durable.

*Réserve : aucune source ne montre l'équipe Beam qualifiant les rétractations d'erreur
de conception. C'est un coût assumé de leur part, pas un regret documenté — notre choix
est un choix de portée, pas une correction.*

---

## 10. Pièges d'API

### 10.1 Un flux ne se relit pas — il se clone

Un `Stream` consommé deux fois s'exécute deux fois, ou rend zéro élément s'il capture un
`*bufio.Scanner`. **Aucun signal.** Java lève `IllegalStateException` ; .NET a créé une
règle d'analyse dédiée (CA1851). **Go ne fait ni l'un ni l'autre.**

Réponse : pas de `Memoize` implicite qui masquerait le coût, mais `Split` en mode
Broadcast (§6) pour le clonage, `Materialize` **explicite** quand la source n'est pas
rejouable, et une convention de nommage pour les sources à passage unique.

### 10.2 Nom du type central — tranché

La RFC Rust 2996 a renommé `Stream` en `AsyncIterator` parce que « stream » est trop
générique — `io.Reader`, `net.Conn` et les flux réseau sont tous des « streams ».

**Décision : on garde `Stream`.** La collision est moins gênante en Go qu'en Rust,
parce que le paquet qualifie l'usage à la lecture : `sluice.Stream[T]` est sans
ambiguïté là où Rust importait `Stream` dans un espace de noms partagé. `AsyncIterator`
serait de surcroît un contresens — notre modèle est **synchrone** : pas de `Future`,
pas de point de suspension, tout se passe sur la même pile. C'est précisément ce qui
donne les traces d'exécution natives et le `defer` exécuté à l'arrêt anticipé.

### 10.3 Chaînage : wrapper retenu

Go n'a pas de méthodes génériques. Deux options :

```go
Map(Filter(s, pred), f)        // imbriqué : lecture à l'envers
s.Filter(pred).Map(f)          // chaîné : exige un wrapper struct
```

**Décision : wrapper.** Le tout-batch réduit le nombre d'opérateurs à écrire, ce qui
rend le coût du wrapper plus facile à absorber.

> **Leçon transversale (conduit, pipes, Node.js).** L'élégance théorique ne compense
> jamais une API non familière, et la dette d'API s'accumule vite. **Geler tôt le
> noyau** (`Stream`, `Batch`, `Diagnostic`), garder le reste en opérateurs
> remplaçables. Et **jamais de mode implicite** : un stream dont le comportement change
> selon qu'un consommateur s'est abonné a coûté trois versions majeures à Node.js.

---

## 11. Sécurité — propriétés vérifiables

| # | Garantie | Vérification |
|---|---|---|
| S1 | Aucune allocation non bornée. Toute borne est un paramètre requis. | Dépassement → erreur, pas OOM |
| S2 | Aucun `panic` ne traverse une frontière publique. | Fuzzing des parseurs |
| S3 | Limites par défaut restrictives. | Revue des défauts |
| S4 | `unsafe` interdit sauf audit nommé. | `go vet` + revue |
| S5 | Timeouts obligatoires sur toute I/O. | Connexion muette → libérée |
| S6 | Pas de fuite de goroutine. | Détecteur maison |
| S7 | Les erreurs internes ne fuient pas vers le client. | **Garanti par la structure** : `cause` non exporté (§4.3) |
| S8 | Toute entrée réseau est bornée avant allocation. | Fuzzing |
| S9 | Tout `iter.Pull` interne appelle `stop()` sur tous les chemins. | Test avec panic injectée |
| S10 | Tout opérateur propage `false` et laisse la génératrice se dérouler. | Test de finalisation prompte |
| S11 | Aucun buffer non borné entre deux opérateurs. | Revue + test de pression |
| **S12** | **Plafond de diagnostics par élément et par lot.** | Lot pathologique → troncature comptée |
| **S13** | **Aucune valeur ne transite par le texte SQL.** | Revue du builder + fuzzing |

**S9** — si la séquence n'est pas épuisée et que `stop()` n'est pas appelé, la coroutine
ne se termine **jamais**.

**S10** — c'est le problème n°1 du streaming, avant la performance. L'auteur de `conduit`
a documenté son propre échec : avec un `take 4`, le handle de fichier **restait ouvert**
pendant tout le reste du pipeline. `iter.Seq` gère bien ce cas — un `defer f.Close()`
s'exécute dès l'arrêt anticipé — **mais uniquement si tous les opérateurs propagent
honnêtement le `false`**. Invariant testé, pas convention.

**S11** — avant la 1.5, Flink reposait sur TCP pour sa contre-pression : un consommateur
lent bloquait *toutes* les connexions logiques du multiplex. Notre `yield` **est** le
crédit ; tout buffer non borné réintroduit ce bug.

### 0dep — la règle exacte

- **Production : zéro dépendance.** Stdlib uniquement, sans exception.
- **Tests/outils : autorisés** s'ils ne peuvent pas remonter dans le binaire final.

---

## 12. Architecture applicative

Le framework n'impose aucune structure de projet. Un `Stream` est une fonction : rien à
injecter, aucun registre global, aucun runtime implicite (mais un contexte stratifié,
cf. §3.1).

- **DDD / Clean** — le domaine manipule `Stream[Entity]` sans importer le package web ;
  les adaptateurs vivent en périphérie.
- **Monolithe** — composition en mémoire, sans sérialisation.
- **Microservices** — la frontière réseau est un opérateur de plus ; le pipeline logique
  est identique, seul le transport change.

---

## 13. Plan d'implémentation

| Étape | Contenu | Valide |
|---|---|---|
| 0 | ✅ **Fait** — [BENCHMARK-ETAPE-0.md](BENCHMARK-ETAPE-0.md) : plafond 0,31 ns, batch ×1,9, `iter.Pull` 68,6 ns | §2.4 — le dénominateur |
| 1 | `Stream`, `Batch`, `Diagnostic`, `Path`, `Source`, opérateurs O(1), terminaux | Le cœur |
| 2 | `Split` unique + `Parallel` + opérateurs O(k) + tests S9/S10/S11 | §6, S1, S6 |
| 3 | Opérateurs N→1 : `Concat`, `Merge`, `MergeJoinBy`, `ZipLongest`, `Coalesce` | §7 |
| 4 | `Join` (hash borné, interval) — le merge join dérive de §7.2 | §9 |
| 5 | Vertical HTTP de bout en bout, diagnostics compris | §1.1 |
| 6 | Connecteur PostgreSQL : protocole v3, `= ANY`, COPY, hydratation générée | §8 |
| 7 | Pushdown dynamique (`Demand` + génération atomique) | §3.4 |

**L'étape 0 est faite.** Le tout-batch est validé (×1,9 à ×2,0 dès deux étages), le
coût de `iter.Pull` corrigé (68,6 ns et non 20 ns), et le budget d'un opérateur du
noyau établi : **~1,5 ns par étage et par élément**. Tout nouvel opérateur se mesure
contre ce budget.

**L'étape 7 est volontairement tardive** : le pushdown est la piste à plus fort levier
(20×+), mais il suppose des opérateurs stabilisés pour avoir un sens.

---

## 14. Journal des révisions v0.2 → v0.3

| # | Changement | Origine |
|---|---|---|
| 1 | **Tout-batch : `Stream[T] = iter.Seq[Batch[T]]`, forme unique** | Relecture — « si une méthode traite N éléments, elle en traite 1 » |
| 2 | **§1 : tout est opération de stream**, exemple PATCH complet | Relecture — le point n'était pas dit |
| 3 | **§1.2 : le connecteur DB suit la même forme** ; connecteurs à réimplémenter | Relecture |
| 4 | **§2.2 : l'ambiguïté du signal d'arrêt disparaît** avec le lot | Relecture — cas `Stream[bool]` |
| 5 | **§3.1 : correction — le contexte circule bien**, stratifié, porté par le lot | Relecture — §6 v0.2 mal écrite |
| 6 | **§3.2 : quatre causes d'arrêt distinguées**, patron `context` | Relecture (arrêt interne) + recherche |
| 7 | **§4 : modèle `Diagnostic` complet** | FHIR OperationOutcome + SARIF 2.1.0 |
| 8 | **§4.4 : affordances jointes au même `Path`** | Trou identifié en relecture — sans précédent |
| 9 | **§6 : `Split` unique à trois modes** | Relecture + Akka Streams |
| 10 | **§6.3 : lock-step mono-goroutine → buffer O(1)** | La 5e option, non identifiée avant |
| 11 | **§8.3 : `= ANY($1)`** — une forme canonique, un plan | Relecture (« truc à la LINQ ») |
| 12 | **§8.1 : `database/sql` écarté** — pas de batching possible | Recherche |
| 13 | **§3.4 : pushdown dynamique** (sideways information passing) | Recherche — gains 22×/25× |
| 14 | **§10.1 : un flux se clone, ne se relit pas** ; `Memoize` implicite retiré | Relecture |
| 15 | **§10.3 : wrapper retenu pour le chaînage** | Relecture |
| 16 | **S12, S13 ajoutés** | §4.5, §8.8 |
| 17 | **§7 : famille N→1 complète** (Concat, Merge, Interleave, ZipLongest) | Relecture — « 2 flux, comment n'en faire qu'un » |
| 18 | **§7.2 : `MergeJoinBy` primitive centrale** — 6 sémantiques par filtrage | Rust `itertools::merge_join_by` |
| 19 | **`Union`/`Intersect`/`Except` non triés écartés** — HashSet non borné, viole S1 | Sort-merge des moteurs SQL |
| 20 | **§7.5 : `Coalesce`** — les lots ne s'alignent pas, on recompose | DataFusion `CoalesceBatchesExec` |
| 21 | **§7.6 : contextes jamais fusionnés**, chaque lot garde le sien | `context.Merge` rejeté en Go |
| 22 | **§7.7 : `mergeDelayError` par défaut** | Rx `merge` vs `mergeDelayError` |

### Ce qui n'a pas changé

Le choix **pull-based** reste renforcé : Kersten et al. établissent que pull/push et
granularité sont orthogonaux et qu'aucune direction ne domine ; DuckDB est passé au push
pour des raisons d'architecture, **sans invoquer la performance du pull** ; et le README
de Timely Dataflow propose comme correctif à sa sortie push non bornée… de faire
retourner un itérateur par les opérateurs.

---

## Annexe — sources

**Moteurs de requêtes**
[MonetDB/X100 (CIDR 2005)](https://www.cidrdb.org/cidr2005/papers/P19.pdf) ·
[Kersten et al., PVLDB 11(13), 2018](https://www.vldb.org/pvldb/vol11/p2209-kersten.pdf) ·
[Neumann, PVLDB 4(9), 2011](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf) ·
[Saving Private Hash Join, PVLDB 18](https://www.vldb.org/pvldb/vol18/p2748-kuiper.pdf) ·
[Graefe, Volcano, IEEE TKDE 6(1), 1994](https://dl.acm.org/doi/10.1109/69.273032) ·
[DuckDB — push-based execution](https://github.com/duckdb/duckdb/issues/1583) ·
[DataFusion — Dynamic Filters](https://datafusion.apache.org/blog/2025/09/10/dynamic-filters/) ·
[Sideways Information Passing, ICDE 2008](https://dl.acm.org/doi/10.1109/ICDE.2008.4497486)

**Moteurs de flux**
[Flink — Network Stack](https://flink.apache.org/2019/06/05/a-deep-dive-into-flinks-network-stack/) ·
[Flink — Joining](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/joining/) ·
[Kafka Streams — Core Concepts](https://kafka.apache.org/42/streams/core-concepts/) ·
[The Dataflow Model](https://research.google/pubs/the-dataflow-model-a-practical-approach-to-balancing-correctness-latency-and-cost-in-massive-scale-unbounded-out-of-order-data-processing/) ·
[Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow/blob/master/README.md) ·
[Materialize — Four Thoughts](https://materialize.com/blog/four-thoughts-four-years-materialize/) ·
[Akka Streams — Graphs](https://doc.akka.io/libraries/akka-core/current/stream/stream-graphs.html)

**Itérateurs, split, annulation**
[Go Blog — Range Over Function Types](https://go.dev/blog/range-functions) ·
[Go Blog — Pipelines](https://go.dev/blog/pipelines) ·
[Russ Cox — Coroutines for Go](https://research.swtch.com/coro) ·
[pkg.go.dev/iter](https://pkg.go.dev/iter) ·
[Sinclair Target — Error Handling with Iterators](https://sinclairtarget.com/blog/2025/07/error-handling-with-iterators-in-go/) ·
[Snoyman — The core flaw of pipes and conduit](https://www.yesodweb.com/blog/2013/10/core-flaw-pipes-conduit) ·
[Denicola — On the Streams Standard](https://domenic.me/streams-standard/) ·
[Rust RFC 2996](https://rust-lang.github.io/rfcs/2996-async-iterator.html) ·
[WHATWG Streams — tee](https://streams.spec.whatwg.org/#rs-tee) ·
[MDN — ReadableStream.tee()](https://developer.mozilla.org/en-US/docs/Web/API/ReadableStream/tee) ·
[Python itertools.tee](https://docs.python.org/3/library/itertools.html#itertools.tee) ·
[CA1851 — Multiple enumerations](https://learn.microsoft.com/en-us/dotnet/fundamentals/code-analysis/quality-rules/ca1851)

**Diagnostics**
[FHIR R4 OperationOutcome](https://hl7.org/fhir/R4/operationoutcome.html) ·
[SARIF 2.1.0 (OASIS)](https://docs.oasis-open.org/sarif/sarif/v2.1.0/errata01/os/sarif-v2.1.0-errata01-os-complete.html) ·
[RFC 9457 — Problem Details](https://www.rfc-editor.org/rfc/rfc9457.html) ·
[RFC 6901 — JSON Pointer](https://www.rfc-editor.org/rfc/rfc6901.html) ·
[JSON:API](https://jsonapi.org/format/) ·
[ValidationProblemDetails](https://learn.microsoft.com/en-us/dotnet/api/microsoft.aspnetcore.mvc.validationproblemdetails?view=aspnetcore-9.0) ·
[Siren](https://github.com/kevinswiber/siren) ·
[ICU MessageFormat](https://unicode-org.github.io/icu/userguide/format_parse/messages/)

**Bases de données**
[PG protocol-flow](https://www.postgresql.org/docs/current/protocol-flow.html) ·
[PG message-formats](https://www.postgresql.org/docs/current/protocol-message-formats.html) ·
[PG COPY](https://www.postgresql.org/docs/current/sql-copy.html) ·
[Crunchy — ANY vs IN](https://www.crunchydata.com/blog/postgres-query-boost-using-any-instead-of-in) ·
[Mihalcea — parameter padding](https://vladmihalcea.com/improve-statement-caching-efficiency-in-clause-parameter-padding/) ·
[Brent Ozar — TVP sniffing](https://www.brentozar.com/archive/2018/03/table-valued-parameters-unexpected-parameter-sniffing/) ·
[EF Core — split queries](https://learn.microsoft.com/en-us/ef/core/querying/single-split-queries) ·
[go-database-sql — surprises](http://go-database-sql.org/surprises.html) ·
[pgx](https://github.com/jackc/pgx) · [sqlc](https://docs.sqlc.dev/en/latest/) ·
[fasthttp](https://github.com/valyala/fasthttp)
