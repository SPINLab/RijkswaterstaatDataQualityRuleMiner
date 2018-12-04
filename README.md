# Probabilistic Knowledge Graph Clause 

A Probabilistic Knowledge Graph Clause (PKGC) specifies a Horn-like clause $u \leftarrow q, r, s$ which has a probability
$p$ of holding for all entities $e \in E$ of type $t$ in a knowledge graph $KG = (R, P, A)$.

A PKGC $\phi = [p] head \leftarrow body$
* $head := right-bound-assertion $
* $body := \{ typE(u, t) \} [\cup assertion|left-bound-assertion ]$
* $assertion := p(e, r) \in KG$
* $left-bound assertion := p(e, \sigma(t))$
* $right-bound assertion := p(\sigma(t), r)$
* $\sigma(t) := e \in E: type(e, t)$
* $p := probability$

Here,
* $R := \{E,L\}$ is the set of resources in knowledge graph $KG$
	- $E = \{e_0, e_1, \ldots, e_n\}$ is the set of all entities in $R$
	- $L = \{l_0, l_1, \ldots, l_n\}$ is the set of all literals in $R$
* $P = \{p_0, p_1, \ldots, p_n\}$ is the set of all predicates in knowledge graph $KG$
* $A = \{a_0, a_1, \ldots, a_n\}$ is the set of assertions in knowledge graph $KG$
	- $a_k := p_q(e_i,r_j)$ with $p_q \in P \land e_i \in E \land r_j \in R$ states that entity $e_i$ has resource $r_j$ for its property $p_q$
* $t :=$ a type class, such that $\exists type(e, t) \in A \land e \in E$

Example:
* $\phi_1 = \{ (p_a, \{e_i, e_j, e_k\}), (p_b, \{l_m,l_n\})\} \leftarrow \{ (type, \{t\}) \}$
	- A CGFD of depth $0$ specifies _all_ likely predicates and predicate-values for all entities of the given type
	- $\forall e \in E: type(e,\mathrm{T}) \rightarrow (\neg p_a(e, u) \lor \exists p_a(e, u) \land u \in \{e_i, e_j,  e_k\} \land u \neq e) \land (\neg p_b(e,l) \lor \exists p_b(e,l) \land l \in \{l_m, l_n\} \land l \neq e)$
* $\phi_2 = \{ (p_a, \{e_i, e_j\}), (p_b, \{l_m\})\} \leftarrow \{ (type, \{t\}), (p_c, \{e_k\}) \}$
	- A CGFD of depth $1$ specifies the likely predicates and predicate-values for all entities of the given type given the constraints which directly apply to those entities
	- $\forall e \in E: type(e,\mathrm{T}) \rightarrow (\exists p_c(e, u) \land u \in \{e_k\} \land u \neq e \rightarrow (\neg p_a(e, v) \lor \exists p_a(e, v) \land v \in \{e_i, e_j\} \land v \neq e) \land (\neg p_b(e,l) \lor \exists p_b(e,l) \land l \in \{l_m\} \land l \neq e))$
* $\phi_3 = \{ (p_a, \{e_i, e_j\})\} \leftarrow \{(type, \{t\}), (p_b, \{e_k\}), (e_k.p_c, \{l_m, l_n\})\}$
	- A CGFD of depth $>1$ specifies the likely predicates and predicate-values for all entities of the given type given the constraints which directly apply to those entities _as well as_ any given constraints which indirectly apply to those entities via the paths specified in the constraints
	- $\forall e \in E: type(e,t) \rightarrow (\exists p_b(e, u) \land u \in \{e_k\} \land u \neq e \land p_c(u, l) \land l \in \{l_m, l_n\} \land l \neq e \rightarrow (\neg p_a(e, w) \lor \exists p_a(e, w) \land w \in \{e_i, e_j\} \land w \neq e))$

## Algorithm 

To compute all CGFDs up to and including depth $d^+$ with a support of $\theta \geq \tau_\theta$, we
1. initialize the generation tree by computing all top-level (i.e., $d=0$) CGFDs with $\theta \geq \tau_\theta$ by
	a) selecting all type classes $t$ with more than $\tau_\theta$ members
	b) computing all common predicate-object pairs for the members of $t$
	c) generating a top-level CGFD $\phi$ for each type $t$ containing the common predicate-resource pairs
2. explore all depth $d$ pendant incidents $p(u, v) \in \phi.body$ as candidate endpoints for extension by
	a) vertically exploring all non-duplicate combinations of $\phi$'s depth $d$ pendant predicates $\{p_0, p_1, \ldots, p_n\}$
	b) horizontally exploring all non-duplicate combinations of $\phi$'s depth $d$ pendant entities $\{v_0, v_1, \ldots, v_n\}$
3. extend all depth $d$ pendant entities $v$ with a depth $d+1$ candidate extension predicate-resource pair by
	a) vertically extending $v$ with all non-duplicate combinations of candidate extension predicates $\{q_0, q_1, \ldots, q_n\}$
	b) horizontally extending $q(v, \_)$ with all non-duplicate combinations of candidate extension resources $\{r_0, r_1, \ldots, r_n\}$
4. recursively repeat step 2 and 3 up to and including depth $d^+$ for all CGFDs of depth $d < d^+$ with a support $\theta \geq \tau_\theta$

## Pseudo Code 

Generation forest $\Omega(t, d)$:: a forest in which each tree is rooted at a top-level ($d=0$) CGFD of type $t$, and in which the vertices on depth $d$ hold the $t$-type CGFDs of that depth
- The set of vertices (CGFDs) on depth $d$ of the $t$-type tree is denotes by $\Omega(t, d)$

### Main loop 
```
-input: knowledge graph KG = (R, P, A), max depth d⁺, support threshold τ
-output: generation forest Ω with all CGFDs up to and including depth = d⁺

Ω = initialize_generation_forest(KG, τ)

d = 0
while d < d⁺:
	for t ∈ Ω.types:
		patterns = Ø
		for φ ∈ Ω(t, d):
			pendant_incidents_set = {p(u, v) ∈ φ.body: Δ(_, p(u, v)) = d ∧ OTP(p)}
			patterns U explore(KG, Ω, φ, pendant_incidents_set)
		Ω(t, d+1) = patterns
	d = d + 1
	
return Ω
```

OTP:: Object-type property, i.e, $p(e, u) \rightarrow \forall u \in E $
$\Delta(\_, p(u, v))$:: the minimum number of hops between the pivot entity $\_$ and entity $v$

### Generation forest initialization 
```
-input: knowledge graph KG = (R, P, A), support threshold τ
-output: generation forest Ω with all CGFDs of depth = 0

Ω = Ø 
types = {u ∈ E: ∃e in E ∧ type(e, u)}
for t in types:
	E⁻ = {e ∈ E: type(e, t)}
	if |E⁻| < τ:
		continue
		
	head = Ø
	for p ∈ P:
		S = [u: p(e, u) ∧ e ∈ E⁻]  // list
		p_freq = |S|
		if p_freq < τ:
			continue

		for u ∈ {u: u ∈ S}:
			freq = |[u ∈ S]|
			p = freq / p_freq
			
			head = p(_, u)
			body = { self(_, _) }
			Ω(t, 0) U {p:head ← body}
			
		T = [t: type(u, t) ∧ u ∈ S]  // list
		for t' ∈ {t: t ∈ T}:
			freq = |[t' ∈ T]|
			p = freq / p_freq
			
			head = p(_, σ(t'))  // σ(t') := variable of type
			body = { self(_, _) }
			Ω(t, 0) U {p:head ← body}
			
return Ω
```

(self, members):: a reflexive property for all members of a type $t$ which allows the extension from top-level ($d=0$) CGFDs to depth $d=1$
parent is empty clause: T ← Ø

### Explore
```
-input: knowledge graph KG = (R, P, A), generation forest Ω, CGFD φ, pendant incidents set I
-output: a set of extended CGFDs

patterns = Ø
while I ≠ Ø:
	p(u, v) = I.pop()
	
	candidate_extensions = {χ.head: χ ∈ Ω(t, 0) ∧ type(v, t)}
	extensions = extend(KG, φ, p(u, v), candidate_extensions)
	patterns U extensions
	
	for ψ ∈ extensions:
		patterns U explore(KG, Ω, ψ, I)
		
return patterns
```

candidate_extensions can be empty if facts with type t subjects do not exceed τ

### Extend 
```
-input: knowledge graph KG = (R, P, A), CGFD φ, pendant incident p(u, v), candidate extensions set ℂ
-output: a set of extended CGFDs

patterns = Ø 
while ℂ ≠ Ø:
	q(v', w) = ℂ.pop()
	
	θ = support(KG, φ, q(v', w))
	if θ ≥ τ:
		head = φ.head.copy()
		body = φ.body.copy() U { q(v', w) }
		ψ = head ← body

		ψ.support = support_of(KG, φ, ψ)
		ψ.confidence = confidence_of(KG, ψ)
		
		if 0 < ψ.support < φ.support:
			patterns U { ψ }
			patterns U extend(KG, ψ, p(u, v), ℂ.copy())
	
return patterns
```

### Support Update 
```
-input knowledge graph KG, parent φ, self ψ
-output pattern support

E⁻ = {e ∈ E: e ⊨ φ.body}
for e ∈ E⁻:
	if e ⊭ ψ.body:
		rmv e from E⁻

return |E⁻|
```

### Confidence Update
```
-input knowledge graph KG, self ψ
-output pattern confidence

E⁻ = {e ∈ E: e ⊨ ψ.body}
for e ∈ E⁻:
	if e ⊭ ψ.head:
		rmv e from E⁻
		
return |E⁻|
```

Each φ keeps track of the entities e of type t for which φ.body (e ⊨ φ.body) and for which also φ.head (e ⊨ φ) holds
