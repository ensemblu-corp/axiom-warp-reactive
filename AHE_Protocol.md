# The AHE Protocol (Axiom-Hostile-Environment)

## Philosophy
The **AHE Protocol** defines the strict architectural boundary between the Axiom Core—our domain of functional, immutable, and performant data structures—and the Vert.x Runtime—our high-performance, non-blocking I/O host.

In Axiom, we treat the `io.vertx` namespace as the **"Untrusted Zone."** It is fast but structurally flawed, relying on mutable collections, imperative loops, and memory-heavy allocations that contradict our sovereign design.

## Core Mandates

### 1. The "Drain" Protocol
Data must never linger in Vert.x containers. The moment a `Future`, `CompositeFuture`, or `Message` resolves, the data must be **drained** immediately into an Axiom-native structure (`PersistentList`, `PersistentMap`, or `Result`).
* **Rule:** Never expose `Future<List<...>>` or `Future<Map<...>>` to business logic.
* **Mechanism:** Use functional folds (`fold`) to drain Vert.x collections into `Transient` Axiom builders, then `freeze()`.

### 2. Zero-Allocation Bridge
When the interface demands a bridge to legacy types (e.g., `CompositeFuture.all(List)`), avoid `java.util.ArrayList` at all costs.
* **Rule:** Use lightweight, zero-copy `AbstractList` views or fixed-size array wrappers. 
* **Rule:** Never perform heavy iteration inside bridge methods.

### 3. Perimeter Containment
Business logic and state transformation must occur **strictly** within the Axiom domain.
* **Rule:** Vert.x handlers, lambdas, and callbacks are "Hostile Zones." They exist only for transport.
* **Rule:** Wrap all external resources (e.g., `SqlConnection`, `Transaction`) in Axiom-governed interfaces (e.g., `Async`, `SovereignGate`) that handle resource lifecycle management implicitly.

### 4. Immutable Sovereignty
Axiom structures (`PersistentList`, `PersistentMap`) must be treated as the ultimate source of truth.
* **Rule:** If Vert.x requires data, pass a `frozen` snapshot. 
* **Rule:** Any data returned by Vert.x is assumed "unsafe" until validated and converted to an Axiom type.

## Architectural Goal
By enforcing these protocols, we create a **Sandbox**. Inside the sandbox, the system is pure, functional, and highly optimized. Outside the sandbox is the "Wilderness" of low-level I/O. The `reactive` jar acts as the containment vessel, preventing the "awful implementation" patterns of the underlying runtime from polluting our business domain.