# Types: make the compiler enforce the rules

## Contents
1. Choosing the right construct
2. Value classes for IDs and units
3. Data classes for records
4. Sealed interfaces for states and results
5. Parse, don't validate
6. Interfaces at boundaries
7. Anti-patterns

## 1. Choosing the right construct

| You have… | Use |
|---|---|
| An ID, unit, or constrained primitive (email, money, percent) | `@JvmInline value class` |
| A record of related values | `data class` (immutable `val`s) |
| A fixed set of states / outcomes with different data | `sealed interface` + `data class` / `data object` |
| A fixed set of plain labels with no data | `enum class` |
| A capability something provides (repo, clock, gateway) | `interface` (or `fun interface` for one method) |
| A single stateless thing | `object` |

## 2. Value classes for IDs and units

```kotlin
@JvmInline value class UserId(val raw: String)
@JvmInline value class OrderId(val raw: String)

@JvmInline value class Money(val cents: Long) {
    operator fun plus(other: Money) = Money(cents + other.cents)
    operator fun times(qty: Int) = Money(cents * qty)
}

@JvmInline value class Percent(val value: Int) {
    init { require(value in 0..100) { "Percent out of range: $value" } }
}
```

Now `fun cancel(order: OrderId)` can never receive a `UserId` by mistake.

## 3. Data classes for records

```kotlin
data class CartItem(val productId: ProductId, val price: Money, val quantity: Int)
data class Cart(val items: List<CartItem>)
```

- All properties `val`. Change with `.copy(...)`.
- Read-only collections (`List`, not `MutableList`) in public APIs.
- Put small derived queries on the class (`fun isEmpty()`), not big workflows.

## 4. Sealed interfaces for states and results

Replace Boolean flags and nullable combos:

```kotlin
// Bad: 4 combos possible, only 2 valid
data class Session(val isLoggedIn: Boolean, val user: User?, val token: String?)

// Good: only valid states exist
sealed interface Session {
    data object SignedOut : Session
    data class SignedIn(val user: User, val token: AccessToken) : Session
}
```

Always use exhaustive `when` without `else`, so adding a new state forces every caller to handle it:

```kotlin
fun headerFor(session: Session): String = when (session) {
    Session.SignedOut -> "Welcome"
    is Session.SignedIn -> "Hi, ${session.user.name}"
}
```

UI state follows the same pattern:

```kotlin
sealed interface ScreenState<out T> {
    data object Loading : ScreenState<Nothing>
    data class Ready<T>(val data: T) : ScreenState<T>
    data class Error(val message: String) : ScreenState<Nothing>
}
```

Type-directed transitions — functions that only accept valid source states:

```kotlin
fun pay(order: Order.Pending, payment: PaymentId): Order.Paid = Order.Paid(order.id, payment)
```

## 5. Parse, don't validate

Convert raw input into a valid type once, at the edge. Inside, functions take the valid type and never re-check.

```kotlin
@JvmInline value class Email private constructor(val raw: String) {
    companion object {
        fun parse(input: String): Email? =
            input.trim().takeIf(::looksLikeEmail)?.let(::Email)

        private fun looksLikeEmail(s: String) = "@" in s && "." in s.substringAfter("@")
    }
}

fun sendWelcome(to: Email) { /* no validation needed here, ever */ }
```

For richer errors, return a sealed result instead of `null`:

```kotlin
sealed interface EmailParse {
    data class Valid(val email: Email) : EmailParse
    data object Blank : EmailParse
    data object InvalidFormat : EmailParse
}
```

## 6. Interfaces at boundaries

Anything that touches the outside world (network, DB, clock, files, random) hides behind an interface and is injected through the constructor.

```kotlin
fun interface Clock { fun now(): Instant }

interface OrderRepository {
    fun save(order: Order)
    fun find(id: OrderId): Order?
}

class PlaceOrder(
    private val repository: OrderRepository,
    private val gateway: PaymentGateway,
    private val clock: Clock,
) { /* ... */ }
```

Benefits: core logic stays pure, tests use fakes, files stay small.
Return interface types from public functions; keep implementations `internal` when possible.

## 7. Anti-patterns

- `String`/`Long` IDs passed everywhere ("primitive obsession")
- `isX` + `isY` Boolean pairs that encode a state machine
- `var` properties on domain models
- `when (x) { ... else -> }` over a sealed type
- `!!` to silence the compiler instead of modeling the absence
- God interfaces with 15 methods — split by capability (interface segregation)
- Inheritance trees for code reuse — prefer composition and delegation (`by`)
