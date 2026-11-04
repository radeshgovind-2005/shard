# Functions: the Stepdown Rule

## Contents
1. The idea
2. Before / after
3. Extracting branch and loop bodies
4. Command–query separation
5. Arguments
6. Error handling is "one thing"
7. Extension functions as reusable verbs

## 1. The idea

Read a file top to bottom like a newspaper:
headline → summary → details.
Each function is a "TO paragraph": *to place an order, we validate the cart, calculate the total, charge the payment, and save the order.*

## 2. Before / after

**Before** — one function, four levels of abstraction mixed:

```kotlin
fun placeOrder(cart: Cart, user: User): Order {
    if (cart.items.isEmpty()) throw IllegalStateException("empty")
    for (item in cart.items) {
        if (item.quantity > inventory.stockOf(item.productId)) throw IllegalStateException("no stock")
    }
    var total = 0.0
    for (item in cart.items) total += item.price * item.quantity
    if (user.isPremium) total *= 0.9
    val result = gateway.charge(user.cardToken, total)
    if (!result.ok) throw IllegalStateException("payment failed")
    val order = Order(UUID.randomUUID().toString(), user.id, cart.items, total)
    repository.save(order)
    return order
}
```

**After** — the story on top, details below:

```kotlin
fun placeOrder(cart: Cart, user: User): OrderResult {
    val validCart = validate(cart) ?: return OrderResult.InvalidCart
    val total = totalFor(validCart, user)
    val payment = charge(user, total)
    return finishOrder(payment, validCart, user, total)
}

private fun validate(cart: Cart): ValidCart? =
    cart.takeIf { it.isNotEmpty() && it.allInStock() }?.let(::ValidCart)

private fun Cart.allInStock(): Boolean = items.all(::isInStock)

private fun isInStock(item: CartItem): Boolean =
    item.quantity <= inventory.stockOf(item.productId)

private fun totalFor(cart: ValidCart, user: User): Money =
    cart.subtotal().withDiscountFor(user)

private fun finishOrder(payment: Payment, cart: ValidCart, user: User, total: Money): OrderResult =
    when (payment) {
        is Payment.Approved -> OrderResult.Placed(saveOrder(cart, user, total))
        is Payment.Declined -> OrderResult.PaymentDeclined(payment.reason)
    }
```

Notice:
- `placeOrder` has 4 lines, all at the same level.
- Every name answers "what", the body answers "how".
- Ctrl+click on any name goes exactly one level deeper.

## 3. Extracting branch and loop bodies

Bodies of `if`, `when` branches and loops should be one call or one expression.

```kotlin
// Before
users.forEach { user ->
    if (user.lastLogin < cutoff && !user.isAdmin) {
        user.status = Status.Inactive
        mailer.send(user.email, "We miss you")
    }
}

// After
users.filter(::isDormant).forEach(::deactivate)

private fun isDormant(user: User) = user.lastLogin < cutoff && !user.isAdmin

private fun deactivate(user: User) {
    markInactive(user)
    sendComeBackEmail(user)
}
```

Prefer collection operations (`filter`, `map`, `sumOf`, `groupBy`, `any`, `all`) with named function references over manual loops with mutable variables.

## 4. Command–query separation

A function either **does** something (command, returns `Unit`) or **answers** something (query, returns a value, no side effects). Not both.

```kotlin
// Bad: answers AND mutates
fun checkAndReserve(item: Item): Boolean

// Good
fun canReserve(item: Item): Boolean
fun reserve(item: Item)
```

## 5. Arguments

- 0–2 arguments ideal, 3 acceptable, 4+ → group them into a `data class`.
- No Boolean flag arguments (`render(true)`). Split into two functions or use a sealed/enum type.
- Prefer receivers (`cart.subtotal()`) when the function is "about" one object.

## 6. Error handling is "one thing"

A function that handles errors should do only that. Prefer typed results over exceptions for expected failures.

```kotlin
sealed interface LoadProfile {
    data class Loaded(val profile: Profile) : LoadProfile
    data object NotFound : LoadProfile
    data class Failed(val cause: Throwable) : LoadProfile
}

fun loadProfile(id: UserId): LoadProfile =
    runCatching { api.fetch(id) }.fold(::toLoadResult, LoadProfile::Failed)
```

Use exceptions for programmer errors (`require`, `check`, `error`), sealed results for business outcomes.

## 7. Extension functions as reusable verbs

Extensions turn small helpers into readable English and make them reusable everywhere.

```kotlin
fun List<CartItem>.subtotal(): Money = sumOf { it.lineTotal().cents }.let(::Money)
fun CartItem.lineTotal(): Money = price * quantity
fun Money.withDiscountFor(user: User): Money = if (user.isPremium) this * 0.9 else this
```

Rules for extensions:
- Put them in the file of the type they extend, or in a `<Type>Extensions.kt` file next to it.
- Keep them `private` or `internal` unless genuinely shared.
- Don't extend `String`/`Int` with domain logic — create a `value class` instead.
