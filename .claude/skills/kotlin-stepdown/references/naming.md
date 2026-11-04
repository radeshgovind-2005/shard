# Naming: names replace comments

## Functions are verbs

| Kind | Pattern | Examples |
|---|---|---|
| Command (does) | verb + object | `saveOrder`, `sendReceipt`, `deactivate` |
| Query (answers) | noun or `get`-less | `subtotal()`, `totalFor(cart)`, `activeUsers()` |
| Builder | `build…`, `create…`, `to…` | `buildInvoice`, `toDto()` |
| Counter | `count…` | `countUnread()` |
| Predicate | `is…`, `has…`, `can…`, `should…` | `isExpired`, `hasStock`, `canRefund` |
| Parser | `parse…` returning nullable or sealed | `Email.parse(raw)` |

## Types are nouns from the domain

- Use the words the business uses (`Invoice`, not `BillingDataObject`).
- No `Manager`, `Helper`, `Processor`, `Data`, `Info` suffixes unless they truly add meaning.
- Sealed subtypes describe the state: `Pending`, `Paid`, `Shipped` — not `Type1`, `Success2`.

## Length matches scope

- Wide scope (public API) → descriptive name: `calculateShippingCost`.
- Tiny scope (a lambda) → short is fine: `items.map { it.price }`.

## The comment test

If you want to write a comment explaining *what* code does, extract it into a function whose name is that comment.

```kotlin
// Before
// check if user can get free shipping
if (cart.subtotal() > Money(5000) && user.country == Country.PT) { ... }

// After
if (qualifiesForFreeShipping(cart, user)) { ... }
```

Comments are still welcome for *why* (business reasons, workarounds, links to tickets).

## Avoid

- Abbreviations (`calcTot`, `usr`, `mgr`)
- Encoding types in names (`userList`, `strName`) — the type already says it
- Negative booleans (`isNotValid`) — use `isValid` and `!`
- Vague verbs (`handle`, `process`, `doStuff`, `manage`) — say what actually happens
