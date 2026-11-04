# Files and packages: short, one concept each

## Rules

- One public top-level type per file, named after it (`PlaceOrder.kt`).
- Its private helpers and small private extensions may live in the same file.
- A sealed hierarchy and its subtypes stay together in one file.
- Target < 150 lines. Past ~250 lines, split.
- Order inside a file follows the Stepdown Rule: public entry point first, private helpers below in the order they're called.

## Package by feature, not by layer

```
// Prefer
order/
  PlaceOrder.kt           ← use case (the story)
  Order.kt                ← sealed Order states
  OrderId.kt              ← value class
  OrderRepository.kt      ← interface
  CartExtensions.kt       ← subtotal(), allInStock()
  data/RoomOrderRepository.kt  ← implementation
  ui/OrderScreen.kt

// Avoid
models/ … every model of the app
repositories/ … every repo
utils/Utils.kt … 900 lines of everything
```

Feature packages keep related things one Ctrl+click apart and make files naturally small.

## When to split a file

Split when any of these is true:
- Two unrelated public types live in it.
- You scroll to find the function you need.
- The private helpers form their own cluster with its own vocabulary — move them to a new file or class.

## Naming files

- Type files: `TypeName.kt`
- Extensions on a type: `TypeNameExtensions.kt` (or `TypeNames.kt` for top-level factory functions, Kotlin convention like `Collections.kt`)
- Never `Utils.kt`, `Helpers.kt`, `Common.kt`, `Misc.kt` — name the concept.
