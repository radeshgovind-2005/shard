---
name: kotlin-stepdown
description: Write and refactor Kotlin so it reads top-down like a story — tiny descriptive functions (Stepdown Rule, one level of abstraction each), short single-concept files, and type-driven design with interfaces, data classes, value classes and sealed hierarchies. Use this skill whenever writing, generating, reviewing or refactoring ANY Kotlin code (Android, KMP, Compose, Ktor, Spring, scripts), even if the user doesn't mention style, readability or clean code. Also use when the user asks to "make this readable", "split this function", "clean this up", or "refactor" Kotlin.
---

# Kotlin Stepdown

Code should read like a book with a table of contents.
The top function tells the story.
Each Ctrl+click goes exactly one level deeper.
Types carry the rules, so functions don't have to check them.

## The 8 core rules

1. **Stepdown.** A function calls functions one level of abstraction below it. Callers sit above callees in the file.
2. **One level per function.** Never mix "the story" (`chargePayment()`) with "the details" (`amount * 0.23`) in the same body.
3. **Tiny functions.** Aim for 1–7 lines. Over ~10 lines → split. Bodies of `if`, `when` branches and loops are one call.
4. **Names are documentation.** Functions are verbs (`buildInvoice`, `countActiveUsers`). Booleans read as questions (`isExpired`, `hasStock`). No comments explaining *what* — rename instead.
5. **Types tell the truth.** `value class` for IDs/units, `data class` for records, `sealed interface` for states/results, `interface` at every boundary. Make illegal states unrepresentable.
6. **Parse at the edge, trust inside.** Raw input becomes a valid domain type once, at the boundary. Inner code never re-validates.
7. **Short files, one concept.** One public type (plus its close helpers) per file. Target < 150 lines, hard ceiling ~250.
8. **Reuse through small verbs.** Prefer extension functions and small pure functions that many callers can share over copy-paste or utility god-objects.

## Workflow when writing new code

1. Model the domain types first (read `references/types.md`).
2. Write the top-level function as a list of named steps, even before those steps exist.
3. Implement each step below it, one level deeper. Repeat until steps are trivial.
4. Split into files by concept (read `references/files.md`).
5. Run the checklist below.

## Workflow when refactoring existing code

1. Find the longest function. Read it and label each block with what it *does* in 2–4 words.
2. Each label becomes a function name. Extract it (read `references/functions.md`).
3. Replace primitives that carry meaning (`String` ids, `Boolean` flags, nullable combos) with types.
4. Replace `if/else` chains on type or state with a sealed hierarchy + exhaustive `when`.
5. Move extracted pieces into their own files when a file passes ~150 lines.
6. Keep behavior identical. Refactor in small steps; run tests between steps if they exist.

## Reference files — read when relevant

| File | Read when |
|---|---|
| `references/functions.md` | Writing or splitting any function |
| `references/types.md` | Creating models, states, results, IDs, interfaces |
| `references/files.md` | Deciding file/package layout or a file is getting long |
| `references/naming.md` | Naming anything, or a name needs a comment to be understood |
| `references/spark.md` | Writing Kotlin against the Spark API (`Dataset<Row>`, `Column`, `SparkSession`) |

## Final checklist (run before handing back code)

- [ ] Top function reads as a short story of named steps
- [ ] No function mixes abstraction levels
- [ ] No function over ~10 lines without a strong reason
- [ ] Every `if`/`when`/loop body is a single call or expression
- [ ] No raw `String`/`Long` used as an ID or unit where a `value class` fits
- [ ] No Boolean flags that encode state — sealed types instead
- [ ] `when` over sealed types is exhaustive, no `else`
- [ ] Dependencies are interfaces, injected through constructors
- [ ] Each file holds one concept and is under ~150 lines
- [ ] No comment that a better name could replace

## When to bend the rules

- Hot loops where an extra call measurably hurts performance (rare in practice — measure first).
- Generated code, DSL builders, and Compose `@Preview`s.
- A 3-line function split into three 1-line functions that add no meaning. Splitting must add a *name*, not just a jump.
