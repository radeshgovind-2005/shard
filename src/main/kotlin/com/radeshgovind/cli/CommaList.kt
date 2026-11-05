package com.radeshgovind.cli

// "a,b,c" → the parsed set, or null if any item doesn't parse (an empty item included).
internal fun <T> parseCommaList(raw: String, parseItem: (String) -> T?): Set<T>? {
    val items = raw.split(",").map(parseItem)
    return if (null in items) null else items.filterNotNull().toSet()
}
