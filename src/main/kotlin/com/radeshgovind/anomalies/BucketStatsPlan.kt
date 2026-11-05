package com.radeshgovind.anomalies

// How O1 gives each trip its bucket's statistics (D16/D17). Both plans compute the same columns with the same
// semantics; only the physical plan differs. Pipeline.kt picks one from --skip-opts.
enum class BucketStatsPlan {
    // D22, the baseline: a window over the bucket, so every trip is shuffled and sorted by bucket.
    WINDOW,

    // OPT-2 (H2): one row per bucket from a groupBy (partial aggregation, then a tiny shuffle), broadcast back onto
    // the trips. The statistics side scans the trips once more, because nothing is cached.
    BROADCAST_AGGREGATE,
}
