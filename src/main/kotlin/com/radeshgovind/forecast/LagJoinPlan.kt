package com.radeshgovind.forecast

// How O3's 4-week-lag self-join (D27) is executed. Both plans join the same sides the same way (left, on zone and
// hour index); only the physical join differs. Pipeline.kt picks one from --skip-opts.
enum class LagJoinPlan {
    // D27, the baseline: a plain join, so Catalyst picks the strategy by estimated size (a SortMergeJoin on 2025,
    // a broadcast join on 2025-01).
    PLAIN,

    // OPT-3 (H6): a broadcast() hint on the lag side, so the join is a BroadcastHashJoin at any scale. The current
    // side isn't re-shuffled and nothing is sorted for the join.
    BROADCAST_LAG_SIDE,
}
