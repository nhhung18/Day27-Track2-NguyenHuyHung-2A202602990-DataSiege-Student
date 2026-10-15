# Reflection

### Which fault types were hardest to catch, and why?

1. **Subtle In-Distribution Checks (`data_batch`)**:
   Faults like subtle `volume_drop`, `volume_spike`, `null_spike`, and `distribution_shift` were the most challenging. Obvious faults clearly exceed 3-sigma boundaries (e.g., row counts > 800 or null rates > 0.03), making them cleanly separable. In contrast, subtle faults in the private phase were intentionally generated to fall well within the empirical variance of the clean stream (for instance, row counts around 469–519 against a clean baseline span of [448, 554], or null rates around 0.0076–0.0083 against clean rates up to 0.0105). Any static threshold tightened enough to catch these subtle deviations immediately introduces multiple False Positives on normal clean batches. Given the asymmetric scoring penalty ($-0.3 \times \text{FPR}$ on 146 clean events vs $+0.5 \times \text{TPR}$ on 54 faulty events), false alarms rapidly degrade the net score.

2. **Subtle Runtime Variations (`lineage_run`)**:
   Subtle `runtime_anomaly` faults exhibited durations between 4480ms and 4740ms. However, normal clean pipeline runs experienced durations up to 4805ms with standard deviations around 280ms. At an individual event level, a 4600ms run is statistically indistinguishable from a legitimate runtime fluctuation under standard server load. Thresholding below the clean maximum incurs a cluster of false alarms. Conversely, structural lineage anomalies (`missing_upstream` and `orphan_output`) were unambiguous and caught with 100% precision.

3. **Subtle AI Infra Drift (`embedding_batch`)**:
   Subtle `corpus_staleness` and `embedding_drift` were close to normal corpus evolution (e.g., centroid shifts around 0.028–0.031 vs clean shifts up to 0.038). Without a longer reference history, isolated single-batch vector metrics cannot reliably differentiate slight topic rotation from true degradation.

---

### What would you change about your cost/coverage tradeoff, if you had another pass?

1. **Multi-Event Sequential State Tracking (`ctx.state`)**:
   Currently, decisions are evaluated on an event-by-event basis. With another iteration, we would implement Exponentially Weighted Moving Averages (EWMA) and CUSUM (cumulative sum control charts) maintained across consecutive events in `ctx.state`. While a single batch with a small mean shift ($\Delta \approx 1\sigma$) or slight volume change could be random noise, a consecutive sequence of 3–5 batches consistently shifted in the same direction provides high-confidence statistical proof of drift without raising single-event false alarms.

2. **Adaptive Tool Scheduling for Budget-Constrained Streams**:
   In the private phase, the 320-credit budget comfortably accommodated single-pass evaluation across all 200 events (costing 300 credits, $0.0$ overage). However, in the tighter public phase (240 cost vs 220 budget), calling every tool incurred an overage penalty. If facing even tighter budgets, we would implement dynamic skip-metering:
   - Always run low-cost structural checks (`batch_profile` @ 1.0, `lineage_graph_slice` @ 1.0).
   - Dynamically sample higher-cost checks (`feature_drift` @ 2.0, `embedding_drift` @ 2.0) every $k$ intervals unless an adjacent batch or upstream contract checkpoint exhibits warning signs.
   - Use `ctx.tools.budget_remaining()` to switch between strict mode (when credits are abundant) and triage mode (when credits deplete).
