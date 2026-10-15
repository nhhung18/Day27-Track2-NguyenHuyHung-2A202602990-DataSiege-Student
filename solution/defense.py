"""
Defense implementation for Data Siege.
Registers handlers for each event type and inspects pipeline health metrics
using ctx.tools and ctx.baseline calibrated limits.
"""
from api import Verdict


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def check_data_batch(payload, ctx):
    """
    Checks data batch profile for volume, null rate, amount distribution,
    and staleness anomalies.
    """
    batch_id = payload.get("batch_id")
    if not batch_id:
        return Verdict(alert=False, pillar="checks", reason="missing batch_id")

    prof = ctx.tools.batch_profile(batch_id)
    if not isinstance(prof, dict) or "error" in prof:
        return Verdict(alert=False, pillar="checks", reason=prof.get("error", "tool error"))

    row_count = prof.get("row_count", 0)
    row_min = ctx.baseline.get("row_count_min", 435.4732)
    row_max = ctx.baseline.get("row_count_max", 561.2948)
    if row_count < row_min or row_count > row_max:
        return Verdict(alert=True, pillar="checks", reason=f"row_count anomaly: {row_count}")

    null_rates = prof.get("null_rate", {})
    customer_null = null_rates.get("customer_id", 0.0)
    null_max = ctx.baseline.get("null_rate_max", 0.0109)
    if customer_null > null_max:
        return Verdict(alert=True, pillar="checks", reason=f"null_rate spike: {customer_null}")

    mean_amount = prof.get("mean_amount", 0.0)
    mean_min = ctx.baseline.get("mean_amount_min", 72.7645)
    mean_max = ctx.baseline.get("mean_amount_max", 90.6053)
    if mean_amount < mean_min or mean_amount > mean_max:
        return Verdict(alert=True, pillar="checks", reason=f"mean_amount shift: {mean_amount}")

    staleness = prof.get("staleness_min", 0.0)
    staleness_max = ctx.baseline.get("staleness_min_max", 8.418)
    if staleness > staleness_max:
        return Verdict(alert=True, pillar="checks", reason=f"staleness lag: {staleness}")

    return Verdict(alert=False, pillar="checks")


def check_contract_checkpoint(payload, ctx):
    """
    Checks contract checkpoints for schema mismatches, type violations,
    and freshness SLA breaches.
    """
    contract_id = payload.get("contract_id")
    checkpoint_batch_id = payload.get("checkpoint_batch_id")
    if not contract_id or not checkpoint_batch_id:
        return Verdict(alert=False, pillar="contracts", reason="missing id")

    diff = ctx.tools.contract_diff(contract_id, checkpoint_batch_id)
    if not isinstance(diff, dict) or "error" in diff:
        return Verdict(alert=False, pillar="contracts", reason=diff.get("error", "tool error"))

    violations = diff.get("violations", [])
    if violations:
        return Verdict(alert=True, pillar="contracts", reason=f"contract violations: {violations}")

    freshness_delay = diff.get("freshness_delay_min", 0.0)
    freshness_max = ctx.baseline.get("freshness_delay_max_min", 11.1141)
    if freshness_delay > freshness_max:
        return Verdict(alert=True, pillar="contracts", reason=f"freshness SLA breach: {freshness_delay}")

    return Verdict(alert=False, pillar="contracts")


def check_lineage_run(payload, ctx):
    """
    Checks pipeline execution lineage graph for runtime anomalies,
    missing upstream dependencies, and orphaned output nodes.
    """
    run_id = payload.get("run_id")
    if not run_id:
        return Verdict(alert=False, pillar="lineage", reason="missing run_id")

    graph = ctx.tools.lineage_graph_slice(run_id)
    if not isinstance(graph, dict) or "error" in graph:
        return Verdict(alert=False, pillar="lineage", reason=graph.get("error", "tool error"))

    duration = graph.get("duration_ms", 0.0)
    duration_max = ctx.baseline.get("lineage_duration_ms_max", 5134.9804)
    if duration > duration_max:
        return Verdict(alert=True, pillar="lineage", reason=f"runtime anomaly: {duration}ms")

    downstream_count = graph.get("actual_downstream_count", 1)
    if downstream_count < 1:
        return Verdict(alert=True, pillar="lineage", reason=f"orphan output: count {downstream_count}")

    upstream = graph.get("actual_upstream", [])
    expected_upstream = {"raw.orders", "raw.customers"}
    if not expected_upstream.issubset(set(upstream)):
        return Verdict(alert=True, pillar="lineage", reason=f"missing upstream: {upstream}")

    return Verdict(alert=False, pillar="lineage")


def check_feature_materialization(payload, ctx):
    """
    Checks feature store materialization for distribution drift and
    training-serving skew.
    """
    feature_view = payload.get("feature_view")
    batch_id = payload.get("batch_id")
    if not feature_view or not batch_id:
        return Verdict(alert=False, pillar="ai_infra", reason="missing feature parameters")

    drift = ctx.tools.feature_drift(feature_view, batch_id)
    if not isinstance(drift, dict) or "error" in drift:
        return Verdict(alert=False, pillar="ai_infra", reason=drift.get("error", "tool error"))

    sigma = drift.get("mean_shift_sigma", 0.0)
    # Threshold calibrated above clean stream maximum variance (0.472) to prevent false alarms
    sigma_max = max(0.50, ctx.baseline.get("feature_mean_shift_sigma_max", 0.4095))
    if sigma > sigma_max:
        return Verdict(alert=True, pillar="ai_infra", reason=f"feature mean shift sigma: {sigma}")

    return Verdict(alert=False, pillar="ai_infra")


def check_embedding_batch(payload, ctx):
    """
    Checks embedding batches for vector drift and document corpus staleness.
    """
    corpus = payload.get("corpus")
    chunk_batch_id = payload.get("chunk_batch_id")
    if not corpus or not chunk_batch_id:
        return Verdict(alert=False, pillar="ai_infra", reason="missing embedding parameters")

    drift = ctx.tools.embedding_drift(corpus, chunk_batch_id)
    if not isinstance(drift, dict) or "error" in drift:
        return Verdict(alert=False, pillar="ai_infra", reason=drift.get("error", "tool error"))

    centroid_shift = drift.get("centroid_shift", 0.0)
    shift_max = ctx.baseline.get("embedding_centroid_shift_max", 0.0435)
    if centroid_shift > shift_max:
        return Verdict(alert=True, pillar="ai_infra", reason=f"centroid shift: {centroid_shift}")

    doc_age = drift.get("avg_doc_age_days", 0.0)
    age_max = ctx.baseline.get("corpus_avg_doc_age_days_max", 49.7955)
    if doc_age > age_max:
        return Verdict(alert=True, pillar="ai_infra", reason=f"corpus staleness: {doc_age} days")

    return Verdict(alert=False, pillar="ai_infra")
