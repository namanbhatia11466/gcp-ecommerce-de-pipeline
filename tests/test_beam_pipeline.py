import json
from datetime import datetime

from apache_beam.pvalue import TaggedOutput

from pipeline.beam_pipeline import ValidateAndEnrich

VALID_ORDER = {
    "order_id": "o1",
    "customer_id": "c1",
    "product_id": "p1",
    "amount": 42.0,
    "status": "delivered",
}


def _process(order: dict) -> list:
    """ValidateAndEnrich.process() is a plain generator - no pipeline
    context needed to call it directly for unit testing."""
    return list(ValidateAndEnrich().process(json.dumps(order)))


def test_valid_order_is_enriched_and_passes_through():
    results = _process(VALID_ORDER)
    assert len(results) == 1
    enriched = json.loads(results[0])
    assert enriched["status"] == "DELIVERED"
    datetime.fromisoformat(enriched["processed_at"])


def test_missing_required_field_goes_to_dead_letter():
    order = {k: v for k, v in VALID_ORDER.items() if k != "customer_id"}
    results = _process(order)
    assert len(results) == 1
    assert isinstance(results[0], TaggedOutput)
    assert results[0].tag == "dead_letter"


def test_zero_or_negative_amount_goes_to_dead_letter():
    for bad_amount in (0, -5):
        result = _process({**VALID_ORDER, "amount": bad_amount})[0]
        assert isinstance(result, TaggedOutput)
        assert result.tag == "dead_letter"


def test_malformed_json_goes_to_dead_letter():
    results = list(ValidateAndEnrich().process("not valid json"))
    assert len(results) == 1
    assert results[0].tag == "dead_letter"


def test_value_tier_thresholds():
    # Calibrated to the real Olist amount distribution - see beam_pipeline.py
    cases = [
        (10, "low"),
        (79.99, "low"),
        (80, "medium"),
        (249.99, "medium"),
        (250, "high"),
        (1000, "high"),
    ]
    for amount, expected_tier in cases:
        enriched = json.loads(_process({**VALID_ORDER, "amount": amount})[0])
        assert enriched["value_tier"] == expected_tier, f"amount={amount}"
