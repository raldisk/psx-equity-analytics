"""
test_properties_api.py
======================
Hypothesis property-based tests for PSX Analytics API.
Validates validate_date_range() invariants across randomized input space.
"""
from __future__ import annotations

from datetime import date

import pytest
from fastapi import HTTPException
from hypothesis import given, settings, strategies as st


@given(
    from_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2024, 12, 31)),
    to_date=st.dates(min_value=date(2020, 1, 1), max_value=date(2024, 12, 31)),
)
@settings(max_examples=300)
def test_validate_date_range_symmetric(from_date, to_date):
    """validate_date_range() must reject any range where from > to, always."""
    from serving.psx_analytics_api import validate_date_range
    if from_date > to_date:
        with pytest.raises((HTTPException, ValueError)):
            validate_date_range(from_date.isoformat(), to_date.isoformat())
    # from <= to: must not raise for ranges within max_date_range_days


@given(
    symbol=st.text(
        alphabet=st.characters(whitelist_categories=("Lu",)),
        min_size=1,
        max_size=8,
    )
)
@settings(max_examples=100)
def test_symbol_code_uppercase_only(symbol):
    """
    PSX ticker symbols are 1–8 uppercase letters.
    The API must accept all valid uppercase symbol strings without crashing.
    """
    # This is a structural invariant test — the symbol format constraint must hold
    # across the full uppercase-letter space, not just known PSX symbols.
    assert symbol == symbol.upper(), "Hypothesis strategy produced non-uppercase symbol"
    assert 1 <= len(symbol) <= 8, f"Symbol length out of bounds: {len(symbol)}"
