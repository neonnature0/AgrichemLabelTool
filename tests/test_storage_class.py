"""Tests for HSNO storage class derivation."""

import pytest

from src.utils.storage_class import derive_storage_class


class TestDeriveStorageClass:
    def test_flammable(self):
        assert derive_storage_class(["Flammable liquid Category 3"]) == "flammable"

    def test_corrosive(self):
        assert derive_storage_class(["Causes severe skin burns"]) == "corrosive"

    def test_toxic_high(self):
        assert derive_storage_class(["Acute Tox Category 1 (Oral)"]) == "toxic_high"

    def test_toxic(self):
        assert derive_storage_class(["Acute Tox 4 (Oral)"]) == "toxic"

    def test_ecotoxic(self):
        assert derive_storage_class(["Hazardous to the aquatic environment chronic Category 2"]) == "ecotoxic"

    def test_multiple_takes_highest_priority(self):
        """Flammable takes priority over ecotoxic."""
        result = derive_storage_class([
            "Hazardous to the aquatic environment chronic Category 2",
            "Flammable liquid Category 3",
        ])
        assert result == "flammable"

    def test_empty_returns_none(self):
        assert derive_storage_class([]) is None

    def test_no_relevant_classification(self):
        """Classifications that don't match any group → 'general'."""
        assert derive_storage_class(["Some unrecognised classification"]) == "general"

    def test_aquatic_acute(self):
        assert derive_storage_class(["Aquatic Acute 1"]) == "ecotoxic"

    def test_oxidising(self):
        assert derive_storage_class(["Oxidising liquid Category 1"]) == "oxidising"
