"""Tests for the slug generator, cross-referenced with existing DB slugs."""

import re

import pytest

from src.utils.slugify import make_slug

# Regex from the existing products.slug CHECK constraint
SLUG_REGEX = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


class TestKnownProductSlugs:
    """Verify slugs match existing database seed data."""

    @pytest.mark.parametrize("name,expected_slug", [
        ("Merpan 80 WDG", "merpan-80-wdg"),
        ("Botry-Zen WP", "botry-zen-wp"),
        ("Botry-Zen", "botry-zen"),
        ("Gro-Chem Lime Sulphur", "gro-chem-lime-sulphur"),
        ("Thiram 40F", "thiram-40f"),
        ("Thiram 80 WDG", "thiram-80-wdg"),
        ("Capetec", "capetec"),
        ("Goal Advanced", "goal-advanced"),
        ("Oxy 500SC", "oxy-500sc"),
        ("Kenja 400 SC", "kenja-400-sc"),
    ])
    def test_known_slugs(self, name: str, expected_slug: str):
        assert make_slug(name) == expected_slug


class TestAsteriskStripping:
    """Asterisk restriction markers are stripped."""

    @pytest.mark.parametrize("name,expected", [
        ("mancozeb ***", "mancozeb"),
        ("chlorine dioxide **", "chlorine-dioxide"),
        ("oxyfluorfen *", "oxyfluorfen"),
    ])
    def test_asterisks(self, name: str, expected: str):
        assert make_slug(name) == expected


class TestHyphenatedLineBreaks:
    """Line-break hyphens are rejoined, real hyphens preserved."""

    def test_rejoin_linebreak_hyphen(self):
        assert make_slug("Hort-\ncare Copper Hydrox-\nide") == "hortcare-copper-hydroxide"

    def test_preserve_real_hyphen(self):
        assert make_slug("Botry-Zen") == "botry-zen"

    def test_rejoin_mid_word(self):
        assert make_slug("dithiocarba-\nmate") == "dithiocarbamate"


class TestSpecialCharacters:
    """Trademark symbols, parentheses, etc."""

    def test_registered_symbol(self):
        assert make_slug("Pristine®") == "pristine"

    def test_parentheses(self):
        assert make_slug("fatty acids (potassium salts)") == "fatty-acids-potassium-salts"

    def test_ampersand(self):
        slug = make_slug("A & B Product")
        assert SLUG_REGEX.match(slug)

    def test_plus_sign(self):
        slug = make_slug("terbuthylazine + glyphosate")
        assert SLUG_REGEX.match(slug)


class TestIdempotency:
    """Applying make_slug twice produces the same result."""

    @pytest.mark.parametrize("name", [
        "Merpan 80 WDG",
        "Botry-Zen WP",
        "Gro-Chem Lime Sulphur",
        "fatty acids (potassium salts)",
        "mancozeb ***",
    ])
    def test_idempotent(self, name: str):
        slug = make_slug(name)
        assert make_slug(slug) == slug


class TestRegexCompliance:
    """All generated slugs match the DB constraint regex."""

    @pytest.mark.parametrize("name", [
        "Merpan 80 WDG",
        "Gro-Chem Lime Sulphur",
        "fatty acids (potassium salts)",
        "Hort-\ncare Copper Hydrox-\nide",
        "mancozeb ***",
        "Pristine®",
        "A & B Product",
        "terbuthylazine + glyphosate + amitrole *** + oxyfluorfen",
    ])
    def test_regex_match(self, name: str):
        slug = make_slug(name)
        assert SLUG_REGEX.match(slug), f"Slug {slug!r} doesn't match regex"
        assert slug  # not empty
