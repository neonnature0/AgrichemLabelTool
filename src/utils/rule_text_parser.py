"""Extract structured data from resistance management rule text.

Parses natural language rule descriptions into machine-readable fields:
max applications, non-consecutive requirement, tank mix requirement.

Examples of rule text from the schedule:
- "A maximum of one application (EL3 - EL47) per season of any Group 3 fungicide in tank mix with sulphur."
- "A maximum of 2 non-consecutive applications per season."
- "A maximum of 3 applications per season of any Group 29 fungicides."
- "Follow label instructions."
- "Apply integrated weed management practices."
"""

from __future__ import annotations

import re

# Word-to-number mapping for rule text
_WORD_TO_NUM: dict[str, int] = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
}

_RE_MAX_APPS = re.compile(
    r"maximum of (\d+|one|two|three|four|five|six)\b",
    re.IGNORECASE,
)

_RE_NON_CONSECUTIVE = re.compile(r"non[- ]consecutive", re.IGNORECASE)

_RE_TANK_MIX = re.compile(
    r"(?:in )?tank mix with (.+?)(?:\.|$)",
    re.IGNORECASE,
)


def extract_max_applications(rule_text: str | None) -> int | None:
    """Extract the maximum number of applications from rule text.

    >>> extract_max_applications("A maximum of 2 non-consecutive applications per season.")
    2
    >>> extract_max_applications("A maximum of one application per season.")
    1
    >>> extract_max_applications("Follow label instructions.")
    """
    if not rule_text:
        return None
    m = _RE_MAX_APPS.search(rule_text)
    if not m:
        return None
    val = m.group(1).lower()
    if val in _WORD_TO_NUM:
        return _WORD_TO_NUM[val]
    return int(val)


def extract_non_consecutive(rule_text: str | None) -> bool:
    """Check if rule requires non-consecutive applications.

    >>> extract_non_consecutive("A maximum of 2 non-consecutive applications per season.")
    True
    >>> extract_non_consecutive("A maximum of 3 applications per season.")
    False
    """
    if not rule_text:
        return False
    return bool(_RE_NON_CONSECUTIVE.search(rule_text))


def extract_tank_mix_requirement(rule_text: str | None) -> str | None:
    """Extract tank mix partner requirement from rule text.

    >>> extract_tank_mix_requirement("in tank mix with sulphur.")
    'sulphur'
    >>> extract_tank_mix_requirement("in tank mix with an effective botrytis product.")
    'an effective botrytis product'
    >>> extract_tank_mix_requirement("A maximum of 3 applications per season.")
    """
    if not rule_text:
        return None
    m = _RE_TANK_MIX.search(rule_text)
    if not m:
        return None
    return m.group(1).strip()
