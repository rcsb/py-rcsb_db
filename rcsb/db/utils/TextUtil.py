"""
Convenience utilities for strings.

**DEPRECATED**: Use builtin `html.unescape` instead of `unescapeXmlCharRef`.
"""

from html import unescape


def unescapeXmlCharRef(iStr):
    """
    Convert html character entities into unicode.
    """
    try:
        return unescape(iStr)
    except Exception:
        return iStr  # TODO: Don't
