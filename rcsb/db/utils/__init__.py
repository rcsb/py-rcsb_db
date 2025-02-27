
from rcsb.utils.io import TimeUtil as _TU


def unescapeXmlCharRef(s: str) -> str:
    import html
    import warnings

    msg = f"{unescapeXmlCharRef.__name__} is deprecated. Use builtin `html.unescape()` instead."
    warnings.warn(msg, DeprecationWarning)
    return html.unescape(s)


class TimeUtil(_TU):

    def __init__(self, **kwargs):
        import warnings

        msg = f"{self.__class__.__name__} is deprecated. Use builtin `datetime` (best) or {_TU.__name__} instead."
        warnings.warn(msg, DeprecationWarning)
        super().__init__(**kwargs)
