##
# File: TextUtil.py
# Date: 8-Jan-2019
#
# Collection of text utilities -
#
##

try:
    from html import unescape  # python 3.4+
except ImportError:
    try:
        from html.parser import HTMLParser  # python 3.x (<3.4)
    except ImportError:
        from HTMLParser import HTMLParser  # python 2.x
    unescape = HTMLParser().unescape


def unescapeXmlCharRef(i_str):
    """
    Convert html character entities into unicode.
    """
    try:
        return unescape(i_str)
    except Exception:
        return i_str


if __name__ == '__main__':
    print("BEGIN")
    print("%r" % unescapeXmlCharRef(None))
    print("%r" % unescapeXmlCharRef(''))
    print("%r" % unescapeXmlCharRef('&lt;b&gt;'))
    print("%r" % unescapeXmlCharRef('Here is a &quot;').encode('utf-8'))
    print("%r" % unescapeXmlCharRef('Here is a &Phi;').encode('utf-8'))
    print("%r" % unescapeXmlCharRef('Here is a &Psi;'))
    print("%r" % unescapeXmlCharRef('Here is a &alpha;'))
    print("%r" % unescapeXmlCharRef('Here is a &#xa3;'))

    print("%r" % unescapeXmlCharRef('Here is a &#8453;'))
    print("%r" % unescapeXmlCharRef('Here is a &#9734;'))
    print("%r" % unescapeXmlCharRef('Here is a &#120171;'))
    for ichar in range(1, 8000):
        myStr = "decimal %6d char &#%d;" % (ichar, ichar)
        print("%r" % unescapeXmlCharRef(myStr))
    print("END")
