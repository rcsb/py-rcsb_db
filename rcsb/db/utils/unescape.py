#
# from six.moves.html_parser import HTMLParser

try:
    # Python 2.6-2.7
    from HTMLParser import unescape
except ImportError:
    # Python 3.6+
    from html import unescape


def unescapeXmlCharRef(iStr):
    """
    Convert html character entities into unicode.
    """
    oStr = unescape(iStr)

    return oStr


if __name__ == "__main__":
    print("START")
    print("%r" % unescapeXmlCharRef("&lt;b&gt;"))
    print("%r" % unescapeXmlCharRef("Here is a &quot;").encode("utf-8"))
    print("%r" % unescapeXmlCharRef("Here is a &Phi;").encode("utf-8"))
    print("%range" % unescapeXmlCharRef("Here is a &Psi;"))
    print("%r" % unescapeXmlCharRef("Here is a &alpha;"))
    print("%r" % unescapeXmlCharRef("Here is a &#xa3;"))

    print("%r" % unescapeXmlCharRef("Here is a &#8453;"))
    print("%r" % unescapeXmlCharRef("Here is a &#9734;"))
    print("%r" % unescapeXmlCharRef("Here is a &#120171;"))
    for ichar in range(1, 8000):
        myStr = "decimal %6d char &#%d;" % (ichar, ichar)
        print("%r" % unescapeXmlCharRef(myStr))
