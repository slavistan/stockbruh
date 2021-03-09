import os

from src import rss
from src import util

def read_test_parameters(testname):
    """Given a fulltext-extraction test name return tuple of url, HTML and target fulltest.

    Parameters
    ----------
    testname: str
        Name of the fulltest-extraction test to run. Must not include the path.

    Returns
    -------
    tuple
        Tuple consisting of url, raw HTML and the target fulltext to compare
        extraction against.
    """
    with open(f"test/extract-fulltext/{testname}.txt") as f:
        fulltext = f.read()
    with open(f"test/extract-fulltext/{testname}.html") as f:
        foo = f.readlines()
        url = foo[0].rstrip("\n")  # first line carries URL
        html = "".join(foo[1:])  # rest is raw HTML
    return (url, html, fulltext)

def test_all():
    # get all names
    names = { name.split("/")[-1][:-4] for name in os.listdir("test/extract-fulltext") if name.endswith(".txt") }
    for name in names:
        url, html, fulltext = read_test_parameters(name)
        assert fulltext == rss.extract_fulltext(url, html)


def testfoo():
    assert True