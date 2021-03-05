"""RSS utility."""
import logging
import re
import sqlite3
from pathlib import Path

import bs4

import pandas as pd

import requests

import tldextract

DEFAULT_RSS_FIELD_NAMES = {"link": "link", "guid": "guid", "pubDate": "pubDate",
                           "title": "title", "description": "description"}
DEFAULT_RSS_DBTABLE_KEYS = ["guid", "link"]


def feeds_to_dataframe(urls: list, tags: dict = DEFAULT_RSS_FIELD_NAMES) -> pd.DataFrame:
    """Download RSS feeds and return as dataframe.

    Non-existing tags or tags without content are stored as empty strings "".

    Parameters
    ----------
    urls: list of str
        URL strings, e.g. https://news.co.uk/rss.xml

    tags: dict (optional)
        Key-value pairs of RSS tags to fetch and their corresponding colum
        name in the output table. By default, the tags *link*, *guid*,
        *pubDate*, *title* and *description* are downloaded and their
        information stored in eponymous columns. Keys and values must be
        unique.

    Returns
    -------
    pandas.DataFrame
        All records retrieved for the specified urls. Columns names are
        determined by the values of 'tags'.

    Examples
    -----------
    To limit the RSS tags to *link* and *pubDate* and to name the corresponding
    dataframe columns *rss_link* and *rss_pubdate* call

        urls = ["https://www.finanznachrichten.de/rss-nachrichten-meistgelesen",
                "https://www.finanznachrichten.de/rss-marktberichte"]
        feeds_to_dataframe(urls, tags={"link": "rss_link", "pubDate": "rss_pubdate"})
    """
    df = pd.DataFrame(columns=[val for val in tags.values()], dtype="string")
    for url in urls:
        response = requests.get(url)
        if response.status_code == 200:
            soup = bs4.BeautifulSoup(response.text, 'xml').find("rss")
            if soup is not None:
                for item in soup.find_all("item"):
                    record = {}
                    for rss_tag, field_name in tags.items():
                        tag = item.find(rss_tag)
                        record[field_name] = tag.text if tag is not None else ""
                    df = df.append(record, ignore_index=True)
        else:
            logging.error(f"Download of RSS feed {url} failed.")
    return df


def feeds_to_database(urls: list, dbpath: str, tablename: str = "items", tags: dict = DEFAULT_RSS_FIELD_NAMES,
                      keys: list = DEFAULT_RSS_DBTABLE_KEYS) -> None:
    """Download RSS feeds and store to sqlite database.

    Parameters
    ----------
    urls: list of str
        URL strings, e.g. https://news.co.uk/rss.xml

    dbpath: str
        Relative or absolute path to sqlite database. Database will be created if it does not exist.

    tablename: str (optional)
        Name of database table where information will be stored. Default:
        "items".

    tags: dict (optional)
        Key-value pairs of RSS tags to fetch and their corresponding colum
        name in the database table. By default, the tags *link*, *guid*,
        *pubDate*, *title* and *description* are downloaded and their
        information stored in eponymous columns. Keys and values must be
        unique.

    keys: list of str (optional)
        Subset of column names in *tags*'s values which shall constitue the
        compound primary key. Must be specified if non-default *tags* are used.
        By default, the *link* and *guid* column are used as primary key.

    Examples
    -----------
    The following call will fetch the RSS tags *link*, *pubDate* and *title*,
    name the corresponding columns *rss_link*, *rss_pubdate* and *rss_title* in
    a database table *items*, which will use the columns *rss_link* and
    *rss_title* as compound primary key (unless the database already exists).

        urls = ["https://www.finanznachrichten.de/rss-nachrichten-meistgelesen",
                "https://www.finanznachrichten.de/rss-marktberichte"]
        feeds_to_sqlite(urls, "db/foo.db", tablename="items",
                          tags={"link": "rss_link", "pubDate": "rss_pubdate", "title": "rss_title"},
                          keys=["rss_link", "rss_title"])
    """
    df = feeds_to_dataframe(urls, tags)
    columns = list(df)  # retrieve column names
    # create path to db
    path = Path(dbpath)
    if not path.is_file():
        path.parent.mkdir(parents=True, exist_ok=True)
        # construct instruction to create table based on given column names. E.g.
        # CREATE TABLE items (guid TEXT, link TEXT, ..., PRIMARY KEY (guid, link))
        create_table_instruction = f"CREATE TABLE {tablename} (" \
            + " TEXT, ".join(columns + [""]) \
            + " PRIMARY KEY (" \
            + ", ".join(keys) \
            + "))"
        conn = sqlite3.connect(str(path))
        conn.execute(create_table_instruction)
    else:
        conn = sqlite3.connect(str(path))
    # construct instruction to insert records into table. E.g
    # INSERT OR IGNORE INTO items (guid, link) VALUES (?, ?)
    insert_instruction = f"INSERT OR IGNORE INTO {tablename} (" + ", ".join(columns) + ") VALUES (" \
        + ("?, " * len(columns)).rstrip(", ") + ")"
    conn.executemany(insert_instruction, list(df.itertuples(index=False, name=None)))
    conn.commit()
    conn.close()


def rss_trace_link(link: str) -> str:
    """Return destination URL of a resource pointed by an RSS feed's link.

    Some RSS feeds return links pointing not the actual article, but rather an
    intermediate "appetizer" page which only links the page or article in
    question. This is often the case when the target content is provided
    outside the RSS feed's own domain and refers to an external site or portal.

    This function returns the final URL of the content the RSS feed links to
    indirectly. Depending on the input URL, the correct (handcrafted)
    extraction scheme is chosen to retrieve the destination URL.

    Parameters
    ----------
    link: str
        The RSS feed's link tag's text.

    Returns
    -------
    str
        Destination URL of the targeted resource.
    """
    extr = tldextract.extract(link)
    domain, suffix = extr.domain, extr.suffix
    tld = f"{domain}.{suffix}"
    response = requests.get(link, timeout=3)
    soup = bs4.BeautifulSoup(response.text, "html.parser")

    if tld == "finanznachrichten.de":
        content = soup.find("div", {"id": "artikelTextPuffer"})
        if content is None:
            errmsg = f"Incomplete handler for tld '{tld}' (link: {link})."
            logging.error(errmsg)
            raise NotImplementedError(errmsg)

        onclick_span = content.find("span", {"onclick": True})
        if onclick_span is None:
            # Page shows full content. Nothing to do.
            return link
        else:
            # Page shows only embedded article preview. Track down external news id, an 8 digit number hidden inside
            # the value of the 'onclick' property of a 'span' tag. The ID is then used to retrieve the redirection target.
            # Example: '<span onclick="FN.artikelKomplettID('0', 52172551) ...'"
            onclick = onclick_span["onclick"]
            matches = re.findall(r"\d{8}", onclick)
            if len(matches) == 0:
                errmsg = f"Incomplete handler for tld '{tld}' (link: {link})."
                logging.error(errmsg)
                raise NotImplementedError(errmsg)
            news_id = matches[0]
            redirect = requests.get(f"https://www.finanznachrichten.de/ext/nachricht-komplett-{news_id}-0.htm")
            destination = redirect.url
            return destination
    else:
        errmsg = f"Missing handler for tld '{tld}' (link: {link})."
        logging.error(errmsg)
        raise NotImplementedError(errmsg)


def extract_single_tag(html, tag, attribute, value):
    return html.find(tag, {attribute: value}).text.replace("\r", "")


def extract_multi_tag(html, tag, attribute, value, cut=0):
    text = ''
    try:
        texts = html.find(tag, {attribute: value}).find_all("p")
        text = ''
        if cut == 0:
            for paragraph in texts:
                if paragraph.attrs == {}:
                    text = text + paragraph.text + ' '
        else:
            for paragraph in texts[:cut]:
                if paragraph.attrs == {}:
                    text = text + paragraph.text + ' '
    except:
        pass
    return text


def cleanup_by_tld(html, tld) -> str:
    html = bs4.BeautifulSoup(html, 'html.parser')
    text = ""
    if tld == 'finanznachrichten.de':
        tag = html.find("div", {"id": "artikelTextPuffer"})
        if tag is not None:
            text = tag.text
        try:
            text = extract_single_tag(
                html, "div", "id", "artikelTextPuffer")
        except:
            try:
                text = extract_multi_tag(
                    html, "div", "id", "artikelTextPuffer")
            except:
                pass

    elif tld == 'ariva.de':
        text = extract_multi_tag(
            html, "div", "id", "pageSingleNews", -3)

    elif tld == 'finanzen.at':
        text = extract_single_tag(
            html, "div", "class", "news-content")

    elif tld == 'deraktionaer.de':
        text = extract_multi_tag(
            html, "div", "id", "article-body", -1)

    elif tld == 'fool.de':
        text = extract_multi_tag(
            html, "section", "id", "full_content", -1)

    elif tld == 'timschaefermedia.com' or tld == 'feingold-research.com':
        text = extract_multi_tag(
            html, "div", "class", "entry-content")

    elif tld == '4investors.de':
        texts = html.find("article").find_all("p")[1].contents[:-9]
        for paragraph in texts:
            try:
                text = text + paragraph + ' '
            except:
                pass

    elif tld == 'markteinblicke.de':
        text = extract_multi_tag(
            html, "div", "class", "td-post-content", -2)

    elif tld == 'moneycab.com':
        text = extract_multi_tag(
            html, "div", "class", "entry__post-content", -1)

    elif tld == 't3n.de':
        text = extract_multi_tag(
            html, "div", "id", "main-content", -2)

    elif tld == 'it-times.de':
        text = extract_multi_tag(
            html, "div", "class", "media-body")

    elif tld == 'finanzen.net':
        texts = html.find(
            "div", {"id": "news-container"}).find_all("p", {"class": "TEXT"})
        text = ''
        for paragraph in texts:
            text = text + paragraph.text + ' '

    elif tld == 'nebenwerte-magazin.com':
        try:
            text = extract_multi_tag(
                html, "div", "class", "article-description")
        except:
            text = extract_multi_tag(
                html, "div", "class", "post_content", -6)

    elif tld == 'goldinvest.de':
        text = extract_multi_tag(
            html, "div", "itemprop", "articleBody", -6)

    elif tld == 'resource-capital.ch':
        text = extract_multi_tag(
            html, "div", "class", "entry__article", -3)

    elif tld == 'rumas.de':
        text = extract_multi_tag(
            html, "div", "itemprop", "articleBody", -2)

    elif tld == 'electrive.net':
        text = extract_multi_tag(
            html, "section", "class", "content")

    elif tld == 'boerse-online.de':
        text = extract_multi_tag(
            html, "div", "class", "content news_detail", -4)

    elif tld == 'bullvestorbb.com':
        text = extract_multi_tag(
            html, "div", "class", "entry-content", -13)

    elif tld == 'boerse-daily.de':
        text = extract_multi_tag(
            html, "div", "class", "ce_text")

    elif tld == 'finanzen.ch':
        text = extract_multi_tag(
            html, "div", "class", "instrument-description", -2)

    elif tld == 'finanztreff.de':
        text = extract_multi_tag(
            html, "div", "class", "article")

    elif tld == 'trading-treff.de':
        text = extract_multi_tag(
            html, "div", "class", "entry-content", -5)

    elif tld == 'start-trading.de':
        text = extract_multi_tag(
            html, "div", "class", "entry-content", -7)

    elif tld == 'fuchsbriefe.de':
        text = extract_multi_tag(
            html, "div", "itemprop", "articlebody")

    elif tld == 'ratgebergeld.at':
        text = extract_multi_tag(
            html, "div", "class", "wpb_text_column wpb_content_element", -6)

    elif tld == 'anlegerverlag.de':
        text = extract_multi_tag(
            html, "div", "class", "entry-content")

    elif tld == 'investinghaven.com':
        text = extract_multi_tag(
            html, "div", "class", "content-inner", -1)

    elif tld == 'kapitalerhoehungen.de':
        texts = html.find("article").find_all("p")
        for paragraph in texts[:-4]:
            if paragraph.attrs == {}:
                text = text + paragraph.text + ' '

    elif tld == 'mydividends.de':
        text = extract_multi_tag(
            html, "div", "itemprop", "articleBody", -1)

    elif tld == 'esg-aktien.de':
        text = extract_multi_tag(
            html, "div", "id", "mainContent")

    elif tld == 'ki-portal.de':
        text = extract_multi_tag(
            html, "div", "class", "content clearfix")

    elif tld == 'plastverarbeiter.de':
        text = extract_multi_tag(
            html, "section", "class", "post-content", -3)

    elif tld == 'chemietechnik.de':
        text = extract_multi_tag(
            html, "article", "itemprop", "articleBody", -2)

    elif tld == 'de.com':  # Subdomain prüfen
        text = extract_multi_tag(
            html, "div", "id", "fxs_article_body", -1)

    elif tld == 'bondguide.de':
        text = extract_multi_tag(
            html, "div", "class", "entry-content", -1)

    elif tld == 'inv3st.de':
        texts = html.find("article").find_all("p")
        for paragraph in texts[:-6]:
            if paragraph.attrs == {}:
                text = text + paragraph.text + ' '

    elif tld == 'mein-geld-medien.de':
        text = extract_multi_tag(
            html, "div", "itemprop", "articleBody", -1)

    elif tld == 'boersengefluester.de':
        text = extract_multi_tag(
            html, "div", "class", "entry-content", -1)

    elif tld == 'stock-world.de':  # recheck
        pass

    elif tld == 'kgk-rubberpoint.de':
        text = extract_multi_tag(
            html, "section", "class", "post-content")

    elif tld == 'boersennews.de':
        text = extract_multi_tag(
            html, "div", "itemprop", "articleBody", -8)

    elif tld == 'boerse-global.de':
        text = extract_multi_tag(
            html, "div", "class", "entry-content clearfix", -2)

    elif tld == 'lynxbroker.de':
        text = extract_multi_tag(
            html, "div", "class", "article__content", -2)

    elif tld == 'anleihen-finder.de':
        text = extract_multi_tag(
            html, "div", "class", "news", -20)

    elif tld == 'onvista.de':
        # text = extract_multi_tag(
        #     html, "div", "id", "newsContentContainer", -2)
        try:
            texts = html.find(
                "div", {"id": 'newsContentContainer'}).find_all("font")
            for paragraph in texts[:-5]:
                text = text + paragraph.text + ' '
            if text == '':
                text = extract_multi_tag(
                    html, "div", "id", "newsContentContainer")
        except:
            pass

    elif tld == 'xtb.com':
        text = extract_multi_tag(
            html, "div", "class", "market-news-single-content", -1)

    elif tld == 'anleihencheck.de':
        text = html.find("span", {"class": "analysen_content"}).text

    elif tld == 'asscompact.de':
        text = extract_multi_tag(
            html, "div", "class", "story-body", -1)

    elif tld == 'boerse.de':
        text = extract_multi_tag(
            html, "div", "class", "newsBox readMe", -1)

    elif tld == 'abam-gmbh.com':
        texts = html.find("div", {
                          "class": "fusion-column-wrapper fusion-flex-column-wrapper-legacy"}).find_all("p")
        for paragraph in texts:
            if paragraph.attrs == {}:
                text = text + paragraph.text + ' '

    elif tld == 'solarserver.de':
        try:
            text = extract_multi_tag(
                html, "div", "class", "postContent bodyCopy entry-content clearfix", -1)
        except:
            pass

    elif tld == 'platow.de':
        text = extract_multi_tag(
            html, "div", "class", "article-description")

    elif tld == 'index-radar.de':
        text = extract_multi_tag(
            html, "div", "class", "post-content", -7)

    elif tld == 'finance-magazin.de':
        text = extract_multi_tag(
            html, "section", "id", "content", -1)

    elif tld == 'pv-magazine.de':
        text = extract_multi_tag(
            html, "div", "class", "entry-content", -1)

    elif tld == 'neue-verpackung.de':
        text = extract_multi_tag(
            html, "article", "class", "article")

    elif tld == 'peh.de':
        text = extract_multi_tag(
            html, "div", "class", "financity-single-article-content", -1)

    elif tld == 'euwid-recycling.de':
        text = extract_multi_tag(
            html, "div", "class", "news-single-item", -3)

    elif tld == 'aktien-global.de':
        text = extract_multi_tag(
            html, "div", "itemprop", "articleBody", -1)

    elif tld == 'automobil-produktion.de':
        text = extract_multi_tag(
            html, "article", "itemprop", "articleBody", -2)

    elif tld == 'derboersianer.com':
        text = extract_multi_tag(
            html, "div", "class", "entry post-entry")

    elif tld == 'finanzjournalisten.de':
        texts = html.find_all("div", {
            "class": "et_pb_text_inner"})[1].find_all("p")
        for paragraph in texts:
            if paragraph.attrs == {}:
                text = text + paragraph.text + ' '

    elif tld == 'shareribs.com':
        text = extract_single_tag(
            html, "div", "class", "newsbody")

    elif tld == 'fondscheck.de':
        text = extract_single_tag(
            html, "span", "class", "analysen_content")

    elif tld == 'plusvisionen.de':
        text = extract_multi_tag(
            html, "div", "class", "entry")

    elif tld == 'fondsdiscount.de':
        text = extract_multi_tag(
            html, "div", "class", "article-body", -2)

    elif tld == 'investresearch.net':
        text = extract_multi_tag(
            html, "div", "class", "entry-content")

    elif tld == 'rohstoffbrief.com':
        text = extract_multi_tag(
            html, "div", "class", "post-content", -7)

    elif tld == 'heizoel24.de':
        text = extract_multi_tag(
            html, "span", "itemprop", "articleBody")

    elif tld == 'ideas-daily.de':
        text = extract_multi_tag(
            html, "section", "id", "main-content-section")

    elif tld == 'aktien.guide':
        text = extract_multi_tag(
            html, "div", "class", "news-content", -2)

    elif tld == 'ntg24.de':
        text = extract_multi_tag(
            html, "div", "class", "articleContent", -1)

    elif tld == 'kapitalmarkt.blog':
        text = extract_multi_tag(
            html, "div", "class", "post-entry", -3)

    elif tld == 'world-news-monitor.de':
        text = extract_multi_tag(
            html, "div", "class", "entry-content")

    elif tld == 'miningscout.de':
        text = extract_multi_tag(
            html, "div", "class", "post-content", -1)

    elif tld == 'nebenwerte-online.de':
        text = extract_multi_tag(
            html, "div", "class", "entry-content", -1)

    elif tld == 'ideas-magazin.de':
        text = extract_multi_tag(
            html, "div", "class", "ce-bodytext")

    elif tld == 'vontobel.com':
        text = extract_multi_tag(
            html, "span", "class", "column three details", -1)

    elif tld == 'aktienfinder.net':
        text = extract_multi_tag(
            html, "div", "class", "the_content_wrapper", -1)
    elif tld == 'smartinvestor.de':
        text = extract_multi_tag(
            html, "div", "id", "content", -15)

    elif tld == 'intelligent-investieren.net' or tld == 'scenarieconomici.it' or tld == 'tichyseinblick.de' or tld == 'tmx.com':
        pass
    elif tld == 'sg-zertifikate.de' or tld == 'boerse-social.com' or tld == 'clausvogt.com' or tld == 'hsbc-zertifikate.de':
        pass
    elif tld == 'formationstrader.de' or tld == 'mailchi.mp' or tld == 'fruchtportal.de' or tld == 'derfinanzinvestor.de':
        pass
    elif tld == 'youtube.com' or tld == 'ethische-rendite.de' or tld == 'deutsche-wirtschafts-nachrichten.de':
        pass
    elif tld == 'pharma-food.de' or tld == 'onemarkets.de' or tld == 'was-audio.de' or tld == 'oddo-bhf.com':
        pass
    elif tld == 'assetstandard.com' or tld == 'tradingeconomics.com' or tld == 'barchart.com' or tld == 'bnpparibas.com':
        pass
    elif tld == 'finanzen100.de' or tld == 'wallstreet-online.de':
        pass
    else:
        print(tld)
        pass

    return text if text else None


if __name__ == "__main__":
    #urls = ["https://lukesmith.xyz/rss.xml", "https://www.finanznachrichten.de/rss-nachrichten-meistgelesen"]
    # rss_trace_link("https://www.finanznachrichten.de/nachrichten-2021-03/52172551-chart-check-itm-power-diese-marke-muss-heute-halten-124.htm")
    rss_trace_link("https://www.finanznachrichten.de/nachrichten-2021-03/52158803-opening-bell-tripadvisor-alibaba-bilibili-johnson-johnson-plug-paypal-fuelcell-tesla-nio-398.htm")
    # tags = {"link": "lelink", "guid": "rss_guid", "title": "rss_title"}
    # keys = ["rss_guid", "rss_title"]
    # print(feeds_to_dataframes(urls, tags=tags).to_csv())
    # archive_rss_feed(urls, "db/rss.db", "items", tags, keys)
    # feeds_to_database(urls, "db/foo.db", tablename="items",
    #                   tags={"link": "rss_link", "pubDate": "rss_pubdate", "title": "rss_title"},
    #                       keys=["rss_link", "rss_title"])
