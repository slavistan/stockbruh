#!/usr/bin/env python3


"""Main modules."""
import argparse
import configparser
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

import requests

from src import rss
from src import util


def rss_fetch() -> int:
    """Fetch RSS feeds and store new records to database.

    Returns
    -------
    int
        Number of inserted records.
    """
    # Read configuration data
    cfg = configparser.ConfigParser(inline_comment_prefixes=";")
    cfg.read("setup.cfg")
    urls = [item for (key, item) in cfg.items("rss-feeds")]
    feedsdb_path = cfg["project"]["rss-feedsdb-path"]
    feedsdb_schema = cfg["project"]["rss-feedsdb-schema"]

    # Count rows in database table before insertion
    util.create_db(feedsdb_path, feedsdb_schema)
    conn = sqlite3.connect(feedsdb_path)
    rows_before = int(conn.execute("SELECT COUNT(*) FROM items").fetchone()[0])
    conn.close()

    # Store feeds to database
    for ii, url in enumerate(urls, 1):
        log.info(f"Fetching items from RSS feed {ii}/{len(urls)}: {url}")
        try:
            rss.feeds_to_database([url], feedsdb_path, tablename="items",
                                  tags={"guid": "rss_guid", "link": "rss_link", "pubDate": "rss_pubdate",
                                        "title": "rss_title", "description": "rss_description"},
                                  keys=["rss_guid", "rss_link"])
        except Exception as e:
            log.error(e)
        ii += 1

    # Count rows in database table after insertion
    conn = sqlite3.connect(feedsdb_path)
    rows_after = int(conn.execute("SELECT COUNT(*) FROM items").fetchone()[0])
    conn.close()

    return rows_after - rows_before


## Command-line argument configuration.
#
# Declares each subcommand and declares its signature.

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser_verbosity_meg = parser.add_mutually_exclusive_group()
parser_verbosity_meg.add_argument("-v", "--verbose", action="store_true", default=False,
                                  help="Increase logging verbosity. Lowers debug level from INFO to DEBUG.")
parser_verbosity_meg.add_argument("-q", "--quiet", action="store_true", default=False,
                                  help="Decreases logging verbosity. Increases debug level from INFO to WARNING.")
subparsers = parser.add_subparsers(help="command help", dest="command")

# configure subcommand
parser_fetch_rss_feeds = subparsers.add_parser("rss-fetch", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser_download_html = subparsers.add_parser("rss-download-html", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser_download_html.add_argument("-m", "--maxitems", type=int, default=32,  # FIXME: enforce nonneg integers
                                  help="Stop after given number of items have been processed. Used to chunk up "
                                       "workload into batches of predictable duration.")

parser_extract_fulltext = subparsers.add_parser("rss-extract-fulltext", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser_extract_fulltext.add_argument("-m", "--maxitems", type=int, default=32,  # FIXME: enforce nonneg integers
                                     help="Stop after given number of items have been processed. Used to chunk up "
                                          "workload into batches of predictable duration.")

args = parser.parse_args()
cfg = configparser.ConfigParser(inline_comment_prefixes=";")
cfg.read("setup.cfg")

## Logging configuration.
#
# All logging output including that from imported modules is saved to file at
# DEBUG-level verbosity. Additionally, logging output of this project's own
# modules is sent to console. The console output's verbosity can be adjusted
# via command-line flags (-v / -q).

# Root logger setup. Writes ALL logging output to file.
logdir = Path(cfg["project"]["logdir"])
if not logdir.is_dir():
    logdir.mkdir(parents=True, exist_ok=True)
logfilename = str(datetime.now().strftime(r"%Y-%m-%d")) + ".log"
logpath = str(logdir / logfilename)  # log-directory/YYYY-MM-DD.log; one logfile per day
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)
fh = logging.FileHandler(logpath)
fh.setFormatter(logging.Formatter('[%(levelname)s] ⌚ %(asctime)s - %(name)s - %(funcName)s: %(message)s'))
root_logger.addHandler(fh)

# Module-level logger. Writes local logging output to console. This logger
# shall be used within every child module like so:
#
#     import logging
#     log = logging.getLogger("stockbro")
#     log.info("...")
#
log = logging.getLogger("stockbro")
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
if args.quiet:
    ch.setLevel(logging.WARNING)
elif args.verbose:
    ch.setLevel(logging.DEBUG)
else:
    ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter('[%(levelname)s] %(funcName)s: %(message)s'))
log.addHandler(ch)


## Command dispatch.

if args.command == "rss-fetch":
    rows_created = rss_fetch()
    log.info(f"Generated {rows_created} new RSS record(s).")

elif args.command == "rss-extract-fulltext":
    # Create accessed databases if necessary
    util.create_db(cfg["project"]["rss-feedsdb-path"], cfg["project"]["rss-feedsdb-schema"])
    util.create_db(cfg["project"]["rss-catalogdb-path"], cfg["project"]["rss-catalogdb-schema"])

    conn_feeds = sqlite3.connect(cfg["project"]["rss-feedsdb-path"])
    conn_catalog = sqlite3.connect(cfg["project"]["rss-catalogdb-path"])
    query_join = """
        SELECT rss_guid, rss_link, rss_pubdate, rss_title, rss_description, dest_url, html FROM
            (
                SELECT rss_guid, rss_link, rss_pubdate, rss_title, rss_description FROM items LEFT JOIN progress
                    USING(rss_guid, rss_link) WHERE progress.can_delete IS NULL
            ) LEFT JOIN html USING(rss_guid, rss_link) WHERE html IS NOT NULL
        """
    records = conn_feeds.execute(query_join).fetchmany(args.maxitems)
    successful = 0  # number of successful downloads
    for ii, record in enumerate(records, 1):
        rss_guid, rss_link, pubdate = record[0], record[1], record[2]
        title, description = record[3], record[4]
        dest_url, html = record[5], record[6]

        try:
            # Extract fulltext, convert date to standard format and store to 'rss-catalog.db'
            # TODO: Date conversion is missing
            fulltext = rss.extract_fulltext(dest_url, html)
            date = pubdate
            conn_catalog.execute("INSERT INTO texts VALUES (?, ?, ?, ?, ?)",
                                 (dest_url, date, title, description, fulltext))

            # Mark as done in 'rss-feeds.db'. Note that the 'progress' table is
            # empty by default so that we may simple insert values instead of
            # updating them.
            conn_feeds.execute("INSERT INTO progress VALUES (?, ?, ?)",
                               (rss_guid, rss_link, 1))

            successful += 1
        except Exception as e:
            # Exceptions are raised for urls whose extraction scheme is missing or incomplete
            log.error(e)

    conn_feeds.commit()
    conn_feeds.close()
    conn_catalog.commit()
    conn_catalog.close()
    log.info(f"Successfully downloaded the raw html of {successful}/{len(records)} RSS items.")

elif args.command == "rss-download-html":
    # Set up database and connection
    util.create_db(cfg["project"]["rss-feedsdb-path"], cfg["project"]["rss-feedsdb-schema"])
    conn = sqlite3.connect(cfg["project"]["rss-feedsdb-path"])

    # Retrieve all records whose raw html is missing
    query_join = "SELECT items.rss_guid, items.rss_link FROM " \
        "items LEFT JOIN html USING (rss_guid, rss_link) " \
        "WHERE (html.html IS NULL)"
    records = conn.execute(query_join).fetchmany(args.maxitems)

    # For each record attempt to download the raw html and write it to the database
    successful = 0  # number of successful downloads
    for ii, record in enumerate(records, 1):
        log.info(f"Downloading raw HTML of RSS item {ii}/{len(records)}.")
        guid, dest_url = record[0], record[1]
        try:
            dest_url = rss.rss_trace_link(dest_url)  # track down destination url, not the appetizer
            reply = requests.get(dest_url, headers={'User-Agent': util.USERAGENT}, timeout=3)
            reply.raise_for_status()  # throw if 400 ≤ ret_code ≤ 600
            html = reply.text
            conn.execute("INSERT INTO html (rss_guid, rss_link, dest_url, html) VALUES (?, ?, ?, ?)", (guid, dest_url, dest_url, html))
            successful += 1
        except requests.exceptions.RequestException as e:  # catches all of requests' exceptions
            log.error(f"Error for requests.get('{udest_urlrl}'): {e}")
        except sqlite3.Error as e:  # catches all of sqlite3's exceptions
            log.error(f"sqlite3 error while trying to store '{udest_urlrl}': {e}")
        except Exception as e:
            log.error(f"Miscellaneous error while trying to store '{udest_urlrl}': {e}")

    log.info(f"Successfully downloaded the raw html of {successful}/{len(records)} RSS items.")
    conn.commit()
    conn.close()
