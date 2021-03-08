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
    rss.feeds_to_database(urls, feedsdb_path, tablename="items",
                          tags={"guid": "rss_guid", "link": "rss_link", "pubDate": "rss_pubdate",
                                "title": "rss_title", "description": "rss_description"},
                          keys=["rss_guid", "rss_link"])

    # Count rows in database table after insertion
    conn = sqlite3.connect(feedsdb_path)
    rows_after = int(conn.execute("SELECT COUNT(*) FROM items").fetchone()[0])
    conn.close()

    return rows_after - rows_before


# TODO: Remove functions and __name__ == ...; Use executable script directly

#
# Parse command-line arguments
#
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
parser_download_html.add_argument("-m", "--maxitems", type=int, default=128,  #FIXME: enfore nonneg integers
                                  help="Stop after given number of items have been processed. Used to chunk up "
                                       "workload into batches of predictable duration.")

parser_extract_fulltext = subparsers.add_parser("rss-extract-fulltext", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

args = parser.parse_args()
cfg = configparser.ConfigParser(inline_comment_prefixes=";")
cfg.read("setup.cfg")

# Configure logging. Affects code in all child modules. Logs are always
# stored at full maximum information in the logfile, but the log level may
# be selectively selected for terminal output.
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

log = logging.getLogger("stockbro")
if args.quiet:
    log.setLevel(logging.WARNING)
elif args.verbose:
    log.setLevel(logging.DEBUG)
else:
    log.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('[%(levelname)s] %(funcName)s: %(message)s'))
log.addHandler(ch)


# Dispatch command
if args.command == "rss-fetch":
    # TOOD: Remove rss_fetch() and place implementation here
    rows_created = rss_fetch()
    log.info(f"Generated {rows_created} new RSS record(s).")

elif args.command == "rss-extract-fulltext":
    # Create accessed databases if necessary
    util.create_db(cfg["project"]["rss-feedsdb-path"], cfg["project"]["rss-feedsdb-schema"])
    util.create_db(cfg["project"]["rss-catalogdb-path"], cfg["project"]["rss-catalogdb-schema"])

    conn_feeds = sqlite3.connect(cfg["project"]["rss-feedsdb-path"])
    conn_catalog = sqlite3.connect(cfg["project"]["rss-catalogdb-path"])
    query_join = "SELECT items.rss_guid, items.rss_link, rss_pubdate, rss_title, rss_description FROM " \
        "items LEFT JOIN progress ON (items.rss_guid = progress.rss_guid AND items.rss_link = progress.rss_link) " \
        "WHERE (can_delete IS NULL OR can_delete != 1)"
    for row in conn_feeds.execute(query_join):
        link = row[1]
        title = row[3]
        description = row[4]
        # CONTINUEHERE
        #  1. copy: rss-feeds::items::rss_link -> rss-catalog::items::link
        #           rss_pubdate -> pubdate
        #           rss_description -> description
        #  2. pubdate zu standardisiertem Format konvertieren -> pubdate
        #  3. volltext extrahieren -> fulltext
        #  4. rss-feeds::progress::can_delete = 1
        print(str(row))

    conn_feeds.commit()
    conn_feeds.close()
    conn_catalog.commit()
    conn_catalog.close()
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
        guid, url = record[0], record[1]
        try:
            dest_url = rss.rss_trace_link(url)  # track down destination url, not the appetizer
            reply = requests.get(dest_url, headers={'User-Agent': util.USERAGENT}, timeout=3)
            reply.raise_for_status()  # throw if 400 ≤ ret_code ≤ 600
            html = reply.text
            conn.execute("INSERT INTO html (rss_guid, rss_link, dest_url, html) VALUES (?, ?, ?, ?)", (guid, url, dest_url, html))
            successful = successful + 1
        except requests.exceptions.RequestException as e:  # catches all of requests' exceptions
            log.error(f"Error for requests.get('{url}'): {e}")
        except sqlite3.Error as e:  # catches all of sqlite3's exceptions
            log.error(f"sqlite3 error while trying to store '{url}': {e}")
        except Exception as e:
            log.error(f"Miscellaneous error while trying to store '{url}': {e}")

    log.info(f"Successfully downloaded the raw html of {successful}/{len(records)} RSS items.")
    conn.commit()
    conn.close()
