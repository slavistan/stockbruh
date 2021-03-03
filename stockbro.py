"""Main modules."""
import argparse
import configparser
import logging
import sqlite3
from datetime import datetime
from pathlib import Path

from src import rss


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
    dbpath = cfg["project"]["rss-feedsdb-path"]
    tablename = cfg["project"]["rss-feedsdb-table-items"]

    # Count rows in database table before insertion
    path = Path(dbpath)
    if Path(dbpath).is_file():
        conn = sqlite3.connect(str(path))
        rows_before = int(conn.execute(f"SELECT COUNT(*) FROM {tablename}").fetchone()[0])
        conn.close()
    else:
        rows_before = 0

    # Store feeds to database
    rss.feeds_to_database(urls, dbpath, tablename=tablename,
                          tags={"guid": "rss_guid", "link": "rss_link", "pubDate": "rss_pubdate",
                                "title": "rss_title", "description": "rss_description"},
                          keys=["rss_guid", "rss_link"])

    # Count rows in database table after insertion
    conn = sqlite3.connect(str(path))
    rows_after = int(conn.execute(f"SELECT COUNT(*) FROM {tablename}").fetchone()[0])
    conn.close()

    return rows_after - rows_before


if __name__ == "__main__":
    #
    # Parse command-line arguments
    #
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbosity", action="count", help="Enable verbose output", default=0)
    subparsers = parser.add_subparsers(help="command help", dest="command")

    # configure history subcommand
    parser_fetch_rss_feeds = subparsers.add_parser("rss-fetch")

    args = parser.parse_args()

    # Configure logging. Affects code in all child modules. Logs are always
    # stored at full maximum information in the logfile, but the log level may
    # be selectively selected for terminal output.
    cfg = configparser.ConfigParser(inline_comment_prefixes=";")
    cfg.read("setup.cfg")
    logdir = Path(cfg["project"]["logdir"])
    if not logdir.is_dir():
        logdir.mkdir(parents=True, exist_ok=True)
    logfilename = str(datetime.now().strftime(r"%Y-%m-%d")) + ".log"
    logpath = str(logdir / logfilename)  # log-directory/YYYY-MM-DD.log; one logfile per day
    logger_file = logging.FileHandler(logpath)
    file_formatter = logging.Formatter('[%(levelname)s] ⌚ %(asctime)s %(funcName)s: %(message)s')
    logger_file.setFormatter(file_formatter)
    logger_file.setLevel(logging.DEBUG)
    terminal_formatter = logging.Formatter('[%(levelname)s] %(funcName)s: %(message)s')
    logger_terminal = logging.StreamHandler()
    logger_terminal.setFormatter(file_formatter)
    if args.verbosity == 0:
        logger_terminal.setLevel(logging.WARNING)
    elif args.verbosity == 1:
        logger_terminal.setLevel(logging.INFO)
    else:
        logger_terminal.setLevel(logging.DEBUG)
    root_logger = logging.getLogger()
    root_logger.addHandler(logger_file)
    root_logger.addHandler(logger_terminal)
    root_logger.setLevel(logging.DEBUG)

    #
    # Run selected command
    #
    if args.command == "rss-fetch":
        rows_created = rss_fetch()
        logging.info(f"Generated {rows_created} new RSS record(s).")
