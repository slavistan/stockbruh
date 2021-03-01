"""Main modules."""
import configparser

from src import rss


def rss_stuff():
    cfg = configparser.ConfigParser()
    cfg.read("setup.cfg")
    urls = [item for (key, item) in cfg.items("rss-feeds")]
    dbpath = cfg["project"]["rss-db-path"]
    tablename = cfg["project"]["rss-db-table-items"]
    rss.archive_rss_feed(urls, dbpath, tablename=tablename)


if __name__ == "__main__":
    rss_stuff()
