from src import rss

def test_rss_trace_link():
    # Appetizer preview
    link = "https://www.finanznachrichten.de/nachrichten-2021-03/52172551-chart-check-itm-power-diese-marke-muss-heute-halten-124.htm"
    dest = "https://www.deraktionaer.de/artikel/aktien/chart-check-itm-power-diese-marke-muss-heute-halten-20226666.html?feed=TRtvHrugxEKV2n-qR2P-ag"
    assert rss.rss_trace_link(link) == dest

    # Direct content
    link = "https://www.finanznachrichten.de/nachrichten-2021-03/52158803-opening-bell-tripadvisor-alibaba-bilibili-johnson-johnson-plug-paypal-fuelcell-tesla-nio-398.htm"
    dest = link
    assert rss.rss_trace_link(link) == dest

    link = "https://www.finanznachrichten.de/nachrichten-2021-03/52206697-curevac-neues-kursziel-aktiviert-441.htm"
    dest = "https://www.start-trading.de/2021/03/05/curevac-neues-kursziel-aktiviert/"
    assert rss.rss_trace_link(link) == dest