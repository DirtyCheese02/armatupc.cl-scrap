from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRAPER_DIR = ROOT / "ScrapDB" / "PythonsScrap"
sys.path.insert(0, str(SCRAPER_DIR))

import Scrap_PCExpress as pcexpress


LISTING_HTML = """
<div class="product-list">
  <div class="product-list__content row">
    <div class="product-list__item">
      <div class="product-list__image">
        <a href="/123-procesador-demo"><img src="/demo.webp"></a>
      </div>
      <p class="product-list__manufacturer">AMD</p>
      <h5 class="product-list__name">Procesador demo P/N 100-TEST-BOX</h5>
      <p class="product-list__price">$129.990</p>
    </div>
  </div>
</div>
<ul class="pagination"><li>1</li><li>2</li></ul>
"""

DETAIL_HTML = """
<h1 class="rm-product-page__title">Procesador AMD demo P/N 100-TEST-BOX</h1>
<a href="index.php?route=product/manufacturer/info&manufacturer_id=1">AMD</a>
<div class="rm-product-page__prices"><h3 class="text-primary">$129.990</h3></div>
<img data-zoom-image="https://img.example/demo.webp" title="demo">
<div>Stock web <span>Disponible</span></div>
"""


class PCExpressScraperTests(unittest.TestCase):
    def test_listing_uses_public_html_and_pagination(self):
        products, pages = pcexpress.parse_listing(LISTING_HTML, "CPU")
        self.assertEqual(pages, 2)
        self.assertEqual(products[0]["url"], "https://tienda.pc-express.cl/123-procesador-demo")
        self.assertEqual(products[0]["price"], "129990")

    def test_detail_extracts_mpn_price_and_stock(self):
        product = {
            "type": "CPU",
            "url": "https://tienda.pc-express.cl/123-procesador-demo",
            "name": "Demo",
            "brand": "AMD",
            "price": "129990",
            "image_url": "",
        }
        result = pcexpress.parse_product_detail(DETAIL_HTML, product)
        self.assertIsNotNone(result)
        self.assertEqual(result["part #"], "100-TEST-BOX")
        self.assertEqual(result["price"], "129990")
        self.assertEqual(result["availability"], "available")


if __name__ == "__main__":
    unittest.main()
