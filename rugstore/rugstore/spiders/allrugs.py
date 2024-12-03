import scrapy


class AllrugsSpider(scrapy.Spider):
    name = "allrugs"
    allowed_domains = ["therugshopuk.co.uk"]
    start_urls = ["https://www.therugshopuk.co.uk/rugs-by-type/rugs.html"]

    def parse(self, response):
        for item in response.css("div.product-item-info"):
            yield {
                "title": item.css("img.product-image-photo.image::attr(alt)").get(),
                "link": item.css("a.product-item-link::attr(href)").get(),
                "price": item.css("span.price::text").get(),
            }
        next_page = response.css("a[title=Next]::attr(href)").get()
        if (next_page) is not None:
            yield response.follow(next_page, callback=self.parse)
