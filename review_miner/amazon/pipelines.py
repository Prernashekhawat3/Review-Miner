# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import json

class JsonExportPipeline:
    def open_spider(self, spider):
        # Initialize the structure
        self.data = {
            "products": [],
            "sponsored_ads": []
        }

    def process_item(self, item, spider):
        # Add items to the respective category
        if item["category"] == "products":
            self.data["products"].append(item)
        elif item["category"] == "sponsored_ads":
            self.data["sponsored_ads"].append(item)
        return item

    def close_spider(self, spider):
        # Write the grouped data to a JSON file
        with open("output.json", "w") as f:
            json.dump(self.data, f, indent=4)
