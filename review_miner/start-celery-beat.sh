#!/bin/bash

# Start Celery beat for Amazon
/home/mediaamp-main/globalscraper/scrapyenv/bin/celery -A tasks.celery beat --schedule=/home/mediaamp-main/globalscraper/dsiqglobalscraper/globalscraper/celerybeat-schedule-amazon --loglevel=info &

# Start Celery beat for Walmart
/home/mediaamp-main/globalscraper/scrapyenv/bin/celery -A tasks.celery beat --schedule=/home/mediaamp-main/globalscraper/dsiqglobalscraper/globalscraper/celerybeat-schedule-walmart --loglevel=info &

