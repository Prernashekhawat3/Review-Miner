from celery import Celery
from celery.schedules import crontab
from kombu import Queue, Exchange
import tasks
from config import environment

env = environment['server']
celery = Celery('tasks', broker=env['celery-broker'])

celery.conf.update(
    broker_url=env['celery-broker'],
    result_backend=env['celery-broker'],
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Kolkata',
    enable_utc=True,
    broker_connection_retry_on_startup=True,
    task_queues=[
            Queue('amazon', Exchange('amazon'),
                  routing_key='amazon'),
            Queue('cvs', Exchange('cvs'),
                  routing_key='cvs'),
        ],
    )

# Explicitly register tasks
celery.autodiscover_tasks(['tasks'])

def update_beat_schedule():
    tasks_info = [
        # ('amz_browsenodes', 'amz_browsenodes', 'amazon'),
        ('amazon_pdp', 'amz_pdp', 'amazon'),
        ('amz_listings', 'amz_listings', 'amazon'),
        ('cvs_listings', 'cvs_listings', 'cvs'),
        ('cvs_pdp', 'cvs_pdp', 'cvs'),
        # ('wmt_listings', 'wmt_listings', 'walmart'),
        # ('wmt_browsenodes', 'wmt_browsenodes', 'walmart'),
        # ('walmart_spider', 'wmt_pdp', 'walmart'),
        # ('amazon_spider', 'amz_reviews', 'amazon'),
        # ('walmart_spider', 'wmt_reviews', 'walmart')
    ]

    schedules = {}
    for spider_name, task_name, queue_name in tasks_info:
        schedules[task_name] = {
            'task': 'tasks.publish_scraper_task',
            'schedule': crontab(minute='*/5'),  # Every  5 minute
            'args': (spider_name, task_name, 'HIGH'),
            'options': {'queue': queue_name}
        } 
    
    # for spider_name, task_name, queue_name in tasks_info:
    #     if 'browsenodes' in task_name:
    #         schedules[task_name] = {  
    #             'task': 'tasks.publish_scraper_task',
    #             'schedule': crontab(minute=30, hour=10),  #Everyday
    #             'args': (spider_name, task_name, 'HIGH'),
    #             'options': {'queue': queue_name}
    #         }
    #     elif 'listings' in task_name:
    #         schedules[task_name] = {
    #             'task': 'tasks.publish_scraper_task',   
    #             'schedule': crontab(minute=30, hour=11), 
    #             'args': (spider_name, task_name, 'HIGH'),
    #             'options': {'queue': queue_name}
    #         }

    # Update the Celery beat schedule
    celery.conf.beat_schedule = schedules

# Call the function to set the schedule, possibly in app startup or configuration
update_beat_schedule()
