import subprocess, json, platform, sys, os, netifaces, getpass, psycopg2
from datetime import datetime
from celeryconfig import celery, env

# Add project paths
project_path = os.getcwd()
sys.path.append(project_path)

celery.config_from_object('celeryconfig')

# Spider settings mapping
def get_settings_module(spider_name):
    if spider_name.startswith('amz'):
        return 'amazon.settings'
    else:
        return 'cvs.settings'

def get_machine_info():
    machine_info = {}
    try:
        interfaces = netifaces.interfaces()
        for interface in interfaces:
            if interface == 'eno1':
                addresses = netifaces.ifaddresses(interface)
                if netifaces.AF_INET in addresses:
                    for addr in addresses[netifaces.AF_INET]:
                        ip_address = addr['addr']
                        machine_info["IP Address"] = ip_address
                        break
            else:
                machine_info["IP Address"] = "192.168.1.102"
                break
    except Exception as e:
        print(f"Error getting IP address: {e}")
        machine_info["IP Address"] = "Unknown" 

    try:
        username = getpass.getuser()
        machine_info["Username"] = username
    except Exception as e:
        print(f"Error getting username: {e}")
        machine_info["Username"] = os.getenv('USER', 'Unknown')

    try:
        system_info = {
            "System": platform.system(),
            "Node Name": platform.node(),
            "Release": platform.release(),
            "Version": platform.version(),
            "Machine": platform.machine(),
            "Processor": platform.processor()
        }
        machine_info["System Info"] = system_info
    except Exception as e:
        print(f"Error getting system information: {e}")
        machine_info["System Info"] = {}

    return machine_info

def update_task_logger_status(uuid, status, system_info=None):
    try:
        conn = psycopg2.connect(env['db'])
        cursor = conn.cursor()
        if system_info:
            cursor.execute(
                "UPDATE task_logger SET status = %s, system_info = %s WHERE id = %s",
                (status, json.dumps(system_info), uuid)
            )
        else:
            cursor.execute(
                "UPDATE task_logger SET status = %s WHERE id = %s",
                (status, uuid)
            )
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error updating task logger: {e}")

def build_scrapy_command(spider_name, task_urls, unique_id, celery_task_id, filename, task_type, settings_module):
    if task_type in ['amz_browsenodes', 'cvs_browsenodes']:
        # Command for browsenodes (without URLs)
        cmd = f'scrapy crawl {spider_name} -a task_id="{unique_id}" -a celery_id="{celery_task_id}" -o {filename} -s SETTINGS_MODULE={settings_module}'
    else:
        # Command for other tasks (with URLs)
        urls_str = ','.join(task_urls)
        cmd = f'scrapy crawl {spider_name} -a urls="{urls_str}" -a task_id="{unique_id}" -a celery_id="{celery_task_id}" -o {filename} -s SETTINGS_MODULE={settings_module}'
    return cmd

@celery.task(bind=True, name='tasks.run_spider')
def run_spider(self, uuid, task_urls, spider_name, task_type, priority, queue_name):
    try:
        print(f'Process Started for {spider_name} *****************************************')

        # if cron >= datetime.utcnow():
        #     print(f'Task is scheduled for future, skipping execution.')
        
        update_task_logger_status(uuid, 'IN_PROGRESS')

        unique_id = uuid
        celery_task_id = self.request.id
        timestamp = datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')                                       

        # Ensure target directory exists
        new_dir = os.path.join(os.getcwd(), 'data')
        if not os.path.exists(new_dir):
            os.mkdir(new_dir)

        filename = os.path.join(new_dir, f'{spider_name}__{unique_id}__{celery_task_id}__{timestamp}.csv')

        # Determine the settings module
        settings_module = get_settings_module(spider_name)

        # Build the scrapy command
        cmd = build_scrapy_command(spider_name, task_urls, unique_id, celery_task_id, filename, task_type, settings_module)

        # Ensure the working directory is set to the project path
        project_path = os.path.dirname(os.path.abspath(__file__))
        os.chdir(project_path)
        
        # Set the PYTHONPATH environment variable
        env = os.environ.copy()
        env['PYTHONPATH'] = project_path

        # Set the SCRAPY_SETTINGS_MODULE environment variable
        env['SCRAPY_SETTINGS_MODULE'] = settings_module

        print(f'Running command: {cmd}')
        result = subprocess.call(cmd, shell=True, env=env)

        # Check the result of the subprocess call
        if result != 0:
            raise Exception(f'Command failed with exit code {result}')

        system_info = get_machine_info()
        update_task_logger_status(uuid, 'COMPLETED', system_info)

        print(f'Process End for {spider_name} *****************************************')
        print(f'Output CSV: {filename}')
        return filename
    except Exception as e:
        print(f'Error in running spider {spider_name}: {str(e)}')
        update_task_logger_status(uuid, 'FAILED')
        
class Publisher:
    def publish_task(self, spider_name, task_type, priority):
        try:
            conn = psycopg2.connect(env['db'])
            cursor = conn.cursor()
            cursor.execute("SELECT id, task_urls, start_date, functionality FROM task_logger WHERE status = 'PENDING' AND task_name = %s", (spider_name,))
            print(f'Pending tasks for {spider_name}: {cursor.rowcount}')
            pending_tasks = cursor.fetchall()

            if not pending_tasks:
                print(f'No pending tasks for {spider_name}.')
                return      
            
             # Determine the queue name based on the spider name
            if spider_name.startswith('amz'):
                queue_name = 'amazon'
            elif spider_name.startswith('cvs'):
                queue_name = 'cvs'
            else:
                queue_name = 'default'
            
            for task in pending_tasks:
                uuid, task_urls, start_date, functionality= task  
                task_urls = task_urls
                cron_time = functionality.get('cron_time')

                if start_date == datetime.now().date() and cron_time <= datetime.now().strftime('%H:%M'): 
                
                    result = run_spider.apply_async(
                            args=[str(uuid), task_urls, spider_name, task_type, priority, queue_name],
                            queue=queue_name
                        )
                    result_id = result.id

                    # Update task status and store Celery task ID
                    cursor.execute(
                        "UPDATE task_logger SET status = %s, celery_task_id = %s WHERE id = %s",
                        ('PUBLISHED', result_id, uuid)
                    )
                    print(f'Starting task {result_id} for spider {spider_name} on queue {queue_name}')
                    conn.commit()

                else:
                    print(f'Task {uuid} for spider {spider_name} is not due yet.')

            cursor.close()
            conn.close()

        except Exception as e:
            print(f'Error occurred: {e}')
            if conn:
                conn.rollback()
            if cursor:
                cursor.close()
            if conn:
                conn.close()

# Publishing task for beat 
@celery.task(name='tasks.publish_scraper_task')
def publish_scraper_task(spider_name, task_type,priority):
    publisher = Publisher()
    publisher.publish_task(spider_name, task_type, priority)
