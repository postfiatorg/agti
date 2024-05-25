import threading
from agti.utilities.user_tools.user_cron_job import CronUpdateWrapper
import schedule
import time
from agti.data.sec_methods.update_cik import RunCIKUpdate
from agti.data.sec_methods.recent_data_batch_load import SECRecentDataBatchLoad
from agti.utilities.settings import PasswordMapLoader

class TyphusUpdate:
    def __init__(self, pw_map):
        ## REFERENCE TASK ID 2024-05-23_12:31__YY23
        self.pw_map = pw_map
        self.user_name = 'spm_typhus'
        self.cron_update_wrapper = CronUpdateWrapper(pw_map=pw_map)
        self.cik_update = RunCIKUpdate(pw_map=pw_map, user_name=self.user_name)
        self.sec_recent_data_batch_load = SECRecentDataBatchLoad(pw_map=pw_map, user_name=self.user_name)
        self.cron_thread = None
        self.run_count = 0  # To keep track of the number of runs

    ## STEP 1
    def update_cik_and_output_cik_df(self):
        print("Running update_cik_and_output_cik_df function...")
        self.cik_update.write_cik_df()
        cached_cik_df = self.cik_update.output_cached_cik_df()
        print('CACHED CIK DF')
        print(cached_cik_df)
        self.run_count += 1
        return cached_cik_df

    def start_cik_cron_job(self):
        """ This updates the CIK data for ticker references in the SEC """ 
        def job_wrapper():
            print("Executing time-triggered job wrapper...")
            self.cron_update_wrapper.schedule_time_triggered_cron_job(
                user_function=self.update_cik_and_output_cik_df,
                user_name=self.user_name, 
                task_id='2024-05-23_12:31__YY23', 
                evidence_url='https://github.com/postfiatorg/agti/blob/main/data/sec_methods/update_cik.py', 
                node_address='rKZDcpzRE5hxPUvTQ9S3y2aLBUUTECr1vN', 
                days=['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'],
                times=['3:00 PM']
            )
            print("Scheduled time-triggered job wrapper executed.")

        self.cron_thread = threading.Thread(target=job_wrapper)
        self.cron_thread.start()

    def stop_cik_cron_job(self):
        """ this stops the CIK cron job"""
        if self.cron_thread and self.cron_thread.is_alive():
            print("Stopping cron job...")
            self.cron_update_wrapper.stop_cron_job()
            self.cron_thread.join()
            print("Cron job stopped.")

    def test_cik_cron_job(self):
        def job_wrapper():
            print("Executing test job wrapper...")
            self.cron_update_wrapper.schedule_periodic_cron_job(
                user_function=self.update_cik_and_output_cik_df,
                user_name=self.user_name, 
                task_id='2024-05-23_12:31__YY23', 
                evidence_url='https://github.com/postfiatorg/agti/blob/main/data/sec_methods/update_cik.py', 
                node_address='rKZDcpzRE5hxPUvTQ9S3y2aLBUUTECr1vN', 
                interval_minutes=2
            )
            print("Scheduled test job wrapper executed.")

        self.cron_thread = threading.Thread(target=job_wrapper)
        self.cron_thread.start()

        def stop_test_job():
            print("Stopping test job after 10 minutes...")
            self.stop_cik_cron_job()
            completion_score = (self.run_count / 5) * 100
            print(f"Test job completion score: {completion_score}%")
            self.run_count = 0  # Reset the run count

        # Schedule the stop_test_job function to run after 10 minutes
        threading.Timer(600, stop_test_job).start()

    ## STEP 2
    def update_recent_sec_data_batch_load_and_output_recent(self):
        print("Running 3 hour batch sec update function...")
        
        self.sec_recent_data_batch_load.run_sec_data_batch_loadfor_3_hours()
        cached_batch_data_df = self.sec_recent_data_batch_load.load_cached_sec_updates()
        print('CACHED BATCH DATA DF')
        print(cached_batch_data_df)
        self.run_count += 1
        return cached_batch_data_df

    def start_recent_data_batch_load_cron_job(self):
        """ This updates the CIK data for ticker references in the SEC """ 
        def job_wrapper():
            print("Executing time-triggered job wrapper...")
            self.cron_update_wrapper.schedule_time_triggered_cron_job(
                user_function=self.update_recent_sec_data_batch_load_and_output_recent,
                user_name=self.user_name, 
                task_id='2024-05-23_12:31__YY23', 
                evidence_url='https://github.com/postfiatorg/agti/blob/main/data/sec_methods/recent_data_batch_load.py', 
                node_address='rKZDcpzRE5hxPUvTQ9S3y2aLBUUTECr1vN', 
                days=['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'],
                times=['7 AM', '3 PM']
            )
            print("Scheduled time-triggered job wrapper executed.")

        self.cron_thread = threading.Thread(target=job_wrapper)
        self.cron_thread.start()

    def stop_recent_data_batch_load_cron_job(self):
        """ this stops the CIK cron job"""
        if self.cron_thread and self.cron_thread.is_alive():
            print("Stopping cron job...")
            self.cron_update_wrapper.stop_cron_job()
            self.cron_thread.join()
            print("Cron job stopped.")

    def test_recent_data_batch_load_cron_job(self):
        def job_wrapper():
            print("Executing test job wrapper...")
            self.cron_update_wrapper.schedule_periodic_cron_job(
                user_function=self.update_recent_sec_data_batch_load_and_output_recent,
                user_name=self.user_name, 
                task_id='2024-05-23_12:31__YY23', 
                evidence_url='https://github.com/postfiatorg/agti/blob/main/data/sec_methods/recent_data_batch_load.py', 
                node_address='rKZDcpzRE5hxPUvTQ9S3y2aLBUUTECr1vN', 
                interval_minutes=2
            )
            print("Scheduled test job wrapper executed.")

        self.cron_thread = threading.Thread(target=job_wrapper)
        self.cron_thread.start()

        def stop_test_job():
            print("Stopping test job after 10 minutes...")
            self.stop_recent_data_batch_load_cron_job()
            completion_score = (self.run_count / 5) * 100
            print(f"Test job completion score: {completion_score}%")
            self.run_count = 0  # Reset the run count

        # Schedule the stop_test_job function to run after 10 minutes
        threading.Timer(600, stop_test_job).start()