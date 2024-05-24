import threading
from agti.utilities.user_tools.user_cron_job import CronUpdateWrapper
import schedule
import time
from agti.data.sec_methods.update_cik import RunCIKUpdate

class TyphusUpdate:
    def __init__(self, pw_map):
        ## REFERENCE TASK ID 2024-05-23_12:31__YY23
        self.pw_map = pw_map
        self.user_name = 'spm_typhus'
        self.cron_update_wrapper = CronUpdateWrapper(pw_map=pw_map)
        self.cik_update = RunCIKUpdate(pw_map=pw_map, user_name=self.user_name)
        self.cron_thread = None
        self.run_count = 0  # To keep track of the number of runs

    def update_cik_and_output_cik_df(self):
        print("Running update_cik_and_output_cik_df function...")
        self.cik_update.write_cik_df()
        cached_cik_df = self.cik_update.output_cached_cik_df()
        print('CACHED CIK DF')
        print(cached_cik_df)
        self.run_count += 1
        return cached_cik_df

    def start_cik_cron_job(self):
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