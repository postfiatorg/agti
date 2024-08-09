from agti.data.sec_methods.update_cik import RunCIKUpdate
from agti.data.sec_methods.recent_data_batch_load import SECRecentDataBatchLoad
from agti.utilities.data_update_details import DataUpdateDetails
from agti.data.sec_methods.sec_filing_update import SECFilingUpdateManager
from agti.utilities.scheduler import TaskScheduler
class TyphusSECManager:
    def __init__(self,pw_map):
        self.pw_map = pw_map
        self.cik_update = RunCIKUpdate(pw_map=self.pw_map, user_name='spm_typhus')
        self.sec_recent_data_batch_load = SECRecentDataBatchLoad(pw_map=self.pw_map, user_name='spm_typhus')
        self.data_update_details = DataUpdateDetails(pw_map=self.pw_map)
        self.sec_filing_update_manager = SECFilingUpdateManager(pw_map=self.pw_map, user_name='spm_typhus')
        self.task_scheduler = TaskScheduler()
    def run_full_sec_update(self):
        try:
            table_to_work='update_cik'
            db_table_ref ='sec__'+table_to_work
            self.cik_update.write_cik_df()
            self.data_update_details.update_node_on_user_data_update(user_name='spm_typhus',
                node_name='agti_corp',
                task_id='2024-05-28_23:54__FJ37',
                full_evidence_url='https://github.com/postfiatorg/agti/blob/main/agti/data/sec_methods/update_cik.py',
                date_column='date_of_update',                                     
                db_table_ref=db_table_ref)
        except:
            print('failed CIK UPDATE')
            pass
        try:
            table_to_work='update_recent_filings'
            db_table_ref ='sec__'+table_to_work
            self.sec_recent_data_batch_load.write_recent_sec_updates()
            self.data_update_details.update_node_on_user_data_update(user_name='spm_typhus',
                node_name='agti_corp',
                task_id='2024-05-28_23:54__FJ37',
                full_evidence_url='https://github.com/postfiatorg/agti/blob/main/agti/data/sec_methods/recent_data_batch_load.py',
                date_column='full_datetime',                                     
                db_table_ref=db_table_ref)
            print('DID SEC Filing Update Recent Batch Load')
        except:
            print('failed SEC Filing Update Recent Batch Load')
            pass
        
        try:
            table_to_work='full_filing_details'
            db_table_ref ='sec__'+table_to_work
            self.sec_filing_update_manager.run_full_filing_update()
            self.data_update_details.update_node_on_user_data_update(user_name='spm_typhus',
                node_name='agti_corp',
                task_id='2024-05-28_23:54__FJ37',
                full_evidence_url='https://github.com/postfiatorg/agti/blob/main/agti/data/sec_methods/sec_filing_update.py',
                date_column='upload_date',                                     
                db_table_ref=db_table_ref)
            print('DID SEC Filing Update Full Filing Load')
        except:
            print('failed SEC Filing Update Recent Batch Load')
            pass

    def schedule_sec_update(self):
        """
        Schedules the run_full_sharadar_update_and_update_node function to run at 11:59 PM
        on Monday, Tuesday, Wednesday, Thursday, and Friday.
        """
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        times = ["7:00","7:15","7:30","7:40","7:50","8:00","8:10","8:20",
                "8:25","8:30","8:35","8:40","8:45","8:50","8:55","9:00",
                "9:10","9:15","9:20","9:30","13:20","16:00","16:05","16:10","16:15","16:20",
                "16:25","16:30","16:35","16:40","16:45","16:50","17:00","17:10","17:20","17:30","17:40","17:50","18:00",
                "18:30","19:00","20:00"]
        self.task_scheduler.schedule_tasks_for_days_and_times(self.run_full_sec_update, 
                                                              "run_full_sec_update", 
                                                              days, 
                                                              times)