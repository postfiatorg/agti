from agti.utilities.data_update_details import DataUpdateDetails
from agti.data.sharadar.sharadar_bulk_update import SharadarDataUpdate
import pandas as pd
from agti.utilities.scheduler import TaskScheduler
class TyphusSharadarUpdate:
    def __init__(self,pw_map):
        self.pw_map= pw_map
        self.task_scheduler =  TaskScheduler()
        
    def run_full_sharadar_update_and_update_node(self):
        user_name ='spm_typhus'
        table_to_work = 'tickers'
        sharadar_update = SharadarDataUpdate(pw_map=self.pw_map, user_name=user_name)
        data_update_details = DataUpdateDetails(pw_map=self.pw_map)
        
        sharadar_update.kick_off_sharadar_table_bulk_load(table_to_load=table_to_work)
        db_table_ref ='sharadar__'+table_to_work
        data_update_details.update_node_on_user_data_update(user_name='spm_typhus',
            node_name='agti_corp',
            task_id='2024-05-29_20:00__TK37',
            full_evidence_url='https://github.com/postfiatorg/agti/blob/main/agti/data/sharadar/sharadar_bulk_update.py',
            date_column='lastpricedate',                                     
            db_table_ref=db_table_ref)
        print(f"DID {db_table_ref}")
        ## sep 
        table_to_work = 'sep'
        sharadar_update.kick_off_sharadar_table_bulk_load(table_to_load=table_to_work)
        db_table_ref ='sharadar__'+table_to_work
        data_update_details.update_node_on_user_data_update(user_name='spm_typhus',
            node_name='agti_corp',
            task_id='2024-05-29_20:00__TK37',
            full_evidence_url='https://github.com/postfiatorg/agti/blob/main/agti/data/sharadar/sharadar_bulk_update.py',
            date_column='date',                                     
            db_table_ref=db_table_ref)
        print(f"DID {db_table_ref}")
        ## daily
        table_to_work = 'daily'
        sharadar_update.kick_off_sharadar_table_bulk_load(table_to_load=table_to_work)
        db_table_ref ='sharadar__'+table_to_work
        data_update_details.update_node_on_user_data_update(user_name='spm_typhus',
            node_name='agti_corp',
            task_id='2024-05-29_20:00__TK37',
            full_evidence_url='https://github.com/postfiatorg/agti/blob/main/agti/data/sharadar/sharadar_bulk_update.py',
            date_column='date',                                     
            db_table_ref=db_table_ref)
        print(f"DID {db_table_ref}")
        table_to_work = 'sf3'
        sharadar_update.kick_off_sharadar_table_bulk_load(table_to_load=table_to_work)
        db_table_ref ='sharadar__'+table_to_work
        data_update_details.update_node_on_user_data_update(user_name='spm_typhus',
            node_name='agti_corp',
            task_id='2024-05-29_20:00__TK37',
            full_evidence_url='https://github.com/postfiatorg/agti/blob/main/agti/data/sharadar/sharadar_bulk_update.py',
            date_column='calendardate',                                     
            db_table_ref=db_table_ref)
        print(f"DID {db_table_ref}")
        table_to_work = 'sf1'
        sharadar_update.kick_off_sharadar_table_bulk_load(table_to_load=table_to_work)
        db_table_ref ='sharadar__'+table_to_work

        data_update_details.update_node_on_user_data_update(user_name='spm_typhus',
            node_name='agti_corp',
            task_id='2024-05-29_20:00__TK37',
            full_evidence_url='https://github.com/postfiatorg/agti/blob/main/agti/data/sharadar/sharadar_bulk_update.py',
            date_column='datekey',                                     
            db_table_ref=db_table_ref)
        print(f"DID {db_table_ref}")

    def schedule_sharadar_update(self):
        """
        Schedules the run_full_sharadar_update_and_update_node function to run at 11:59 PM
        on Monday, Tuesday, Wednesday, Thursday, and Friday.
        """
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        times = ["23:59"]
        self.task_scheduler.schedule_tasks_for_days_and_times(self.run_full_sharadar_update_and_update_node, 
                                                              "run_full_sharadar_update_and_update_node", 
                                                              days, 
                                                              times)
        