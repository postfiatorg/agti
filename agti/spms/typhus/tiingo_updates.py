from agti.utilities.data_update_details import DataUpdateDetails
import pandas as pd
from agti.data.tiingo.equities import TiingoDataTool
from agti.utilities.scheduler import TaskScheduler
class TyphusTiingoUpdate:
    def __init__(self,pw_map):
        self.pw_map= pw_map
        self.task_scheduler =  TaskScheduler()
        self.tiingo_data_tool = TiingoDataTool(pw_map=pw_map)
    def run_full_tiingo_update_and_update_node(self):
        user_name ='spm_typhus'
        table_to_work = 'tickers'
        self.tiingo_data_tool.update_all_stale_tiingo_data()
        data_update_details = DataUpdateDetails(pw_map=self.pw_map)
        
        db_table_ref ='tiingo__equities'
        data_update_details.update_node_on_user_data_update(user_name='spm_typhus',
            node_name='agti_corp',
            task_id='2024-05-29_20:00__TK37',
            full_evidence_url='https://github.com/postfiatorg/agti/blob/main/agti/data/tiingo/equities.py',
            date_column='date',                                     
            db_table_ref=db_table_ref)
        print(f"DID Tiingo Equity Update")

    def schedule_tiingo_update(self):
        """
        Schedules the run_full_sharadar_update_and_update_node function to run at 11:59 PM
        on Monday, Tuesday, Wednesday, Thursday, and Friday.
        """
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
        times = ["23:00"]
        self.task_scheduler.schedule_tasks_for_days_and_times(self.run_full_tiingo_update_and_update_node, 
                                                              "run_full_tiingo_update_and_update_node", 
                                                              days, 
                                                              times)
        