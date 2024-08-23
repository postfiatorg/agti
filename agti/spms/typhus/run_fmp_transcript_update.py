
from agti.utilities.data_update_details import DataUpdateDetails
from agti.data.fmp.transcripts import FMPDataTool
from agti.utilities.scheduler import TaskScheduler
import datetime

class FMPDataUpdateScheduler:
    def __init__(self, pw_map):
        self.pw_map = pw_map
        self.fmp_data_tool = FMPDataTool(pw_map=self.pw_map)
        self.task_scheduler = TaskScheduler()
        self.data_update_details = DataUpdateDetails(pw_map=self.pw_map)

    def run_fmp_update_and_update_node(self):
        """Run FMP data update and update the node."""
        self.fmp_data_tool.write_full_fmp_history_for_x_years()
        
        self.data_update_details.update_node_on_user_data_update(
            user_name='spm_typhus',
            node_name='agti_corp',
            task_id=f'{datetime.datetime.now().strftime("%Y-%m-%d_%H:%M")}__FMP_UPDATE',
            full_evidence_url='https://github.com/postfiatorg/agti/blob/main/agti/data/fmp/transcripts',
            date_column='simple_date',
            db_table_ref='fmp___earnings_call_transcripts'
        )
        print("FMP data update completed and node updated.")

    def schedule_fmp_updates(self):
        """Schedule FMP data update for specified days and times."""
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        times = ["18:00", "20:00", "23:00", "01:00", "06:00","8:00","8:45"]
        self.task_scheduler.schedule_tasks_for_days_and_times(
            self.run_fmp_update_and_update_node, 
            "run_fmp_update_and_update_node", 
            days, 
            times
        )