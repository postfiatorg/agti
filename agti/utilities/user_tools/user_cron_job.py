import threading
from utilities.generic_pft_utilities import GenericPFTUtilities
import zlib
import base64
import pandas as pd
from io import StringIO
import schedule
import time
import pytz
from datetime import datetime

class CronUpdateWrapper:
    def __init__(self, pw_map):
        self.generic_pft_utilities = GenericPFTUtilities(pw_map)
        self.stop_event = threading.Event()
        self.time_zone = pytz.timezone('US/Eastern')

    def create_standardized_cron_df_update_memo(self, user_name, task_id, evidence_url, last_x_rc_str):
        shortened_url = self.generic_pft_utilities.shorten_url(evidence_url)
        cron_statement = f'CRON EVIDENCE ___ DF Evidence URL:: {shortened_url} | Last X RC :: {last_x_rc_str}'
        output_memo = self.generic_pft_utilities.construct_basic_postfiat_memo(user=user_name, 
                                                                               task_id=task_id,
                                                                               full_output=cron_statement)
        return output_memo

    def convert_df_tail_to_compressed_string(self, input_df, compression_rows=2):
        simplified_op = input_df.tail(compression_rows).transpose().head(compression_rows).transpose()
        df = simplified_op.set_index(simplified_op.columns[0])
        csv_string = df.to_csv(index=True, sep='|', header=True)
        compressed_string = zlib.compress(csv_string.encode('utf-8'))
        return compressed_string

    def reconvert_compressed_string_to_df(self, compressed_string):
        decompressed_string = zlib.decompress(compressed_string).decode('utf-8')
        df = pd.read_csv(StringIO(decompressed_string), sep='|', header=0, index_col=0)
        return df

    def convert_compressed_string_to_non_byte_string(self, compressed_string):
        base64_string = base64.b64encode(compressed_string).decode('utf-8')
        return base64_string

    def convert_non_byte_string_to_df(self, non_byte_string):
        compressed_string = base64.b64decode(non_byte_string)
        decompressed_string = zlib.decompress(compressed_string).decode('utf-8')
        df = pd.read_csv(StringIO(decompressed_string), sep='|', header=0, index_col=0)
        return df

    def send_standardized_cron_df_update(self, user_name, task_id, evidence_url, last_x_rc_str, destination_address):
        sending_wallet = self.generic_pft_utilities.spawn_user_wallet_based_on_name(user_name=user_name)
        cron_memo = self.create_standardized_cron_df_update_memo(user_name=user_name, 
                                                                 task_id=task_id, 
                                                                 evidence_url=evidence_url, 
                                                                 last_x_rc_str=last_x_rc_str)
        resp = self.generic_pft_utilities.send_PFT_with_info(sending_wallet=sending_wallet, amount=1, memo=cron_memo,
                                                             destination_address=destination_address)
        print(f'{user_name} EXECUTED {task_id} at {time.strftime("%Y-%m-%d %H:%M:%S")}')
        print(resp)
        return resp

    def schedule_periodic_cron_job(self, user_function, user_name, task_id, evidence_url, node_address, interval_minutes=60):
        def job():
            print(f"Executing scheduled job at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            df = user_function()
            compressed_string = self.convert_df_tail_to_compressed_string(df)
            non_byte_string = self.convert_compressed_string_to_non_byte_string(compressed_string)
            self.send_standardized_cron_df_update(user_name, task_id, evidence_url, non_byte_string, node_address)
            print(f"Cron job executed and update sent at {time.strftime('%Y-%m-%d %H:%M:%S')}")

        schedule.every(interval_minutes).minutes.do(job)

        print(f"Cron job scheduled to run every {interval_minutes} minutes.")

        while not self.stop_event.is_set():
            schedule.run_pending()
            time.sleep(1)

        print("Cron job stopped.")

    def schedule_time_triggered_cron_job(self, user_function, user_name, task_id, evidence_url, node_address, days, times):
        def job():
            print(f"Executing time-triggered job at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            df = user_function()
            compressed_string = self.convert_df_tail_to_compressed_string(df)
            non_byte_string = self.convert_compressed_string_to_non_byte_string(compressed_string)
            self.send_standardized_cron_df_update(user_name, task_id, evidence_url, non_byte_string, node_address)
            print(f"Time-triggered job executed and update sent at {time.strftime('%Y-%m-%d %H:%M:%S')}")

        for day in days:
            for time_str in times:
                time_obj = datetime.strptime(time_str, '%I:%M %p')
                time_str_24h = time_obj.strftime('%H:%M')
                getattr(schedule.every(), day.lower()).at(time_str_24h).do(job)
                print(f"Scheduled job for {day} at {time_str} EST")

        while not self.stop_event.is_set():
            schedule.run_pending()
            time.sleep(1)

        print("Cron job stopped.")

    def stop_cron_job(self):
        self.stop_event.set()