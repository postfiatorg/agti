import asyncio
import datetime
import pytz
import threading
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

class TaskScheduler:
    """
    A class to schedule and manage tasks to be run at specific times and days of the week.

    Attributes:
        stop_event (threading.Event): Event to signal stopping of tasks.
        tasks_list (list): List to keep track of tasks.


        # Example custom function
        def my_custom_function():
            global a
            a += 1
            logging.info(f"My custom function executed. a = {a}")
        # Example usage
        a = 0  # Initialize variable a
        scheduler = TaskScheduler()
        days = ["Monday", "Tuesday", "Wednesday"]
        times = ["06:00", "18:00"]
        scheduler.schedule_tasks_for_days_and_times(my_custom_function, "update_sec_data", days, times)
        scheduler.list_running_tasks()
        
        # Adding new tasks without deleting the old ones
        days = ["Thursday", "Friday"]
        times = ["07:00", "20:00"]
        scheduler.schedule_tasks_for_days_and_times(my_custom_function, "update_sec_data__nex", days, times)
        scheduler.list_running_tasks()

    """

    def __init__(self):
        self.stop_event = threading.Event()
        self.tasks_list = []
        self.loop = None
        self.thread = None
        self.tasks = []

    async def task(self, custom_function, run_day, run_time, task_name):
        """
        An asynchronous task that runs a custom function at the specified day and time.

        Args:
            custom_function (function): The custom function to be executed.
            run_day (str): The day of the week the task should run.
            run_time (str): The time the task should run in HH:MM format.
            task_name (str): The name of the task.
        """
        est = pytz.timezone('US/Eastern')
        self.tasks_list.append(task_name)
        try:
            while not self.stop_event.is_set():
                now = datetime.datetime.now(est)
                current_day = now.strftime("%A")
                current_time = now.strftime("%H:%M")
                if current_day == run_day and current_time == run_time:
                    custom_function()
                    await asyncio.sleep(60)  # Ensure the task does not run again in the same minute
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            logging.info(f'Task {task_name} cancelled.')
        finally:
            logging.info(f'Task {task_name} executed.')

    async def run_schedule(self):
        """
        Run the scheduled tasks concurrently.
        """
        await asyncio.gather(*self.tasks)

    def start_async_loop(self):
        """
        Start the asyncio loop in a separate thread.
        """
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self.run_schedule())
        except asyncio.CancelledError:
            pass
        finally:
            self.loop.close()

    def schedule_tasks(self, custom_function, task_details):
        """
        Schedule tasks and start the asyncio loop.

        Args:
            custom_function (function): The custom function to be executed.
            task_details (list of tuples): Each tuple contains (task_name, run_day, run_time).
        """
        new_tasks = [
            self.task(custom_function, run_day, run_time, task_name)
            for task_name, run_day, run_time in task_details
        ]
        self.tasks.extend(new_tasks)
        if self.thread is None or not self.thread.is_alive():
            self.thread = threading.Thread(target=self.start_async_loop)
            self.thread.start()
        else:
            asyncio.run_coroutine_threadsafe(self.run_schedule(), self.loop)

        logging.info("Scheduling complete. Tasks will run at the specified times.")

    def schedule_tasks_for_days_and_times(self, custom_function, base_task_name, days, times):
        """
        Schedule tasks for given days and times.

        Args:
            custom_function (function): The custom function to be executed.
            base_task_name (str): The base name of the task.
            days (list of str): The days of the week the task should run.
            times (list of str): The times the task should run in HH:MM format.
        """
        task_details = [(f"{base_task_name} {time} {day}", day, time) for day in days for time in times]
        self.schedule_tasks(custom_function, task_details)

    def stop(self):
        """
        Stop the async loop and the running tasks.
        """
        logging.info("Stopping the tasks.")
        self.stop_event.set()
        if self.loop is not None:
            for task in asyncio.all_tasks(self.loop):
                task.cancel()
            self.loop.call_soon_threadsafe(self.loop.stop)
        if self.thread is not None:
            self.thread.join()  # Wait for the thread to finish

    def list_running_tasks(self):
        """
        List all currently running tasks.
        """
        logging.info(f"Running tasks: {self.tasks_list}")



