import time
from datetime import datetime, timedelta
from agti.utilities.data_update_details import DataUpdateDetails
from agti.utilities.google_sheet_manager import GoogleSheetManager
import pandas as pd

class ETLUpdateManager:
    def __init__(self, pw_map, prod_trading=False):
        """
        Initialize the ETL Update Manager.
        
        Args:
            pw_map: Password map containing necessary credentials
            prod_trading: Boolean to determine which Google credentials to use
        """
        self.pw_map = pw_map
        self.data_update_details = DataUpdateDetails(pw_map=pw_map)
        self.sheet_manager = GoogleSheetManager(prod_trading=prod_trading)
        self.last_update_count = 0
        
    def get_current_updates(self, days=1):
        """Get current updates from database."""
        return self.data_update_details.get_all_existing_updates(ndays=days)
        
    def update_sheet_if_changed(self, days=1):
        """
        Update the 'corbanu_etl' worksheet in the 'odv' workbook if there are new updates.
        
        Args:
            days: Number of days of updates to consider (default: 1)
        """
        try:
            # Get current updates from database
            updates_df = self.get_current_updates(days=days)
            current_update_count = len(updates_df)
            
            # Check if there are new updates
            if current_update_count > self.last_update_count:
                print(f"Found {current_update_count - self.last_update_count} new updates. Updating sheet...")
                
                # Ensure the worksheet exists
                self.sheet_manager.create_worksheet_if_does_not_exist('odv', 'corbanu_etl')
                
                # Write the updated dataframe to the sheet
                self.sheet_manager.write_dataframe_to_sheet('odv', 'corbanu_etl', updates_df)
                
                # Update the last update count
                self.last_update_count = current_update_count
                
                print(f"Successfully updated sheet at {datetime.now()}")
                return True
            else:
                print(f"No new updates found at {datetime.now()}")
                return False
            
        except Exception as e:
            print(f"Error updating sheet: {e}")
            return False

    def monitor_and_update(self, check_interval=60, days=1):
        """
        Continuously monitor for updates and update sheet when changes are detected.
        
        Args:
            check_interval: Time between checks in seconds (default: 60)
            days: Number of days of updates to consider (default: 1)
        """
        print(f"Starting ETL update monitor. Checking every {check_interval} seconds...")
        
        while True:
            try:
                self.update_sheet_if_changed(days=days)
                time.sleep(check_interval)
            except KeyboardInterrupt:
                print("\nMonitoring stopped by user")
                break
            except Exception as e:
                print(f"Error in monitor loop: {e}")
                time.sleep(check_interval)  # Still sleep on error to prevent rapid retries

def start_etl_monitor(pw_map, prod_trading=False, check_interval=60, days=1):
    """
    Convenience function to start the ETL monitor.
    
    Args:
        pw_map: Password map containing necessary credentials
        prod_trading: Boolean to determine which Google credentials to use
        check_interval: Time between checks in seconds (default: 60)
        days: Number of days of updates to consider (default: 1)
    """
    manager = ETLUpdateManager(pw_map, prod_trading)
    manager.monitor_and_update(check_interval=check_interval, days=days)

if __name__ == "__main__":
    # Example usage (assuming pw_map is defined)
    # start_etl_monitor(pw_map)
    pass
