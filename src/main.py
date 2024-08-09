from dotenv import load_dotenv
load_dotenv()

import rms
from com.prod.util import wait_until_entry_time
from rms import RMS_Position_Order_Matching
from rms import RMS_GenerateExpiryCodes
import concurrent.futures
from com.decorators import check_holiday



# Define a function to run the main method
def run_main_method_of_sub_module(obj):
    obj.main()

check_holiday("RMS_Main")
def run_main():
    entry_datetime = rms.rms_main_obj.time_details_obj.algo_run_start_time_datetime
    wait_until_entry_time(entry_datetime=entry_datetime, interval=1)

    sub_modules = {'rms_pos_match': RMS_Position_Order_Matching(module_name='RMS_Pos_Match'),
                   'rms_gen_exp_codes': RMS_GenerateExpiryCodes(module_name='RMS_Generate_Expiry_Codes')
                   }
    rms.rms_main_obj.main()
    rms.rms_m2m_obj.main()
    # Use ThreadPoolExecutor to run the main methods in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit all main method calls to the executor
        futures = [executor.submit(run_main_method_of_sub_module, obj) for obj in sub_modules.values()]

        # Optionally wait for all futures to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # If you need to handle exceptions, access the result here
            except Exception as exc:
                print(f'Generated an exception: {exc}')

    print("All main methods have been called.")

if __name__ == '__main__':
    run_main()
