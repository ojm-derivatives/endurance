from datetime import datetime
from rms.RMS_Main import RMS_Main
import concurrent.futures

rms_main = RMS_Main('Endurance', 'RMS')


# Define a function to run the main method
def run_main_method(obj):
    obj.main()


# Use ThreadPoolExecutor to run the main methods in parallel
with concurrent.futures.ThreadPoolExecutor() as executor:
    # Submit all main method calls to the executor
    futures = [executor.submit(run_main_method, obj) for obj in rms_main.sub_modules.values()]

    # Optionally wait for all futures to complete
    for future in concurrent.futures.as_completed(futures):
        try:
            future.result()  # If you need to handle exceptions, access the result here
        except Exception as exc:
            print(f'Generated an exception: {exc}')

print("All main methods have been called.")
