import inspect
import time
from datetime import datetime, timedelta
import threading
from com.prod.classes import TimeDetails
import traceback

import rms
from rms import RMS_Main
from com.prod.util import *


class RMS_Reports(RMS_Main):
    def __init__(self, project_name, module_name='RMS_Reports'):
        super().__init__(project_name=project_name, module_name=module_name)
        self.orders_df = rms.rms_main_obj.orders_df
        self.positions_net_df = rms.rms_main_obj.positions_net_df
        self.module_name = module_name
        self.time_details_obj = TimeDetails(project_name=project_name, module_name=module_name)
        self.logger.info(f'Initiated the {project_name}-{module_name}-{self.__class__.__name__}')

    def calculate_m2m(self):
        # Simulate fetching order book data
        entry_datetime = self.time_details_obj.algo_run_start_time_datetime
        wait_until_entry_time(entry_datetime=entry_datetime, interval=0.5)
        while self.is_m2m_calculation_running and self.time_details_obj.algo_run_start_time <= datetime.now().time() <= self.time_details_obj.algo_run_end_time:
            try:
                print(f'Module {self.project_name}-{self.module_name}-{self.__class__.__name__}-{inspect.currentframe().f_code.co_name} is running...')
                time.sleep(10)
                if rms.rms_main_obj.positions_net_df is None:
                    print(f'No Position Found yet. Hence, Calculate_m2m is going to sleep for 120 seconds.')
                    time.sleep(120)
                if self.positions_net_df is not None:
                    positions_net_mis_df = self.positions_net_df[self.positions_net_df['product'] == 'MIS']
                    symbols = positions_net_mis_df[positions_net_mis_df['product'] == 'MIS'][
                        'tradingsymbol'].unique().tolist()
                    for symbol in symbols:
                        positions_net_mis_df.loc[positions_net_mis_df['tradingsymbol'] == symbol, 'last_price'] = \
                            self.kite.quote(f'NFO:{symbol}')[f'NFO:{symbol}']['last_price']
                    positions_net_mis_df['m2m'] = positions_net_mis_df['day_sell_quantity'] * positions_net_mis_df[
                        'day_sell_price'] + positions_net_mis_df['quantity'] * positions_net_mis_df['last_price'] - \
                                                  positions_net_mis_df['day_buy_quantity'] * positions_net_mis_df[
                                                      'day_buy_price']
                    self.m2m = positions_net_mis_df['m2m'].sum()
                    print(f'Time: {datetime.now().time().strftime("%H:%M:%S")}            M2M: {self.m2m}')

            except Exception as e:
                print(f'Exception occurred while Calculating the M2M in RMS-Main. Re-trying...')
                print(e)
                tb = e.__traceback__
                traceback.print_tb(tb)

        # self.stop_m2m_calculation()

    def start_m2m_calculation(self):
        self.is_m2m_calculation_running = True
        self.calculate_m2m_thread = threading.Thread(target=self.calculate_m2m)
        self.calculate_m2m_thread.start()
        self.logger.info(
            f'Started the {self.project_name}-{self.module_name}-{self.__class__.__name__}-{inspect.currentframe().f_code.co_name}')

    def stop_m2m_calculation(self):
        self.is_fetch_order_and_position_book_running = False
        if self.calculate_m2m_thread:
            self.calculate_m2m_thread.join()
        self.logger.info(f'Stopped the {self.project_name}-{self.module_name}-{self.__class__.__name__}-{inspect.currentframe().f_code.co_name}')

    def main(self):
        entry_datetime = rms.rms_main_obj.time_details_obj.algo_run_start_time_datetime
        wait_until_entry_time(entry_datetime=entry_datetime, interval=1)
        try:
            self.start_m2m_calculation()
        except Exception as e:
            print(f'Exception occurred in {self.project_name}-{self.module_name}-{self.__class__.__name__}-{inspect.currentframe().f_code.co_name}')
