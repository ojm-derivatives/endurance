import os
import threading
import traceback
import inspect

import kiteconnect
from com.prod.util import *
import pandas as pd
from datetime import datetime
import time
from com.decorators import check_holiday
from com.prod.projects.kite.rms import RMSConfig
from com.prod.projects.kite.kite_access import KiteAccess

import rms


class RMS_Main(RMSConfig):
    def __init__(self, module_name='', logging_level=logging.INFO):
        self.project_name = os.getenv('PROJECT_NAME')
        self.config_filename_with_path = os.getenv('CONFIG_FILENAME_WITH_PATH')
        super().__init__(module_name=module_name)
        self.orders_df = None
        self.positions_net_df = None

        self.logging_level = logging_level
        self.log_path = self.project_home + self.config.get(self.module_name, RMSConfig.LOG_PATH)

        self.kite = get_kite_object_initialised()
        self.logger = initializeLogger(loggerName=module_name, logPath=self.log_path, level=self.logging_level)
        self.logger.info(f'Initiated the {project_name}-{module_name}-{self.__class__.__name__}')

        self.trading_days_df_nf = pd.read_csv(self.expiry_mapping_filename_nf, header=0)
        self.trading_days_df_bnf = pd.read_csv(self.expiry_mapping_filename_bnf, header=0)

        self.trading_days_list_nf = self.trading_days_df_nf.TradingDate.str.replace('-', "").to_list()
        self.trading_days_list_bnf = self.trading_days_df_bnf.TradingDate.str.replace('-', "").to_list()

        self.truedata_obj = get_TD_obj()
        self.kite = get_kite_object_initialised()

        self.expiry_code_td_nf = ""
        self.expiry_code_td_bnf = ""

        self.expiry_code_kite_nf = ""
        self.expiry_code_kite_bnf = ""

        #From Commn_Params Section of the Config file
        self.freeze_qty_nf = self.config.read('Common_Params', 'freeze_qty_nf')
        self.round_lot_size_nf = self.config.read('Common_Params', 'round_lot_size_nf')

        self.dict_of_test_symbol = {'NIFTY 50': {'symbol': 'NIFTY',
                                                 'spot': '',
                                                 'round_strike_size': 50,
                                                 'expiry_code_td': '',
                                                 'expiry_code_kite': ''},
                                    'NIFTY BANK': {'symbol': 'BANKNIFTY',
                                                   'spot': '',
                                                   'round_strike_size': 100,
                                                   'expiry_code_td': '',
                                                   'expiry_code_kite': ''}
                                    }

        #start fetching the Order Book and Position Book continuously using a thread
        self.is_fetch_order_and_position_book_running = False
        self.fetch_order_and_position_book_thread = None
        self.flag_wait_for_matching_pos = False

        #related to the M2M Calculation
        self.is_m2m_calculation_running = False
        self.calculate_m2m_thread = None
        self.m2m = None

    def fetch_order_and_position_book(self):
        # Simulate fetching order book data
        entry_datetime = self.time_details_obj.algo_run_start_time_datetime
        wait_until_entry_time(entry_datetime=entry_datetime, interval=0.5)

        while self.is_fetch_order_and_position_book_running and self.time_details_obj.algo_run_start_time <= datetime.now().time() <= self.time_details_obj.algo_run_end_time:
            try:
                self.orders_df = pd.DataFrame(self.kite.orders())
                time.sleep(1)
                positions = self.kite.positions()
                if len(positions) > 0:
                    self.positions_net_df = pd.DataFrame(positions['net'])
                print(f"Order and Position Book updated at {datetime.now().time().strftime('%H:%M:%S')}")
                if not self.orders_df.empty:
                    completed_orders_df = self.orders_df[(self.orders_df['status'] == 'COMPLETE')]
                    cancelled_orders_df = self.orders_df[(self.orders_df['status'] == 'CANCELLED')]
                    pending_sl_orders_df = self.orders_df[(self.orders_df['status'] == 'TRIGGER PENDING')]
                    time_of_last_completed_or_cancelled_order = max(completed_orders_df.order_timestamp.max(),
                                                                    cancelled_orders_df.order_timestamp.max(),
                                                                    pending_sl_orders_df.order_timestamp.max())
                    difference_curr_time_last_executed_time = (
                            datetime.now() - time_of_last_completed_or_cancelled_order).total_seconds()
                    if difference_curr_time_last_executed_time < 50.0:
                        self.flag_wait_for_matching_pos = True
                time.sleep(3)
                rms.rms_m2m_obj.orders_df = self.orders_df
                rms.rms_m2m_obj.positions_net_df = self.positions_net_df


            except Exception as e:
                print(f'Exception occurred while fetching the Order and Position Book from Kite. Re-trying...')
                print(e)
                tb = e.__traceback__
                traceback.print_tb(tb)
                kite_access_obj = None
                try:
                    print('Trying to generate the Kite Access Token')
                    if str(e).__contains__("Incorrect `api_key` or `access_token`"):
                        kite_access_obj = KiteAccess()
                        kite_access_obj.generate_access_token()
                        self.kite.set_access_token(kite_access_obj.access_token)
                        self.kite = get_kite_object_initialised()
                except Exception as e:
                    print(f"Exception occurred while generating the KITE Access Token. Exception: {e}.\nExiting...")
            except kiteconnect.exceptions.NetworkException as e:
                print(e)

    def start_order_and_position_book_fetching(self):
        self.is_fetch_order_and_position_book_running = True
        self.fetch_order_and_position_book_thread = threading.Thread(target=self.fetch_order_and_position_book)
        self.fetch_order_and_position_book_thread.start()
        self.logger.info(f'Started the {self.project_name}-{self.module_name}-{self.__class__.__name__}-{inspect.currentframe().f_code.co_name}')

    def stop_order_book_fetching(self):
        self.is_fetch_order_and_position_book_running = False
        self.fetch_order_and_position_book_thread.join()
        self.logger.info(f'Stopped the {self.project_name}-{self.module_name}-{self.__class__.__name__}-{inspect.currentframe().f_code.co_name}')

    @check_holiday(module_name='RMS-GenerateExpireCodes')
    def main(self):
        entry_datetime = rms.rms_main_obj.time_details_obj.algo_run_start_time_datetime
        wait_until_entry_time(entry_datetime=entry_datetime, interval=1)
        rms.rms_main_obj.start_order_and_position_book_fetching()
