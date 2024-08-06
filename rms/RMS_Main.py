import threading
import traceback

from com.prod.util import *
import pandas as pd
from datetime import datetime
import time
from com.decorators import check_holiday
from com.prod.projects.kite.rms import RMSConfig

class RMS_Main(RMSConfig):
    def __init__(self, project_name, module_name='', logging_level=logging.INFO):
        super().__init__(project_name=project_name, module_name=module_name)
        self.fetch_order_and_position_book_thread = None
        self.is_fetch_order_and_position_book_running = None
        self.orders_df = None
        self.positions_net_df = None

        self.logging_level = logging_level
        self.log_path = self.project_home + self.config.get(self.module_name, RMSConfig.LOG_PATH)

        self.kite = get_kite_object_initialised()
        self.logger = initializeLogger(loggerName=module_name, logPath=self.log_path, level=self.logging_level)
        self.logger.info(f'Initiated the {project_name} - {module_name} - {self.__class__.__name__}')

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
        self.flag_wait_for_matching_pos = False
        # self.start_order_and_position_book_fetching()

    def fetch_order_and_position_book(self):
        # Simulate fetching order book data
        while self.is_fetch_order_and_position_book_running:
            try:
                self.orders_df = pd.DataFrame(self.kite.orders())
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
                time.sleep(1/5)
            except Exception as e:
                print(f'Exception occurred while fetching the Order and Position Book from Kite. Re-trying...')
                print(e)
                tb = e.__traceback__
                traceback.print_tb(tb)

    def start_order_and_position_book_fetching(self):
        self.is_fetch_order_and_position_book_running = True
        self.fetch_order_and_position_book_thread = threading.Thread(target=self.fetch_order_and_position_book)
        self.fetch_order_and_position_book_thread.start()
        self.logger.info(f'Started the {self.project_name}-{self.module_name}-{self.__class__.__name__}-{inspect.currentframe().f_code.co_name}')

    def stop_order_book_fetching(self):
        self.is_fetch_order_and_position_book_running = False
        self.fetch_order_and_position_book_thread.join()
        self.logger.info(f'Stopped the {self.project_name}-{self.module_name}-{self.__class__.__name__}-{inspect.currentframe().f_code.co_name}')

    def calculate_m2m(self):
        pass

    def place_cover_order_by_thread(self, semaphore, symbol_to_be_covered, tag):
        try:
            # positions = kite.positions()
            # positions_df = pd.DataFrame(positions['net'])
            # positions_df = positions_df.sort_values('quantity')
            order_cover_type = ""

            positions = self.kite.positions()
            positions_df = pd.DataFrame(positions['net'])
            quantity_to_be_covered = int(
                positions_df[positions_df['tradingsymbol'] == symbol_to_be_covered]['quantity'].sum())
            if quantity_to_be_covered < 0:
                order_cover_type = 'BUY'
            elif quantity_to_be_covered > 0:
                order_cover_type = 'SELL'
            if abs(quantity_to_be_covered) <= freeze_qty:
                order_id = self.kite.place_order(tradingsymbol=symbol_to_be_covered,
                                            exchange='NFO',
                                            transaction_type=order_cover_type,
                                            quantity=abs(quantity_to_be_covered),
                                            order_type='MARKET',
                                            # price=0
                                            # trigger_price=originalSL,
                                            product='MIS',
                                            variety='regular',
                                            tag=tag)

            elif abs(quantity_to_be_covered) > freeze_qty:
                quantity_to_be_covered = freeze_qty
                while abs(quantity_to_be_covered) > 0:
                    if abs(quantity_to_be_covered) > freeze_qty:
                        quantity_to_be_covered = freeze_qty

                    order_id = self.kite.place_order(tradingsymbol=symbol_to_be_covered,
                                                exchange='NFO',
                                                transaction_type=order_cover_type,
                                                quantity=abs(quantity_to_be_covered),
                                                order_type='MARKET',
                                                # price=0
                                                # trigger_price=originalSL,
                                                product='MIS',
                                                variety='regular',
                                                tag=tag)
                    time.sleep(1)
                    positions = self.kite.positions()
                    positions_df = pd.DataFrame(positions['net'])
                    quantity_to_be_covered = int(
                        positions_df[positions_df['tradingsymbol'] == symbol_to_be_covered]['quantity'].sum())
        finally:
            semaphore.release()

    def cover_all_at_max_mtm(kite, tag='RMS_COVER_MAX_MTM'):
        i = 0
        positions = kite.positions()
        positions_net_df = pd.DataFrame(positions['net'])
        positions_net_df = positions_net_df.sort_values('quantity')
        num_unique_positions = len(positions_net_df)
        list_of_symbols_to_be_covered = \
            positions_net_df[(positions_net_df['quantity'] < 0) & (positions_net_df['product'] == 'MIS')][
                'tradingsymbol'].unique().tolist()
        while i < num_unique_positions:
            i = i + 1
            try:
                semaphore.acquire()
                thread = threading.Thread(target=place_cover_order_by_thread,
                                          args=(semaphore, list_of_symbols_to_be_covered[i], tag))
                thread.start()
            except Exception as e:
                print(e)
                print(
                    f"The MAX MTM has been exceeded and there has been an exception while executing the 'cover_all_at_max_mtm' function. Kindly Check Manually.")
                print(f'Re-tryting to COVER ALL THE POSITIONS AT MAX MTM EXCEEDED for the {i + 1} time.')
                SendMessage.send_critical(
                    f"The MAX MTM has been exceeded and there has been an exception while executing the 'cover_all_at_max_mtm' function. Kindly Check Manually.")
                SendMessage.send_critical(
                    f'Re-tryting to COVER ALL THE POSITIONS AT MAX MTM EXCEEDED for the {i + 1} time.')
                pass

    @check_holiday(module_name='RMS-GenerateExpireCodes')
    def main(self):
        print(f'{datetime.now()}')
        time.sleep(10)


if __name__ == '__main__':
    main()
