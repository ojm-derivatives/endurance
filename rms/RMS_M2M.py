import inspect
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import threading
from com.prod.classes import TimeDetails
import traceback

import rms
from rms import RMS_Main
from com.prod.util import *


class RMS_M2M(RMS_Main):
    def __init__(self, project_name, module_name=''):
        super().__init__(project_name=project_name, module_name=module_name)
        self.orders_df = rms.rms_main_obj.orders_df
        self.positions_net_df = rms.rms_main_obj.positions_net_df
        self.module_name = module_name
        self.time_details_obj = TimeDetails(project_name=project_name, module_name=module_name)

        self.logger.info(f'Initiated the {project_name}-{module_name}-{self.__class__.__name__}')

        #variables related to M2M Calculations and Management
        self.stoploss_m2m = int(self.config.get('RMS_M2M', 'stoploss_m2m'))
        self.target_m2m = int(self.config.get('RMS_M2M', 'target_m2m'))

        #Related to m2m_df which is to be used in M2M_Reports
        self.m2m = 0
        self.m2m_df = pd.DataFrame(
            columns=['Date', 'Time', 'ProjectName', 'RPT', 'MAX_MTM_ALLOWED', 'n_trades_ce', 'n_trades_pe', 'm2m'])
        self.n_trades_ce = 0
        self.n_trades_pe = 0

    def calculate_m2m(self):
        # Simulate fetching order book data
        entry_datetime = self.time_details_obj.algo_run_start_time_datetime
        wait_until_entry_time(entry_datetime=entry_datetime, interval=0.5)
        while self.is_m2m_calculation_running and self.time_details_obj.algo_run_start_time <= datetime.now().time() <= self.time_details_obj.algo_run_end_time:
            try:
                print(
                    f'Module {self.project_name}-{self.module_name}-{self.__class__.__name__}-{inspect.currentframe().f_code.co_name} is running...')
                time.sleep(10)
                if self.positions_net_df is None:
                    print(f'No Position Found yet. Hence, Calculate_m2m is going to sleep for 120 seconds.')
                    time.sleep(120)
                if self.positions_net_df is not None:
                    if len(self.positions_net_df) < 0:
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
                        self.n_trades_ce = self.count_n_trades('CE')
                        self.n_trades_pe = self.count_n_trades('PE')
                        self.m2m_df.loc[len(self.m2m_df.index)] = [datetime.today().date(),
                                                                   datetime.today().now().strftime("%H:%M:%S"),
                                                                   self.project_name, "***", self.stoploss_m2m,
                                                                   self.n_trades_ce, self.n_trades_pe, self.m2m]
                        print(f'Time: {datetime.now().time().strftime("%H:%M:%S")}            M2M: {self.m2m}')

                if self.m2m < -self.stoploss_m2m:
                    print(f"starting Cover_At_Max_Loss Thread at {datetime.now().time()}")
                    x = threading.Thread(target=self.send_message.send_critical, args=("Max MTM exceeded.",), daemon=True)
                    x.start()
                    self.cover_all_at_max_mtm(tag='RMS_COVER_MAX_LOSS')
                elif self.m2m > self.target_m2m:
                    print(f"starting Thread at {datetime.now().time()}")
                    x = threading.Thread(target=self.send_message.send_critical,
                                         args=("Target MTM achieved. Covering all the Positions.",), daemon=True)
                    x.start()
                    self.cover_all_at_max_mtm(tag='RMS_COVER_MAX_PRO')

            except Exception as e:
                print(f'Exception occurred while Calculating the M2M in RMS-Main. Re-trying...')
                print(e)
                tb = e.__traceback__
                traceback.print_tb(tb)

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
        self.logger.info(
            f'Stopped the {self.project_name}-{self.module_name}-{self.__class__.__name__}-{inspect.currentframe().f_code.co_name}')

    #This function calculates the total number of SHORT order w.r.t. an option_type = 'CE or 'PE'
    def count_n_trades(self, option_type):
        if option_type not in ['CE', 'PE']:
            print(f'option_type should be either "CE" or "PE". The passed option_type value is: {option_type}')
            return
        n_trades = 0
        n_trades += len(self.orders_df[(self.orders_df['transaction_type'] == 'SELL') & (
                self.orders_df['status'] == 'COMPLETE') & (self.orders_df['tradingsymbol'].str[-2:] == option_type)])
        return n_trades

    def place_cover_order_by_thread(self, symbol_to_be_covered, tag):
        try:
            self.positions_net_df = rms.rms_main_obj.positions_net_df
            self.orders_df = rms.rms_main_obj.orders_df
            order_cover_type = ""
            quantity_to_be_covered = int(
                self.positions_net_df[self.positions_net_df['tradingsymbol'] == symbol_to_be_covered]['quantity'].sum())
            if quantity_to_be_covered < 0:
                order_cover_type = 'BUY'
            elif quantity_to_be_covered > 0:
                order_cover_type = 'SELL'
            if abs(quantity_to_be_covered) <= self.freeze_qty_nf:
                order_id = self.kite.place_order(tradingsymbol=symbol_to_be_covered,
                                                 exchange='NFO',
                                                 transaction_type=order_cover_type,
                                                 quantity=abs(quantity_to_be_covered),
                                                 order_type='MARKET',
                                                 product='MIS',
                                                 variety='regular',
                                                 tag=tag)

            elif abs(quantity_to_be_covered) > self.freeze_qty_nf:
                quantity_to_be_covered = self.freeze_qty_nf
                while abs(quantity_to_be_covered) > 0:
                    if abs(quantity_to_be_covered) > self.freeze_qty_nf:
                        quantity_to_be_covered = self.freeze_qty_nf

                    order_id = self.kite.place_order(tradingsymbol=symbol_to_be_covered,
                                                     exchange='NFO',
                                                     transaction_type=order_cover_type,
                                                     quantity=abs(quantity_to_be_covered),
                                                     order_type='MARKET',
                                                     product='MIS',
                                                     variety='regular',
                                                     tag=tag)
                    is_order_completed = False
                    while not is_order_completed and order_id != 0 and order_id is not None:
                        time.sleep(1)
                        order_status = self.orders_df[self.orders_df['order_id'] == order_id]['status'].iloc[0]
                        if order_status == 'COMPLETED':
                            is_order_completed = True
                    time.sleep(1)
                    rms.rms_main_obj.fetch_order_and_position_book()
                    quantity_to_be_covered = int(
                        self.positions_net_df[self.positions_net_df['tradingsymbol'] == symbol_to_be_covered][
                            'quantity'].sum())
        except Exception as e:
            print(f'Exception occurred while trying to cover the {symbol_to_be_covered} in place_cover_by_thread.')
            self.send_message.send_critical(
                f'Exception occurred while trying to cover the {symbol_to_be_covered} in place_cover_by_thread.')

    def cover_all_at_max_mtm(self, tag='RMS_COVER_MAX_MTM'):
        try:
            positions_net_df = self.positions_net_df.sort_values('quantity')
            num_unique_positions = len(positions_net_df)
            list_of_symbols_to_be_covered = \
                positions_net_df[(positions_net_df['quantity'] < 0) & (positions_net_df['product'] == 'MIS')][
                    'tradingsymbol'].unique().tolist()

            max_workers = num_unique_positions*2

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit tasks to the ThreadPoolExecutor
                [executor.submit(self.place_cover_order_by_thread, symbol_to_be_covered, tag) for symbol_to_be_covered
                 in list_of_symbols_to_be_covered]

        except Exception as e:
            print(e)
            message = f"The MAX MTM has been exceeded and there has been an exception while executing the 'cover_all_at_max_mtm' function. Kindly Check Manually." + \
                      f"Re-trying to COVER ALL THE POSITIONS AS MAX MTM EXCEEDED."
            print(message)
            self.send_message.send_critical(message)

    def main(self):
        entry_datetime = rms.rms_main_obj.time_details_obj.algo_run_start_time_datetime
        wait_until_entry_time(entry_datetime=entry_datetime, interval=1)
        try:
            self.start_m2m_calculation()
        except Exception as e:
            print(
                f'Exception occurred in {self.project_name}-{self.module_name}-{self.__class__.__name__}-{inspect.currentframe().f_code.co_name}')
