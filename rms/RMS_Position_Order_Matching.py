import kiteconnect
from com.prod.projects.kite.kite_access import KiteAccess
from com.prod.util import *
import time
import pandas as pd
from datetime import datetime, timedelta
import threading
from requests.exceptions import ReadTimeout as RTOException
from kiteconnect.exceptions import NetworkException as NetworkException
from kiteconnect.exceptions import DataException as DataException
from com.prod.projects.kite.rms import RMSConfig
from com.prod.classes import TimeDetails

import rms
from rms import *

class RMS_Position_Order_Matching(RMS_Main):
    def __init__(self, project_name, module_name=''):
        super().__init__(project_name=project_name, module_name=module_name)
        self.module_name = module_name
        self.time_details_obj = TimeDetails(project_name=project_name, module_name=module_name)

        self.logger.info(f'Initiated the {project_name}-{module_name}-{self.__class__.__name__}')

    def run_PosMS(self):
        while datetime.now().time() <= self.time_details_obj.algo_run_start_time:
            print(f"Current Time: {datetime.now().time().strftime('%H:%M:%S')}\nWaiting for {self.time_details_obj.algo_run_start_time_str} to start.")
            entry_time = self.time_details_obj.algo_short_entry_time_datetime.timestamp()
            sleep_for_a_round_interval(entry_time, 0.5)
            # time.sleep(30 - ((time.time() - entry_time) % 30.0))

        exception_count = 0

        while (datetime.now().time() >= self.time_details_obj.algo_short_entry_time) and datetime.now().time() <= (
                self.time_details_obj.algo_run_end_time_datetime + timedelta(minutes=5)).time():

            try:
                if self.flag_wait_for_matching_pos:
                    print(
                        'The Last order has been completed or cancelled in the last 30 seconds and hence, waiting for 30 more seconds.')
                    time.sleep(30)
                    self.flag_wait_for_matching_pos = False
                print("Inside While Loop at Time: ", datetime.now().time().strftime('%H:%M:%S'))
                time.sleep(15)

                print(
                    f"\n\nTime: {datetime.now().time().strftime('%H:%M:%S')}\nSL Order Qty and Position Qty Matching System Running...\n")

                if not self.positions_net_df.empty:
                    list_of_tradingsymbols = self.positions_net_df['tradingsymbol'].tolist()

                    for tradingsymbol in list_of_tradingsymbols:
                        qty_position_tradingsymbol = \
                            self.positions_net_df[self.positions_net_df['tradingsymbol'] == tradingsymbol][
                                'quantity'].iloc[0]

                        if qty_position_tradingsymbol < 0:
                            qty_position_tradingsymbol = self.positions_net_df[
                                (self.positions_net_df['tradingsymbol'] == tradingsymbol) & (
                                        self.positions_net_df['product'] == 'MIS')]['quantity']
                            if len(qty_position_tradingsymbol) > 0:
                                qty_position_tradingsymbol = qty_position_tradingsymbol.iloc[0]
                                if qty_position_tradingsymbol < 0:
                                    print(
                                        f"\n******************************************\n"
                                        + f"TradingSymbol: {tradingsymbol}\n"
                                        + f"Position Qty: {qty_position_tradingsymbol}")
                                    qty_orders_tradingsymbol = -self.orders_df[
                                        (self.orders_df['tradingsymbol'] == tradingsymbol) & (
                                                self.orders_df['status'] == 'TRIGGER PENDING') & (
                                                self.orders_df['transaction_type'] == 'BUY')]['quantity'].sum()
                                    status_message = f'SL Order Qty: {qty_orders_tradingsymbol}'
                                    print(status_message)
                                    if qty_position_tradingsymbol != qty_orders_tradingsymbol:
                                        message = (
                                            f'\nThe total sum of SL Order Quantity for {tradingsymbol} is found to be {qty_orders_tradingsymbol} \nand while the net position Qty is {qty_position_tradingsymbol}.\n********************************************\n'
                                            )
                                        if -qty_position_tradingsymbol > -qty_orders_tradingsymbol:
                                            mismatch_qty = abs(qty_orders_tradingsymbol - qty_position_tradingsymbol)
                                            message += f'\nThe SL Order Quantity is LESS THAN the Net Positions by \n**{mismatch_qty}**\n'
                                            message += '\nSUGGESTION:\n'
                                            message += f'EITHER PLACE {mismatch_qty} quantity of StopLoss Order for {tradingsymbol} in the OrderBook.\nOR\nCANCEL ({mismatch_qty}) quantity of {tradingsymbol} from Positions.'
                                        elif -qty_position_tradingsymbol < -qty_orders_tradingsymbol:
                                            mismatch_qty = abs(qty_orders_tradingsymbol - qty_position_tradingsymbol)
                                            message += f'\nThe SL Order Quantity is MORE THAN the Net Positions by \n**{mismatch_qty}**\n'
                                            message += '\nSUGGESTION:\n'
                                            message += f'EITHER CANCEL {mismatch_qty} quantity of StopLoss Order for {tradingsymbol} from the OrderBook.\nOR\nPLACE the ({mismatch_qty}) quantity of SHORT for {tradingsymbol} in Positions.'

                                        message += f"\n******************************************\n"
                                        print(message)
                                        self.send_message.send_critical(message)
                                        print('Now Sleeping for 60 seconds...')
                                        time.sleep(60)
                    print('==================================================================')

            except RTOException:
                print(f"ReadTimeOut Exception occurred in {self.module_name}. Re-trying again...")
                self.send_message.send_log("ReadTimeOut Exception occurred in RMS. Re-trying again...")
                exception_count += 1
            except NetworkException as e:
                print(f'ReadTimeOut Exception occurred in {self.module_name}. Re-trying again...')
                self.send_message.send_log(f'ReadTimeOut Exception occurred in {self.module_name}. Re-trying again...')
                exception_count += 1
            except DataException as e:
                print(f'Data Exception occurred in {self.module_name}. Re-trying again...')
                self.send_message.send_log(f'Data Exception occurred in {self.module_name}. Re-trying again...')
                exception_count += 1
            except Exception as e:
                print(self.logger.exception(e))
                print(f'Exception in {self.module_name}!')
                self.send_message.send_log(f"Exception in {self.module_name}.\n {str(e)}")
                exception_count += 1
            finally:
                if exception_count > 15:
                    self.send_message.send_critical(
                        f'There has been an exception in {self.module_name} for more than {exception_count} times.')
                    exception_count = 0

    def main(self):
        try:
            rms_posms_obj = RMS_Position_Order_Matching('Endurance', 'RMS')
            rms_posms_obj.run_PosMS()
        except kiteconnect.exceptions.TokenException as e:
            print(f'TokenException as {e}. Generating the Kite Access Token Now.')
            try:
                kite_access = KiteAccess()
            except Exception as e:
                print(f'Exception: {e} while generating the Access Token')
            else:
                self.main()
