from com.prod.util import *
import pandas as pd
from datetime import datetime
from com.supabase.tables import ExpiryCode
from com.decorators import check_holiday
from com.prod.decorators import exception_handler_decorator_factory, exception_handler_decorator
from rms import RMS_Main


class RMS_GenerateExpiryCodes(RMS_Main):
    def __init__(self, project_name, module_name='RMS_Generate_Expiry_codes'):
        super().__init__(project_name=project_name, module_name=module_name)
        self.test_download_passed = False

        self.logger.info(f'Initiated the {project_name}-{module_name}-{self.__class__.__name__}')

    def check_truedata_download_test(self):
        try:
            self.test_download_passed = False
            for key in self.dict_of_test_symbol:
                spot = str(round(
                    pd.DataFrame(self.truedata_obj.get_historic_data(key, duration='5 D', bar_size='5 min')).tail(1)[
                        'c'].iloc[0] / self.dict_of_test_symbol[key]['round_strike_size']) *
                           self.dict_of_test_symbol[key]['round_strike_size'])
                self.dict_of_test_symbol[key]['spot'] = spot
        except Exception as e:
            message = f'Exception occurred while fetching the spot data from True Data while running {self.module_name}.'

            @exception_handler_decorator_factory()
            def place_holder_function(args, kwargs):
                pass  # This is a placeholder, actual logic is in the decorator

            place_holder_function(exception=e, cls_obj=self, send_message_level='critical', message=message)
        else:
            self.test_download_passed = True

    def generate_and_check_expiry_codes(self):
        try:
            self.test_download_passed = False
            df = pd.DataFrame(self.kite.instruments('NFO'))
            df_nf = df[(df['name'] == 'NIFTY') & (df['segment'] == 'NFO-OPT')].sort_values('expiry')
            df_bnf = df[(df['name'] == 'BANKNIFTY') & (df['segment'] == 'NFO-OPT')].sort_values('expiry')

            self.expiry_code_td_nf = datetime.strftime(df_nf['expiry'].iloc[0], "%y%m%d")
            self.expiry_code_td_bnf = datetime.strftime(df_bnf['expiry'].iloc[0], "%y%m%d")

            self.expiry_code_kite_nf = df_nf['tradingsymbol'].iloc[0][:-7][5:]
            self.expiry_code_kite_bnf = df_bnf['tradingsymbol'].iloc[0][:-7][9:]

            self.dict_of_test_symbol['NIFTY 50']['expiry_code_td'] = self.expiry_code_td_nf
            self.dict_of_test_symbol['NIFTY 50']['expiry_code_kite'] = self.expiry_code_kite_nf

            self.dict_of_test_symbol['NIFTY BANK']['expiry_code_td'] = self.expiry_code_td_bnf
            self.dict_of_test_symbol['NIFTY BANK']['expiry_code_kite'] = self.expiry_code_kite_bnf

            print(f"expiry_code_td_nf:{self.expiry_code_td_nf}")
            print(f"expiry_code_td_bnf:{self.expiry_code_td_bnf}")

            print(f"kite_expiry_code_nf:{self.expiry_code_kite_nf}")
            print(f"kite_expiry_code_bnf:{self.expiry_code_kite_bnf}")

        except Exception as e:
            message = f'Exception occurred while generating the Expiry Codes while running {self.module_name}.'

            @exception_handler_decorator_factory()
            def place_holder_function(args, kwargs):
                pass  # This is a placeholder, actual logic is in the decorator

            place_holder_function(exception=e, cls_obj=self, send_message_level='critical', message=message)

        else:
            print(f'SUCCESS: Generated the Expiry Codes while running {self.module_name}.')
            self.send_message.send_log(
                f'SUCCESS: Generated the Expiry Codes while running {self.module_name}.')
            self.test_download_passed = True
        try:
            self.test_download_passed = False
            for key in self.dict_of_test_symbol:
                test_symbol = self.dict_of_test_symbol[key]['symbol'] + self.dict_of_test_symbol[key][
                    'expiry_code_td'] + self.dict_of_test_symbol[key]['spot'] + 'CE'
                test_data = pd.DataFrame(
                    self.truedata_obj.get_historic_data(test_symbol, duration='5 D', bar_size='5 min'))
                print(f'\nTested downloading Data for {test_symbol}\nTEST DATA:\n{test_data.head(2)}')
        except Exception as e:
            message = f'Exception occurred while fetching the data with the generated Expiry_Code_TD {self.expiry_code_td_nf}. Check Manually.'

            @exception_handler_decorator_factory()
            def place_holder_function(args, kwargs):
                pass  # This is a placeholder, actual logic is in the decorator

            place_holder_function(exception=e, cls_obj=self, send_message_level='critical', message=message)

        else:
            self.test_download_passed = True
        finally:
            if self.test_download_passed:
                print("******************************************************************************************")
                print("                              TEST DOWNLOADING - PASSED")
                print("******************************************************************************************")
                self.send_message.send_signal("TEST DOWNLOADING - PASSED")
            else:
                self.send_message.send_critical('TEST DOWNLOADING - FAILED')

    def set_expiry_codes_to_config_file(self, module_name='RMS_Expiry_Codes'):
        try:
            self.test_download_passed = False
            print(
                f"\n*******************************************************************************************\nSetting the Expiry Code of TD to {self.expiry_code_td_nf} & {self.expiry_code_td_bnf} in config.ini")
            self.config.set(module_name, "expiry_code_td_nf", self.expiry_code_td_nf)
            self.config.set(module_name, "expiry_code_td_bnf", self.expiry_code_td_bnf)
            with open(self.config_filename_with_path, 'w') as file:
                self.config.write(file)
        except Exception as e:
            message = "Exception occurred while setting the Expiry_Code_TD in config.ini"

            @exception_handler_decorator_factory()
            def place_holder_function(args, kwargs):
                pass  # This is a placeholder, actual logic is in the decorator

            place_holder_function(exception=e, cls_obj=self, send_message_level='critical', message=message)

        else:
            print(f"The value of expiry_code_td is set to {self.expiry_code_td_nf}")
            print(f"The value of expiry_code_td_bnf is set to {self.expiry_code_td_bnf}")
            self.test_download_passed = True

        try:
            self.test_download_passed = False
            print(
                f"\n*******************************************************************************************\nSetting the Expiry Code of KITE to NF:{self.expiry_code_kite_nf} & BNF:{self.expiry_code_kite_bnf} in config.ini")
            self.config.set(module_name, "expiry_code_kite_nf", self.expiry_code_kite_nf)
            self.config.set(module_name, "expiry_code_kite_bnf", self.expiry_code_kite_bnf)
            with open(self.config_filename_with_path, 'w') as file:
                self.config.write(file)
        except Exception as e:
            message = "Exception occurred while setting the expiry_code_kite in config.ini"

            @exception_handler_decorator_factory()
            def place_holder_function(args, kwargs):
                pass  # This is a placeholder, actual logic is in the decorator

            place_holder_function(exception=e, cls_obj=self, send_message_level='critical', message=message)

        else:
            self.test_download_passed = True
            print(
                f"The value of expiry_code_kite is set to {self.expiry_code_kite_nf} in config.ini\n*******************************************************************************************")
            print(
                f"The value of expiry_code_kite_bnf is set to {self.expiry_code_kite_bnf} in config.ini\n*******************************************************************************************")

    def update_expiry_mapping_file(self):
        temp_split = self.time_details_obj.today_yyyy_mm_dd_str.split('_')
        todays_date_dd_mm_yyyy = temp_split[2] + "_" + temp_split[1] + "_" + temp_split[0]
        try:
            self.test_download_passed = False
            print(
                f"\n*******************************************************************************************\nSetting the Expiry Code of TD in TradingDays_ExpiryMapping")
            self.trading_days_df_nf.loc[self.trading_days_df_nf[
                                            'TradingDate'] == todays_date_dd_mm_yyyy, 'TrueDataExpiryCode'] = self.expiry_code_td_nf
            self.trading_days_df_nf.to_csv(self.expiry_mapping_filename_nf, index=False)

            print(
                f"\n*******************************************************************************************\nSetting the Expiry Code of TD_BNF in TradingDays_ExpiryMapping")
            self.trading_days_df_bnf.loc[
                self.trading_days_df_bnf[
                    'TradingDate'] == todays_date_dd_mm_yyyy, 'TrueDataExpiryCode'] = self.expiry_code_td_bnf
            self.trading_days_df_bnf.to_csv(self.expiry_mapping_filename_bnf, index=False)


        except Exception as e:

            message = "Exception occurred while setting the Expiry_Code_TD NF & BNF in TradingDays_ExpiryMapping"

            @exception_handler_decorator_factory()
            def place_holder_function(args, kwargs):
                pass  # This is a placeholder, actual logic is in the decorator

            place_holder_function(exception=e, cls_obj=self, send_message_level='critical', message=message)
        else:
            self.test_download_passed = True
            print(
                f"The value of TrueDataExpiryCode is set to NF:{self.expiry_code_td_nf} & BNF: {self.expiry_code_td_bnf} and in TradingDays_ExpiryMapping.\n*******************************************************************************************")

        try:
            self.test_download_passed = False
            print(
                f"\n*******************************************************************************************\nSetting the KITE Expiry Code in TradingDays_ExpiryMapping")
            self.trading_days_df_nf.loc[
                self.trading_days_df_nf[
                    'TradingDate'] == todays_date_dd_mm_yyyy, 'KiteTradingCode'] = self.expiry_code_kite_nf
            self.trading_days_df_nf.to_csv(self.expiry_mapping_filename_nf, index=False)

            self.trading_days_df_bnf.loc[
                self.trading_days_df_bnf[
                    'TradingDate'] == todays_date_dd_mm_yyyy, 'KiteTradingCode'] = self.expiry_code_kite_bnf
            self.trading_days_df_bnf.to_csv(self.expiry_mapping_filename_bnf, index=False)

        except Exception as e:
            message = "Exception occurred while setting the self.expiry_code_kite in TradingDays_ExpiryMapping"

            @exception_handler_decorator_factory()
            def place_holder_function(args, kwargs):
                pass  # This is a placeholder, actual logic is in the decorator

            place_holder_function(exception=e, cls_obj=self, send_message_level='critical', message=message)
        else:
            self.test_download_passed = True
            print(
                f"The value of KiteTradingCode is set to NF: {self.expiry_code_kite_nf} & BNF: {self.expiry_code_kite_bnf} in TradingDays_ExpiryMapping.\n*******************************************************************************************")

    @exception_handler_decorator
    def update_expiry_codes_in_db(self):
        day_of_week_dict = {1: 'Monday', 2: 'Tuesday', 3: 'Wednesday', 4: 'Thursday', 5: 'Firday', 6: 'Saturday',
                            7: 'Sunday'}
        expiry_code_obj = ExpiryCode()
        trading_date = self.time_details_obj.today_date_yyyymmdd_date
        day = day_of_week_dict.get(trading_date.isoweekday())
        #for NIFTY
        expiry_date = datetime.strptime('20' + self.expiry_code_td_nf, '%Y%m%d')
        expiry_code_td = self.expiry_code_td_nf
        expiry_code_kite = self.expiry_code_kite_nf
        scrip = 'NIFTY'
        if trading_date.isoweekday() in (1, 2, 3, 4, 5):
            # Insert or update the data
            expiry_code_obj.upsert_expiry_code(scrip=scrip, trading_date=trading_date, day=day, expiry_date=expiry_date,
                                               expiry_code_td=expiry_code_td, expiry_code_kite=expiry_code_kite)

        # for BANKNIFTY
        expiry_date = datetime.strptime('20' + self.expiry_code_td_bnf, '%Y%m%d')
        expiry_code_td = self.expiry_code_td_bnf
        expiry_code_kite = self.expiry_code_kite_bnf
        scrip = 'BANKNIFTY'

        # Insert or update the data
        expiry_code_obj.upsert_expiry_code(scrip=scrip, trading_date=trading_date, day=day, expiry_date=expiry_date,
                                           expiry_code_td=expiry_code_td, expiry_code_kite=expiry_code_kite)

    @check_holiday(module_name='RMS-GenerateExpiryCodes')
    def main(self):
        self.check_truedata_download_test()
        self.generate_and_check_expiry_codes()
        self.set_expiry_codes_to_config_file()
        self.update_expiry_mapping_file()
        self.update_expiry_codes_in_db()