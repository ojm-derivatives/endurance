from rms.RMS_Main import RMS_Main
from rms.RMS_GenerateExpiryCodes import RMS_GenerateExpiryCodes
from rms.RMS_Position_Order_Matching import RMS_Position_Order_Matching
from rms.RMS_M2M import RMS_M2M
from dotenv import load_dotenv
import os

load_dotenv()

# declaring the below two objects in __init__ so that their resources can be accessed from anywhere in the project by just import rms
rms_main_obj = RMS_Main(module_name='RMS')
rms_m2m_obj = RMS_M2M('Endurance', 'RMS')