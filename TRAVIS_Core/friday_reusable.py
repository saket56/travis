''' 
    Created By: Rohit Abhishek 
    Function: This module is collections of reusable functions that travis require for various operations like 
              workspace creation, purging old directories from the system, dd credentials, emailer, generation of data chunks etc.

'''
import csv
import fnmatch
import getpass
import json
import logging
import math
import os
import queue
import random
import shutil
import sys
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from io import BufferedReader
from time import mktime, struct_time
from typing import Generator

import flatten_json
import pythoncom
import win32com.client as win32
import yaml
from cryptography.fernet import Fernet
from friday_exception import ProcessingException, ValidationException

mylogger =  logging.getLogger(__name__)

class CustomJSONEncoder(json.JSONEncoder):
    """Custom Json encoder to retrieve deciaml and date values"""

    def default(self, o):
        if isinstance(o, date):
            return str(o)
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, struct_time):
            return datetime.fromtimestamp(mktime(o))

        return super(CustomJSONEncoder, self).default(o)


# create a dataclass to hold message data 
@dataclass()
class StatusMessage:
    run_id:str
    root_option:str
    sub_option:str
    output_location:str
    status:str
    message:str


def get_yajl_location() -> str:
    """ get location of yajl file on the system """

    yajl_dll = None 

    # check if yajl file is bundled along or provided as separate folder 
    if os.path.exists(os.path.join(os.path.dirname(sys.executable), "library", "lib", "yajl.dll")):
        yajl_dll = os.path.join(os.path.dirname(sys.executable), "library", "lib", "yajl.dll")
    elif os.path.exists(os.path.join(os.path.dirname(sys.executable), "yajl.dll")):
        yajl_dll = os.path.join(os.path.dirname(sys.executable), "yajl.dll")
    elif os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "library", "lib", "yajl.dll")):
        yajl_dll = os.path.join(os.path.dirname(os.path.abspath(__file__)), "library", "lib", "yajl.dll")
    else:
        root_path = os.path.dirname(os.path.abspath(__file__))
        for root, dirs, files in os.walk(root_path):
            if "yajl.dll" in files:
                yajl_dll = os.path.join(root, "yajl.dll")    

    return yajl_dll


def get_function_name() -> str:
    """
    :return: name of caller
    """
    return sys._getframe(1).f_code.co_name

def put_status_message_queue(mainwin=None, queue:queue.Queue=None, message="") -> None:
    """ Put message on status queue """
    queue.put(message)
    mainwin.event_generate("<<MessageGenerated>>")    


def replace_escape_character(text:str="") -> str:
    """ replace escape characters for mongo command line run """
    if "@" in text:
        return text.replace("@", "%40")
    return text


def setup_user_workspace(workspace_directory:str="") -> 'tuple(str, dict)':
    """ loads TRAVIS configurations to a dictionary. 
        gets the workspace location from the configuration if not found, 
        creates user workspace at defaulted location
    """

    mylogger.info(get_function_name())

    if workspace_directory != "" and workspace_directory is not None:
        if os.path.exists(workspace_directory) and not os.path.isfile(workspace_directory): 
            workspace_directory = os.path.join(workspace_directory, getpass.getuser(), "Travis")
        elif os.path.exists(workspace_directory) and os.path.isfile(workspace_directory): 
            workspace_directory = os.path.join(os.path.dirname(workspace_directory), getpass.getuser(), "Travis")
        else:
            workspace_directory = os.path.join(os.path.expanduser("~/Documents"), "Travis")
    else:
        workspace_directory = os.path.join(os.path.expanduser("~/Documents"), "Travis")

    today = datetime.now()
    workspace = today.strftime('%Y%m%d%H%M%S')
    workspace_directory = os.path.join(workspace_directory, workspace)
    os.makedirs(workspace_directory) if not os.path.exists(workspace_directory) else None

    return workspace_directory


def get_config_data(config_location:str="") -> 'dict':
    """ loads yaml data to python dictionary for GUI set up and options """
    
    mylogger.info(get_function_name())

    # print (config_location)
    
    # Get current working directory and look for yaml file and load it
    if config_location == "" or config_location is None:
        config_location = os.path.join(os.path.dirname(os.path.abspath(__file__)),  "FridayConfig.yaml")

    # check if file exists in outer folder 
    if os.path.exists(config_location):
        with open(config_location,'r') as configFile:
            config = yaml.safe_load(configFile)
            return config 

    elif os.path.exists(os.path.join(os.path.dirname(config_location), "static", "FridayConfig.yaml")):
        with open(os.path.join(os.path.dirname(config_location), "static", "FridayConfig.yaml"), 'r') as configFile:
            config = yaml.safe_load(configFile)
            return config             
    else: 
        message = "Invalid YAML File Location"
        raise ProcessingException(message)


def purge_workspace_folders(current_location:'str' = "", parent_location:'str' = "", folder_list:'list'=[]) -> 'None':
    """ Remove folders from any location. Used for removing workpsace folders created over a period of time.
        If current location passed it will locate it will go one level up i.e. parent folder and remove all the sub-folders of the parent
        If parent location is paaaed it will remove sub folders in that location 
        If folder list is passed it will remove the folders mentioned in the list
    """
    
    # This function is called post application launch so it is ok to log
    mylogger.info(get_function_name())
    
    # check the current location and get the parent folder name 
    parent_subdirectory_list = []
    if current_location != "": 
        parent_directory = os.path.dirname(current_location)
        for directory in os.listdir(parent_directory):
            parent_subdirectory_list.append(os.path.join(parent_directory, directory))
    
    # if parent location provided, then get the list of sub directories 
    elif parent_location != "":
        parent_directory = os.path.abspath(parent_location)
        for directory in os.listdir(parent_directory):
            parent_subdirectory_list.append(os.path.join(parent_directory, directory))
    
    # else the copy the directory passed
    elif len(folder_list) > 0: 
        parent_subdirectory_list = folder_list
    
    else: 
        message = "Nothing to remove"
        raise ValidationException(message)
    
    # iterate over each sub folder of parent
    for folder in parent_subdirectory_list:

        try:
            shutil.rmtree(folder)
            mylogger.info('REMOVED ' + str(folder))
            
        except:
            mylogger.info('CANNOT DELETE ' + str(folder))


def return_latest_file(location: 'str' = "", file_filter: 'str' = "*.*") -> 'str':
    """ return latest file in a given location """ 

    mylogger.info(get_function_name() + ' ' + str(location) + ' ' + str(file_filter))

    files = fnmatch.filter(os.listdir(location), file_filter)
    paths = [ os.path.join(location, file) for file in files ]
    
    return max(paths, key=os.path.getctime)


def deduct_month(start_time:'datetime' = datetime.now(), number_of_month:'int'=0) -> 'datetime':
    """ Deduct number of months from the start time. Ensure start_time is object of datetime """
    
    mylogger.info(get_function_name() + ' ' + str(start_time) + ' ' + str(number_of_month))
    
    date=start_time.date().replace(day=1) - timedelta(days=1)

    for i in range(number_of_month):
        last_day = date.replace(day=1) - timedelta(days=1)
        date = last_day
    final_date = date.replace(day=1)

    return final_date


def validate_folder_location(input_location:'str' = "") -> 'tuple(bool, str)':   
    ''' VALIDATE IF FOLDER LOCATION EXISTS '''
    
    mylogger.info(get_function_name() + ' ' + str(input_location))
    
    valid_ind = True
    message = 'Success'
    
    if not os.path.exists(input_location):
        valid_ind = False
        message = 'Invalid input location ' + str(input_location)
        return valid_ind, message
    
    return valid_ind, message


def validate_file_location(file_location:'str' = "", file_list:'list'=[]) -> 'tuple(bool, str)':
    ''' VALIDATE IF FILE IS PRESENT OR NOT '''
    
    mylogger.info(get_function_name() + str(file_location) + ' file list ' + str(file_list))
    
    valid_ind = True
    message = 'Success'

    # check if file location exists and is file 
    if os.path.exists(file_location) and os.path.isfile(file_location):
        valid_ind = True 
        return valid_ind, message
    
    # check if folder exists 
    if not os.path.exists(file_location):
        message = 'Folder doesnt exist: ' + str(file_location)
        valid_ind = False
        return valid_ind, message
    
    # check if file names provided and create a list of input file names 
    if (len(file_list) == 0 or '*' in file_list):
        input_filter = ["*",]
    else:
        input_filter = file_list.copy()

    # check if any files matching to filter condition 
    files = []
    for filter in input_filter: 
        for base in fnmatch.filter(os.listdir(file_location), filter): 
            file_name = os.path.join(file_location, base)

            if '*' in filter and os.path.isdir(file_name):
                continue

            if not os.path.isfile(file_name):
                message = "Is not a file: " + str (file_name)
                valid_ind = False 
                return valid_ind, message

            files.append(file_name)
    
    # check if any file picked for compare
    if len(files) == 0:
        message = "No file found in: " + str(file_location)
        valid_ind = False 
        return valid_ind, message
    
    return valid_ind, message    


def create_subfolder(location_path:'str' = "", current_location:'str'="", sub_folder_name:'str' = 'default', rename_existing:'bool'=True) -> 'str':
    ''' CREATE WORKSPACE SUB FOLDERS FOR OUTPUT DATA '''
    
    mylogger.info(get_function_name() + " " + str(location_path) + ' WORKSPACE FOLDER ' + str(current_location) + ' SUBFOLDER REQUESTED: ' + str(sub_folder_name))
    
    # check if output location exists
    if location_path != None and os.path.exists(location_path) :
        location = os.path.abspath(location_path)
    else:
        location = os.path.abspath(current_location)
        
    # join incoming location and subfolder name
    sub_folder = os.path.join(location, sub_folder_name)
    
    # check if path exists
    if os.path.exists(sub_folder) and rename_existing:
        today = datetime.now()
        os.rename(sub_folder, os.path.join(location, today.strftime('%Y%m%d%H%M%S') + '_' + sub_folder_name + '_backup'))
        os.mkdir(sub_folder)
    elif not os.path.exists(sub_folder):
        os.mkdir(sub_folder)

    return sub_folder    


def create_chunks_gen(list_data:'list'=[], nsize:'int'=0) -> 'Generator':
    """ creates generator object for list of sublist """
    
    mylogger.info(get_function_name())
    
    if nsize == 0:
        nsize = 1

    for i in range(0, len(list_data), nsize):
        yield list_data[i:i+nsize]


def create_chunks(list_data:'list' = [], number_of_elements_in_chunk:int=None, number_of_chunks:int=None):
    """ create smaller chunks of size equal to nsize value passed. 
    Say 100 items and nsize is 2 it will create 50 smaller chunks with 2 element in each chunk """

    mylogger.info(get_function_name())

    if number_of_elements_in_chunk is not None:
        list_with_chunk = [list_data[i:i+number_of_elements_in_chunk] for i in range (0,len(list_data), number_of_elements_in_chunk)]
    elif number_of_chunks is not None:
        number_of_elements_in_chunk = math.ceil(len(list_data) / number_of_chunks)
        list_with_chunk = [list_data[i:i+number_of_elements_in_chunk] for i in range (0,len(list_data), number_of_elements_in_chunk)]
    elif number_of_chunks is None and number_of_elements_in_chunk is None:
        number_of_chunks = 1
        number_of_elements_in_chunk = math.ceil(len(list_data) / number_of_chunks)
        list_with_chunk = [list_data[i:i+number_of_elements_in_chunk] for i in range (0,len(list_data), number_of_elements_in_chunk)]        

    return list_with_chunk          


def create_chunks_dict(dict_data:dict = {}, number_of_elements_in_chunk:int=1, number_of_chunks:int=1):
    """ create smaller chunks of size equal to nsize value passed. 
    Say 100 items and nsize is 2 it will create 50 smaller chunks with 2 element in each chunk """

    mylogger.info(get_function_name())

    list_with_chunk = [] 

    if number_of_elements_in_chunk > 1:
        # create list with chunks
        for chunk in get_dict_chunk(dict_data, number_of_elements_in_chunk=number_of_elements_in_chunk):
            if chunk:
                list_with_chunk.append(chunk) 

    elif number_of_chunks >= 1:
        number_of_elements_in_chunk = math.ceil(len(list(dict_data)) / number_of_chunks)

        for chunk in get_dict_chunk(dict_data, number_of_elements_in_chunk=number_of_elements_in_chunk):
            if chunk:
                list_with_chunk.append(chunk)             
    
    return list_with_chunk    


def get_dict_chunk(dict_data, number_of_elements_in_chunk=1):

    output_dict = {} 

    for index, value in enumerate(dict_data, 1):
        if index % number_of_elements_in_chunk == 0: 
            yield output_dict
            output_dict = {} 
        output_dict[value] = dict_data[value]

    if output_dict:
        yield output_dict


def get_file_filter (file_list:'list'=[]) -> 'list':
    """ call this routine to get list of files entered by the user on gui """
    
    mylogger.info(get_function_name())
    
    file_filter = []
    if len(file_list) == 0 or '*' in file_list:
        file_filter = ['*', ]
    else:
        file_temp_filter = file_list 
        file_filter = list(sorted(set(file_temp_filter), key=file_temp_filter.index))

    return file_filter


def merge_multiple_temp_files(output_location:'str'="", input_file_pattern:'str'="*.csv", output_file_name:'str'="", output_encoding:'str'="utf-8", first_record:str="", remove_temp_files:bool=True) -> None:
    """ merge multiple csv/psv/tsv files to one. make sure there are no headers """

    mylogger.info(get_function_name())

    output_list = fnmatch.filter(os.listdir(output_location), input_file_pattern)

    # to reduce the elapsed time try to merge the data during compare time 
    output_file = open(os.path.join(output_location, output_file_name), "w", encoding=output_encoding)
    if first_record != "":
        output_file.write(first_record)
    
    # iterate over each file and merge it to single csv
    for file in output_list:

        output_smaller_file = open(os.path.join(output_location, file), "r", encoding=output_encoding)
        output_file.write(output_smaller_file.read())
        
        # close and remove smaller file 
        output_smaller_file.close()
        os.remove(os.path.join(output_location, file)) if remove_temp_files else None

    output_file.close()


def get_dd_credentials(dd_config_location:'str'="")-> 'tuple(str, str, str)':
    """ Returns host name, api_key, app_key 
    When calling this function pass a valid absolute location where your Datadog credentials are stored on local machine. If root locaiton is not passed the routine will search c:\\users\\<user-id>\\.datadog\\config file for credentials. Otherwise it will look into <passed-absolute-location>\\config
    If location specified or default location is not present. The routine will raise Exception (FileNotFound)
    Handle this exception in your program """

    mylogger.info(get_function_name())

    HOST = ""
    APP_KEY = ""
    API_KEY = ""
    path = ""
    # evaluate passed location 
    if dd_config_location != "":
        if not os.path.exists(os.path.abspath(dd_config_location)):  
            print ("Location ", dd_config_location, "doest not exist")
            raise FileNotFoundError

        if not os.path.isfile(os.path.join(dd_config_location, "config")): 
            print ("config File not present in ", dd_config_location, "locaiton")
            raise FileNotFoundError

        path = dd_config_location

    else: 
        user = getpass.getuser()
        path = os.path.expanduser("~\.datadog")

        if not os.path.exists(path):
            print ("Location ", path, "doest not exist")
            raise FileNotFoundError

        if not os.path.isfile(os.path.join(path, "config")): 
            print ("config File not present in ", path, "location")
            raise FileNotFoundError        

    # open the file and read thru the keys 
    with open(os.path.join(path, "config"), "r") as config_file: 

        for line in config_file: 
            key, value = line.split("=")

            if key.strip() == "HOST": 
                HOST = value 
            elif key.strip() == "APP_KEY": 
                APP_KEY = value 
            elif key.strip() == "API_KEY": 
                API_KEY = value

    return HOST.strip(), API_KEY.strip(), APP_KEY.strip()


def send_notification(email_to_list = [], email_cc_list = [], email_subject = "Test Email", email_html_body = "", email_body = "", email_attachments = [], email_from = "DeloitteCMSAutomation"): 

    """ 
        Send notification using outlook. call this routine with following inputs: 
    """

    mylogger.info(get_function_name())

    # default to running use if no email id found
    if len(email_to_list) <= 0: 
        email_to_list.append(getpass.getuser() + "@anthem.com") 

    # create outlook email object 
    outlook = win32.dynamic.Dispatch('Outlook.Application', pythoncom.CoInitialize())

    # create new mail item
    mail = outlook.CreateItem(0)

    # set up data 
    mail.To = ';'.join(email_to_list)

    # if len(email_cc_list) > 0: 
    #     mail.Cc = ';'.join(email_cc_list)

    # set subject details 
    mail.Subject = email_subject

    # set mail body 
    if email_body != "": 
        mail.Body = email_body

    if email_html_body != "": 
        mail.HTMLBody = email_html_body

    # set attachments 
    for item in email_attachments: 
        mail.Attachments.Add(item)    

    # count number of accounts set up on current user outlook and try to send it using CMS Mailbox
    account_count = outlook.Session.Accounts.Count

    # check if emailer should be sent from central mailbox
    if account_count > 1 and email_from != "":
        for i in (1, account_count):
            account_name = outlook.Session.Accounts.Item(i).DisplayName
            account_name_prefix, _ =  account_name.split('@')
            if account_name_prefix.lower() == str(email_from).lower():
                cms_automation_account = outlook.Session.Accounts.Item(i)
                mail._oleobj_.Invoke(*(64209, 0, 8, 0, cms_automation_account))
    
    # send the email
    mail.Send()  



def perform_condition_operation(column_value:'str' = "", target_value:'str' = "", operator:'str' = "") -> 'bool':
    """ check if column value and target value based on passed operation """

    mylogger.info(get_function_name())

    if operator.upper() == 'EQ' or operator == '==':
        return column_value == target_value
    elif operator.upper() == 'GT' or operator == '>':
        return column_value > target_value
    elif operator.upper() == 'GE' or operator == '>=':
        return column_value >= target_value
    elif operator.upper() == 'LT' or operator == '<':
        return column_value < target_value
    elif operator.upper() == 'LE' or operator == '<=':
        return column_value <= target_value
    elif operator.upper() == 'NE' or operator == '<>' or operator == '!=':
        return column_value != target_value 


def get_account_list(landing_zone = "", account_status = "", random_samples = 0, account_samples=[], account_root="//ntfs07/SHARED/Reference Data Storage/CMS/AccountList/AccountLists.csv"):
    """ Get dictionary of AWS Accounts with list of attributes as position bound i.e. in following format:  
    { "Account Number" : ["Account Number", "landingzone name", "landing zone env name", "status", "Account name", "Src Env", "Business unit] }
    
    Call this function with combination of two parameters:  
    1. Account Name: 
        a. landing_zone = "leglz_prod" returns a legacy production accounts alone
        b. landing_zone = "leglz_nprod" returns a legacy non production accounts alone
        c. landing_zone = "newlz_prod" returns a new lz production accounts
        d. landing_zone = "newlz_nprod" returns a new lz non-production accounts 
        e. landing_zone = "testlz_nprod" returns test non production accounts 
        f. landing_zone = "new" returns newlz accounts and test lz accounts (prod and non-prod)
        g. landing_zone = "leg" returns legacy accounts (prod and non-prod)
        h. landing_zone = "" return all
    2. Account Status
        a. account_status = "active" - return only active and unknown status accounts (unknown for TESTLZ)
        b. account_status = "inactive" - returns inactive, 
        c. account_status = "pending" - returns pending, 
        d. account_status = "suspended" - returns suspended accounts 
        c. account_status = "" - return all 

    Raises exception if any in-scope account number is not found in the AWS configuration file.
    """    

    mylogger.info(get_function_name())

    master_dictionary = {} 
    sample_dictionary = {} 

    # read account details from CSV file on NAS location 
    account_file = open(account_root, "r")
    csvreader = csv.reader(account_file)

    # skip header 
    header = next(csvreader, None)

    # counter variable 
    counter = 0

    for row in csvreader:

        # override unknown status to active - This for TESTLZ
        status = row[3].strip().upper()

        if status == "UNKNOWN": 
            status = "ACTIVE"
        
        # check if parameters passed found. DONT USE ELSE statement otherwise everything will be included
        if landing_zone.strip().upper() == row[2].strip().upper() and account_status.strip().upper() == status: 
            master_dictionary[row[0]] = row

        elif landing_zone.strip().upper() == row[2].strip().upper() and account_status.strip().upper() == "":  
            master_dictionary[row[0]] = row

        elif landing_zone.strip().upper() == "" and account_status.strip().upper() == status:
            master_dictionary[row[0]] = row

        elif landing_zone.strip().upper() == "" and account_status.strip().upper() == "":
            master_dictionary[row[0]] = row            

        elif landing_zone.strip().upper() in row[2].strip().upper() and account_status.strip().upper() == status:
            master_dictionary[row[0]] = row   

        elif landing_zone.strip().upper() in row[2].strip().upper() and account_status.strip().upper() == "":
            master_dictionary[row[0]] = row   

    # check if random selection needed
    if random_samples > 0 : 
        random_keys = random.sample(master_dictionary.keys(), random_samples)
        sample_dictionary = { k : master_dictionary[k] for k in random_keys } 
    elif len(account_samples) > 0:
        sample_dictionary = { k : master_dictionary[k] for k in account_samples } 
    else: 
        sample_dictionary = master_dictionary.copy()

    all_accounts = list(sample_dictionary.keys())

    # read through the config file and store the accounts in a list 
    user = getpass.getuser()
    path = os.path.expanduser("~\.aws")        
    profile_error = False 

    # find if location or file exists
    if not os.path.exists(path):
        print ("Location ", path, "does not exist")
        raise FileNotFoundError

    if not os.path.isfile(os.path.join(path, "config")): 
        print ("config File not present in ", path, "location")
        raise FileNotFoundError    

    config_accounts = []

    # read through the file 
    with open(os.path.join(path, "config"), "r") as aws_config: 

        # read each line in the file
        for line in aws_config: 

            # check if role arn present in the file
            if "role_arn" in line: 
                _, arn_value = line.split('=')
                _,_,_,_,account_number,_ = arn_value.split(":")
                config_accounts.append(account_number)

    # initialize config missing file 
    with open (os.path.join(path, "config_missing"), "w") as missing: 
        pass

    config_missing = False 

    # validate if all_accounts are present in 
    for account in all_accounts: 
        if account not in config_accounts: 
            config_missing = True
            with open (os.path.join(path, "config_missing"), "a") as missing: 
                row = sample_dictionary[account]
                missing.write('[profile ' + str(account) + ']' + '\n')
                missing.write('output = json' + '\n')
                missing.write('region = us-east-1' + '\n')

                if row[1] == "newlz": 
                    missing.write ('role_arn = arn:aws:iam::' + account + ':role/antm-cloudops' + '\n')
                    missing.write ('source_profile = newlz' + '\n')                         
                elif row[1] == "legacylz":
                    missing.write ('role_arn = arn:aws:iam::' + account + ':role/CloudOperationsExecutionRole' + '\n')
                    missing.write ('source_profile = legacylz' + '\n')     
                elif row[1] == "testlz": 
                    missing.write ('role_arn = arn:aws:iam::' + account + ':role/antm-cloudops' + '\n')
                    missing.write ('source_profile = testlz' + '\n')

                missing.write("\n")

    # if something was NOT written to missing_config file - delete it to avoid confusion 
    if config_missing: 
        raise Exception("Missing accounts in configuration file. config_missing file created in your AWS Configuration Folder " + path)
    else: 
        os.remove(os.path.join(path, "config_missing"))

    return header, sample_dictionary


def perform_data_setup(output_location, mypath, sub_option, application_name, environment_name, rename_existing=False):
    """ set up loot and output file location """ 
    
    mylogger.info(get_function_name())

    # output_root             = create_subfolder(output_location, mypath, str(sub_option), rename_existing=rename_existing)
    # output_file_location    = create_subfolder(output_root, mypath, str(application_name) + '_' + str(environment_name), rename_existing=True)

    output_root             = create_subfolder(output_location, 
                                               mypath, 
                                               str(application_name) + '_' + str(environment_name), 
                                               rename_existing=rename_existing)
    output_file_location    = create_subfolder(output_root,
                                               mypath, 
                                               str(sub_option), 
                                               rename_existing=True)
    return output_file_location


def get_all_files_dict(files: list=None, file_location:str="", file_type: str="Base") -> list:
    """List all files using file filter and location entered by the user"""

    mylogger.info(get_function_name())

    if files is None:
        files=[]

    file_dict = OrderedDict()

    # evaluate file type and get list of file filters
    file_filters = get_file_filter(files)
        
    # iterate over each file filter and create a list of absolute file path
    index = 0

    # check if file filter has "*" value only 
    if len(file_filters) == 1 and file_filters[0] == "*":
        # perform os walk to get the all the files for base and release side 
        for croot, cdirs, cfiles in os.walk(file_location):
            for f in cfiles: 
                if os.path.isfile(os.path.join(croot, f)):
                    file_dict[os.path.join(croot, f)] = index 
                    index += 1
    else: 
        for filter in file_filters:
            directory_list = fnmatch.filter(os.listdir(file_location), filter)

            # create file dictionary
            for file in directory_list:
                file_path = os.path.join(file_location, file)
                file_dict[file_path] = index
                index += 1

    return file_dict


def get_file_handles(file_name:str="", file_codepage:str="utf-8", file_delimiter:str=",", file_type:str="JSON") -> 'tuple[BufferedReader, Generator]':
    """ create file handles for read """

    mylogger.info(get_function_name())

    yajl_dll = get_yajl_location()
    os.environ["YAJL_DLL"] = yajl_dll

    # import json streamer c type parser
    import ijson.backends.yajl2_cffi as ijson

    # set variables
    json_array = False 
    stream_file = None 
    stream_json = None

    if file_type.upper() == "CSV":
        stream_file = open(file_name, "r", encoding=file_codepage, newline="")
        stream_json = csv.reader(stream_file, delimiter=file_delimiter)
    else:
        # check if single json document file or multiple json document file 
        with open(file_name, "rb") as fp:
            json_obj = ijson.parse(fp)
            index, event, value = next(json_obj)
            if event == "start_array":
                json_array = True
        
        if json_array:
            stream_file = open(file_name, "rb")
            stream_json = ijson.items(stream_file, "item")            
        else: 
            stream_file = open(file_name, "r")
            stream_json = json.load(stream_file)

    return stream_file, stream_json


def get_flat_json(data=None, header:list=[], separator:str=".", file_type:str="JSON") -> dict:
    """ Create Ordered Dictionary of stream of data """    
    
    mylogger.info(get_function_name())

    if file_type.upper() == "CSV":
        return OrderedDict(zip(header, data))
    else:
        return flatten_json.flatten(data, separator)
    


def get_json_keys(json_data, flat_json_data, file_name, key_data, mongo_extract=False) -> 'tuple[list, list]':
    """ get json key """

    mylogger.info(get_function_name())

    json_key = [] 
    json_exception_list = []

    # check if mongo extract is set to True
    if mongo_extract:
        if isinstance(json_data["_id"], dict):
            json_key.append(str(json_data["_id"]["$oid"]))
        elif isinstance(json_data["_id", str]):
            json_key.append(str(json_data["_id"]))
        else:
            json_exception_list.append(f"Exception in processing {file_name} \n" + str(json_data))
    else: 
        for x in key_data:
            if x.strip() in flat_json_data.keys(): 
                json_key.append(str(flat_json_data[x]))
            
            elif "[" in x:
                key_name, start_end_pos = x.split("[")
                key_name = key_name.strip()
                start_end_pos = start_end_pos.strip()

                start_pos, end_pos = start_end_pos.split(":")
                end_pos = end_pos.strip("]")

                start_pos=int(start_pos.strip())
                end_pos=int(end_pos.strip())

                json_key.append(str(flat_json_data[key_name][start_pos:end_pos]))
            else:
                json_exception_list.append(f"Exception in processing {file_name} \n" + str(json_data))
                break

    return json_key, json_exception_list


def parse_aws_resource_arn(resource_arn:str="") -> 'tuple[str, str, str]':
    """ parse resource arn passed """

    resource_arn_list = resource_arn.split(":")
    service_name = resource_arn_list[2]
    resource_type = ""
    resource_name = ""

    # check if the length of resource is 8 
    if len(resource_arn_list) == 8:
        resource_type = resource_arn_list[-3]
    elif len(resource_arn_list) == 7:
        resource_type = resource_arn_list[-2]
    elif len(resource_arn_list) == 6 and "/" in resource_arn_list[-1]:
        resource_type = resource_arn_list[-1].split("/")[0]
    else:
        resource_type = ""

    # get resource name 
    resource_name = service_name + ":" + resource_type 
    if resource_type == "":
        resource_name = service_name 

    return service_name, resource_type, resource_name


def get_travis_tokens(travis_key, file_location:str=None) -> tuple:
    """ get travis tokens for validation """

    current_date, start_date, days_used, valid_days = None, None, None, None

    # override token file with file in current folder
    if os.path.exists(os.path.join(os.path.dirname(sys.executable), "travis.dat")):
        file_location = os.path.join(os.path.dirname(sys.executable), "travis.dat")

    if file_location is None or file_location == "":
        return current_date, start_date, days_used, valid_days 

    if not os.path.isfile(file_location):
        return current_date, start_date, days_used, valid_days
    
    if travis_key is None or travis_key == "":
        return current_date, start_date, days_used, valid_days
    
    try:
        # read the file and store as bytes 
        token_file = open(file_location, "r")
        token_data = token_file.read()

        # if file key data is not populated return back
        if token_data is None or token_data == "":
            return current_date, start_date, days_used, valid_days

        # create encrypted format of token data 
        token_data_enc = bytes(token_data, "utf-8")

        # create fernet object 
        travis_key_enc = bytes(travis_key, "utf-8")
        travis_fernet_key = Fernet(travis_key_enc)

        # decrpt token data using travis key 
        decrypted_string = travis_fernet_key.decrypt(token_data_enc)

        # get start date and valid days
        start_date, valid_days = decrypted_string.decode().split("#")
        start_date_formatted = datetime.fromtimestamp(float(start_date))

        # get system date and format the dates 
        current_date =  time.time()
        current_date_formatted = datetime.fromtimestamp(current_date)
        
        # get the date difference 
        date_difference = current_date_formatted - start_date_formatted

        # hours used 
        hours_used = date_difference.total_seconds()/3600
        days_used = hours_used/24

        return current_date, start_date, days_used, valid_days

    except:
        return current_date, start_date, days_used, valid_days
    

