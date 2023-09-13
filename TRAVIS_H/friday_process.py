''' 
    Created By: Rohit Abhishek 
    Function: This module is collection of various operations to be performed on data.
              This module will accept the data from the GUI interface and performs operations based on the call made by the GUI program.
              Has interface with exception module
'''

import base64
import csv
import ctypes as ct
import datetime
import fnmatch
import hashlib
import json
import logging
import math
import multiprocessing
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import threading
import tkinter
import webbrowser
from abc import ABC
from collections import OrderedDict
from concurrent.futures import process
from datetime import date, datetime
from decimal import Decimal
from itertools import zip_longest
from multiprocessing import Process, get_context
from threading import Thread
from time import mktime, struct_time, time
from typing import Callable

# import boto3
import flatten_json
import friday_reusable
import openpyxl
import pandas as pd
from bson import json_util
from friday_exception import ProcessingException, ValidationException
from jinja2 import Environment, FileSystemLoader
from pymongo import MongoClient, errors
import sqlalchemy

# set variables for yajl
yajl_dll = os.path.join(os.path.dirname(sys.executable), "Library", "lib", "yajl.dll")
os.environ["YAJL_DLL"] = yajl_dll

# import json streamer c type parser
import ijson.backends.yajl2_cffi as ijson

mylogger = logging.getLogger(__name__)

# change csv default size
csv.field_size_limit(int(ct.c_ulong(-1).value // 2))



class ThreadWithReturnValue(threading.Thread):
    """Threads sending return value to main process"""

    def __init__(self, group=None, target=None, name=None, args=(), kwargs={}):
        def function():
            self.result = target(*args, **kwargs)

        super().__init__(group=group, target=function, name=name)


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


class CreateCompareDatabase:
    """ Interface to execute SQLite3 Queries for variaous Compares """

    def __init__(self, dbname) -> None:
        """ initialize """

        self.dbname = dbname
        self.local = threading.local()

    def get_connection(self)-> sqlite3.Connection:
        """ get connection using object's lock """

        mylogger.info(friday_reusable.get_function_name())

        if not hasattr(self.local, "connection"):
            self.local.connection = sqlite3.connect(
                self.dbname, check_same_thread=False
            )

        return self.local.connection

    def run_attach_sql(self, attach_db, sql, attach_db_name="RELEASE_DB") -> list:
        """ run query using ATTACH Functionality """

        mylogger.info(friday_reusable.get_function_name())

        connection = self.get_connection()
        cursor = connection.cursor()
        attach_sql = f"ATTACH DATABASE ? AS {attach_db_name}"
        cursor.execute(attach_sql, attach_db)
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.execute(f"DETACH DATABASE {attach_db_name}")
        connection.commit()
        cursor.close()

        return rows
    

    def run_attach_insert_sql(self, attach_db, sql, attach_db_name="RELEASE_DB") -> list:
        """ run query using ATTACH Functionality """

        mylogger.info(friday_reusable.get_function_name())

        connection = self.get_connection()
        cursor = connection.cursor()
        attach_sql = f"ATTACH DATABASE ? AS {attach_db_name}"
        cursor.execute(attach_sql, attach_db)
        cursor.execute(sql)
        cursor.execute(f"DETACH DATABASE {attach_db_name}")
        connection.commit()
        cursor.close()

        return 


    def insert_data(self, table_name, data) -> None:
        """ insert data to the sqlite database """

        mylogger.info(friday_reusable.get_function_name())

        connection = self.get_connection()
        cursor = connection.cursor()

        # create INSERT sql
        for row in data:
            cursor.execute(
                f"INSERT INTO {table_name} VALUES ({','.join(['?']*len(row))});", row
            )
        connection.commit()
        cursor.close()


    def run_ddl(self, ddl) -> None:
        """ run any DDL statement or SQL query where return is not expected """

        mylogger.info(friday_reusable.get_function_name())

        connection = self.get_connection()
        cursor = connection.cursor()
        cursor.execute(ddl)
        connection.commit()
        cursor.close()

    def get_rows(self, select_ddl) -> list:
        """ run sql to retrieve rows from the table """

        mylogger.info(friday_reusable.get_function_name())

        connection = self.get_connection()
        cursor = connection.cursor()
        cursor.execute(select_ddl)
        rows = cursor.fetchall()
        connection.commit()
        cursor.close()

        return rows

    def disconnect(self) -> None:
        """ disconnect from the database """
        
        mylogger.info(friday_reusable.get_function_name())

        connection = self.get_connection()
        connection.close()


class CompareFiles(ABC):
    """ Compare file abstract class """

    def __init__(self, config: dict={}, root_option: str="", sub_option: str="", mypath: str="", progress_label: tkinter.Label=None, gui_config: dict={}, application_name: str="", environment_name: str="", open_browser:bool=True, merge_match_unmatch:bool=True) -> None:

        # initialize variables
        self.__config = config
        self.__root_option = root_option
        self.__sub_option = sub_option
        self.__mypath = mypath
        self.__progress_label = progress_label
        self.__gui_config = gui_config
        self.__application_name = application_name
        self.__environment_name = environment_name
        self.__open_browser = open_browser
        self.__merge_match_unmatch = merge_match_unmatch

        # set configurations needed for comparison
        self.base_config = self.__config.get("BaseConfig")
        self.release_config = self.__config.get("ReleaseConfig")
        self.compare_config = self.__config.get("CompareConfig")
        self.output_config = self.__config.get("OutputConfig")

        # get template directories
        self.__present_working_dir = os.path.dirname(os.path.abspath(__file__))
        self.__template_location = os.path.join(self.__present_working_dir, "templates")
        self.__env = Environment(loader=FileSystemLoader(self.__template_location))
        self.template = self.__env.get_template("compare_report.html")

        # add a variable for debugging
        self.output_retain_temp_files = True
        if "Output_Retain_Temp_Files" not in self.output_config.keys():
            self.output_retain_temp_files = False 

        # create workspace directory
        self.output_location = friday_reusable.perform_data_setup(
            self.output_config.get("Output_Location", ""),
            self.__mypath,
            str(self.__sub_option),
            self.__application_name,
            self.__environment_name,
            rename_existing=False,
        )            

        # set variables
        self.start_time = datetime.now()
        self.end_time = datetime.now()
        self.message = ""
        self.count_table = None
        self.mismatch_table = None
        self.output_header = [
            "Key",
            "Field_Name",
            "Base_Value",
            "Release_Value",
            "Remarks",
        ]

        # get base configurations into variables 
        self.base_location = None 
        self.base_files = None
        self.base_file_code_page = None 
        self.base_file_delimiter = None

        # get release configurations into variables 
        self.release_location = None 
        self.release_files = None
        self.release_file_code_page = None
        self.release_file_delimiter = None

        # get compare configurations into variables 
        self.compare_file_keys = None 
        self.compare_skip_fields = None
        self.compare_case_sensitive = None
        self.compare_match_flag = None 
        self.compare_processor_limit = None 
        self.compare_batch_size = None
        self.compare_parent_child_sep = None

        # get output configurations into variables 
        self.output_location_dir = None 
        self.output_file_code_page = None 
        self.output_file_delimiter = None
        self.output_store_base_release = None
        self.output_generate_summary_flag = None

        # master database file location 
        self.master_db_file = None

        # add a flag 
        self.side_by_side_report = None

        # get template, unmatch, match and out of sequence files
        self.unmatch_file_name = "Unmatch_File.csv"
        self.match_file_name = "Match_File.csv"
        self.out_of_seq_file_name = "Out_Of_Sequence.csv"
        self.exception_file_name = "Exception_File.log"
        self.merged_file_name = "Merged.csv"

        # some base variables
        self.base_file_name = []
        self.base_absolute_file_name = []
        self.base_record_count = []
        self.base_exception_count = []
        self.base_file_sublist = []

        # some release variables
        self.release_file_name = []
        self.release_absolute_file_name = []
        self.release_record_count = []
        self.release_exception_count = []
        self.release_file_sublist = []

        # some compare configuration variables
        self.files_ignored = []
        self.compare_records_len = []
        self.oos_obj_len = []

        # create some additional variables for validation 
        self.amount_regex = re.compile(r'''^\d*\.?\d*$''',re.VERBOSE)
        self.mongo_extract = False


    # set the getter setter property for config
    @property
    def config(self):
        """ getter and setter property """
        return self.__config

    @config.setter
    def config(self, config):
        if bool(config): 
            self.message = "Invalid Configuration Settings"
            raise ValidationException(self.message)
        
        if not bool(config):
            self.__config = config

    # set the getter setter property for root_option
    @property
    def root_option(self):
        """ getter and setter property """
        return self.__root_option

    @root_option.setter
    def root_option(self, root_option):
        if root_option == "": 
            self.message = "First option cannot be spaces"
            raise ValidationException(self.message)
        
        if not bool(root_option) and root_option != "":
            self.__root_option = root_option

    # set the getter setter property for sub_option
    @property
    def sub_option(self):
        """ getter and setter property """
        return self.__sub_option

    @sub_option.setter
    def sub_option(self, sub_option):

        if sub_option == "": 
            self.message = "Sub option cannot be spaces"
            raise ValidationException(self.message)
        
        if not bool(sub_option) and sub_option != "":
            self.__sub_option = sub_option

    # set the getter setter property for mypath
    @property
    def mypath(self):
        """ getter and setter property """
        return self.__mypath

    @mypath.setter
    def mypath(self, mypath):
        if not bool(mypath) and mypath != "":
            self.__mypath = mypath

    # set the getter setter property for progress_label
    @property
    def progress_label(self):
        """ getter and setter property """
        return self.__progress_label

    @progress_label.setter
    def progress_label(self, progress_label):
        if progress_label is None: 
            pass

        if not bool(progress_label):
            self.__progress_label = progress_label

    # set the getter setter property for gui_config
    @property
    def gui_config(self):
        """ getter and setter property """
        return self.__gui_config

    @gui_config.setter
    def gui_config(self, gui_config):
        if not bool(gui_config):
            self.__gui_config = gui_config

    # set the getter setter property for application_name
    @property
    def application_name(self):
        """ getter and setter property """
        return self.__application_name

    @application_name.setter
    def application_name(self, application_name):
        if application_name == "": 
            self.message = "Application name cannot be spaces"
            raise ValidationException(self.message)

        if not bool(application_name):
            self.__application_name = application_name

    # set the getter setter property for environment_name
    @property
    def environment_name(self):
        """ getter and setter property """
        return self.__environment_name

    @environment_name.setter
    def environment_name(self, environment_name):
        if environment_name == "": 
            self.message = "Environment name cannot be spaces"
            raise ValidationException(self.message)

        if not bool(environment_name):
            self.__environment_name = environment_name


    # set the getter setter property for environment_name
    @property
    def open_browser(self):
        """ getter and setter property """
        return self.__open_browser

    @open_browser.setter
    def open_browser(self, open_browser):
        self.__open_browser = open_browser


    # set the getter setter property for environment_name
    @property
    def merge_match_unmatch(self):
        """ getter and setter property """
        return self.__merge_match_unmatch

    @merge_match_unmatch.setter
    def merge_match_unmatch(self, merge_match_unmatch):
        self.__merge_match_unmatch = merge_match_unmatch


    # set the getter property for present_working_directory
    @property
    def present_working_directory(self):
        """ getter property """
        return self.__present_working_dir


    def _validate_base_folder_and_files(self) -> None:
        """Validate base folder and file configurations"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Base")

        # get the base folder and file details 
        self.base_location = self.base_config.get("Base_Location", None)
        self.base_files = self.base_config.get("Base_Files", None)

        # base location validation
        if self.base_location is None:
            self.message = "Corrupted Request Set up for base location. Please correct configurations"
            raise ValidationException(self.message)

        # base files validation
        if self.base_files is None:
            self.message = "Corrupted Request Set up for base files. Please correct configurations"
            raise ValidationException(self.message)

        # checks if Base Location mentioned
        if self.base_location == "":
            self.message = "Invalid Base Location. Please correct the data"
            raise ValidationException(self.message)

        # check if base location is present
        validInd, self.message = friday_reusable.validate_folder_location(self.base_location)
        if not validInd:
            raise ValidationException(self.message)

        # check if base locaion has file present
        validInd, self.message = friday_reusable.validate_file_location(self.base_location, 
                                                                        self.base_files)
        if not validInd:
            raise ValidationException(self.message)

        # if base file is not given in list format
        if not isinstance(self.base_files, list):
            self.message = "Base File must be a list. Please correct the data"
            raise ValidationException(self.message)


    def _validate_base_details(self) -> None:
        """Validate base batch size and code page configurations"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Base")

        self.base_file_code_page = self.base_config.get("Base_File_Code_Page", None)

        # base files code page
        if self.base_file_code_page is None:
            self.message = "Corrupted Request Set up for base file code page. Please correct the configurations"
            raise ValidationException(self.message)


    def _validate_base_csv_details(self) -> None:
        """Validate base csv configurations"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Base")

        self.base_file_delimiter = self.base_config.get("Base_File_Delimiter", None)

        # base delimiter validation
        if self.base_file_delimiter is None:
            self.message = "Corrupted Request Set up for base delimiter. Please correct the configurations"
            raise ValidationException(self.message)


    def _validate_release_folder_and_files(self) -> None:
        """Validate release configurations for CSV Files"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Release")

        # get the base folder and file details 
        self.release_location = self.release_config.get("Release_Location", None)
        self.release_files = self.release_config.get("Release_Files", None)        

        # release location validation
        if self.release_location is None:
            self.message = "Corrupted Request Set up for release location. Please correct configurations"
            raise ValidationException(self.message)

        # release files validation
        if self.release_files is None:
            self.message = "Corrupted Request Set up for release files. Please correct configurations"
            raise ValidationException(self.message)

        # checks if release Location mentioned
        if self.release_location == "":
            self.message = "Invalid release Location. Please correct the data"
            raise ValidationException(self.message)

        # check if release location is present
        validInd, self.message = friday_reusable.validate_folder_location(self.release_location)
        if not validInd:
            raise ValidationException(self.message)

        # check if release locaion has file present
        validInd, self.message = friday_reusable.validate_file_location(self.release_location,
                                                                        self.release_files)
        if not validInd:
            raise ValidationException(self.message)

        # if release file is not given in list format
        if not isinstance(self.release_files, list):
            self.message = "Release File must be a list. Please correct the data"
            raise ValidationException(self.message)


    def _validate_release_details(self) -> None:
        """Validate release Batch size and code page of files"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Release")

        self.release_file_code_page = self.release_config.get("Release_File_Code_Page", None)

        # release files code page
        if self.release_file_code_page is None:
            self.message = "Corrupted Request Set up for release file code page. Please correct the configurations"
            raise ValidationException(self.message)


    def _validate_release_csv_details(self) -> None:
        """Validate CSV Settings for release side"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Release")

        self.release_file_delimiter = self.release_config.get("Release_File_Delimiter", None)

        # release delimiter validation
        if self.release_file_delimiter is None:
            self.message = "Corrupted Request Set up for release delimiter. Please correct the configurations"
            raise ValidationException(self.message)


    def _validate_compare_details(self) -> None:
        """Validate compare configurations"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Compare")

        self.compare_file_keys = self.compare_config.get("File_Keys", None)

        # file keys
        if self.compare_file_keys is None:
            self.message = "Corrupted Request Set up for compare file keys. Please correct the configurations"
            raise ValidationException(self.message)

        if len(self.compare_file_keys) == 0:
            self.message = "Compare Keys cannot be empty. Please correct the data"
            raise ValidationException(self.message)
        

    def _validate_compare_parent_child_sep(self) -> None:
        """Validate compare configurations"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Separator")

        self.compare_parent_child_sep = self.compare_config.get("Parent_Child_Separator", None)

        # file keys
        if self.compare_parent_child_sep is None:
            self.message = "Corrupted Request Set up for parent child separator. Please correct the configurations"
            raise ValidationException(self.message)
               
        if self.compare_parent_child_sep == "": 
            self.compare_parent_child_sep = "."


    def _validate_compare_skip_details(self):
        """Validate compare skip configurations"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Compare")

        self.compare_skip_fields = self.compare_config.get("Skip_Fields", None)

        # skip fields
        if self.compare_skip_fields is None:
            self.message = "Corrupted Request Set up for compare skip fields. Please correct the configurations"
            raise ValidationException(self.message)


    def _validate_compare_case_details(self) -> None:
        """Validate compare case sensitive configurations"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Compare")

        self.compare_case_sensitive = self.compare_config.get("Case_Sensitive_Compare", None)

        # Need matching records flag
        if self.compare_case_sensitive is None:
            self.message = "Corrupted Request Set up for case sensitive compare. Please correct the configurations"
            raise ValidationException(self.message)


    def _validate_compare_match_details(self) -> None:
        """Validate compare match configurations"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Compare")

        self.compare_match_flag = self.compare_config.get("Include_Matching_Records", None)

        # Need matching records flag
        if self.compare_match_flag is None:
            self.message = "Corrupted Request Set up for including matching fields. Please correct the configurations"
            raise ValidationException(self.message)


    def _validate_compare_process_limit(self) -> None:
        """Validate Process Size"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Compare")

        self.compare_processor_limit = self.compare_config.get("Processor_Limit", None)

        # check if Process limit is non-zero integer value
        if not isinstance(self.compare_processor_limit, int):
            self.message = "Corrupted Request Set up for processor limit. Please correct the configurations"
            raise ValidationException(self.message)

        if self.compare_processor_limit <= 0:
            self.message = "Process Limit cannot be negative or zero. Please correct the data"
            raise ValidationException(self.message)


    def _validate_compare_batch_limit(self) -> None:
        """Validate Batch Size"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Compare")

        self.compare_batch_size = self.compare_config.get("Batch_Size", None)

        # check if Batch limit is non-zero integer value
        if not isinstance(self.compare_batch_size, int):
            self.message = "Corrupted Request Set up for batch limit. Please correct the configurations"
            raise ValidationException(self.message)

        if self.compare_batch_size <= 0:
            self.message = "Batch Size cannot be negative or zero. Please correct the data"
            raise ValidationException(self.message)


    def _validate_output_details(self) -> None:
        """Validate Output Details"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Output")

        self.output_location_dir = self.output_config.get("Output_Location", None)
        self.output_file_code_page = self.output_config.get("Output_File_Code_Page", None)

        # output config fields
        if self.output_location_dir is None:
            self.message = "Corrupted Request Set up for output location. Please correct the configurations"
            raise ValidationException(self.message)

        # output config fields
        if self.output_file_code_page is None:
            self.message = "Corrupted Request Set up for output code page. Please correct the configurations"
            raise ValidationException(self.message)

        # check if output loation is present
        if self.output_location_dir != "":
            validInd, self.message = friday_reusable.validate_folder_location(self.output_config.get("Output_Location"))
            if not validInd:
                raise ValidationException(self.message)
            

    def _validate_output_csv_file_details(self) -> None:
        """Validate Output file delimiter"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Output")

        self.output_file_delimiter = self.output_config.get("Output_File_Delimiter", None)

        # output config fields
        if self.output_file_delimiter is None:
            self.message = "Corrupted Request Set up for output file delimiter. Please correct the configurations"
            raise ValidationException(self.message)

        # default output delimiter if not populated
        if self.output_file_delimiter == "":
            self.output_file_delimiter = ","


    def _validate_output_store_base_release_details(self) -> None:
        """Validate Output file delimiter"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Store")

        self.output_store_base_release = self.output_config.get("Output_Store_Base_Release", None)

        # output config fields
        if self.output_store_base_release is None:
            self.message = "Corrupted Request Set up for store base & release. Please correct the configurations"
            raise ValidationException(self.message)


    def _validate_output_generate_report(self) -> None:
        """Validate Output generate html configurations"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Output")

        self.output_generate_summary_flag = self.output_config.get("Output_Generate_Summary", None)

        # validate output summary flag
        if self.output_generate_summary_flag is None:
            self.message = "Corrupted Request Set up for keeping intermediate files. Please correct the configurations"
            raise ValidationException(self.message)
        

    # def _validate_master_db_details(self) -> None:
    #     """Validate compare configurations"""

    #     mylogger.info(friday_reusable.get_function_name())
    #     self.__progress_label.config(text="Validating Master DB")

    #    # no need to throw error if master_db is not present
    #     self.master_db_file = self.gui_config["workspace_setting"]["master_db"]

 

    def validate_input_parameters(self) -> None:
        """ validate input parameters from the configuration file """
        
        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating")

        # validate base details 
        self._validate_base_folder_and_files()
        self._validate_base_details()
        self._validate_base_csv_details()

        # validate release details 
        self._validate_release_folder_and_files()
        self._validate_release_details()
        self._validate_release_csv_details()

        # validate compare details
        self._validate_compare_details()
        self._validate_compare_parent_child_sep()
        self._validate_compare_skip_details()
        self._validate_compare_case_details()
        self._validate_compare_match_details()
        self._validate_compare_process_limit()
        self._validate_compare_batch_limit()
        
        # validate output details
        self._validate_output_details()
        self._validate_output_csv_file_details()
        self._validate_output_store_base_release_details()
        self._validate_output_generate_report()

        # get the master db details
        # self._validate_master_db_details()


    def _set_mongo_extract_flag(self) -> None:
        """ set mongo extract flag """

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Setting Up")        

        # check if the extract is from mongo 
        self.mongo_extract = False
        for key in self.compare_file_keys:
            if "_id" in key and len(self.compare_file_keys) == 1: 
                self.mongo_extract = True


    def _get_base_release_files(self) -> None:
        """ get base and release file details """

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Getting Files")        

        # get absolute file name for base side 
        self.base_file_dict = friday_reusable.get_all_files_dict(files=self.base_files, 
                                                                 file_location=self.base_location, 
                                                                 file_type="Base")
        
        self.base_absolute_file_name = list(self.base_file_dict)
        self.base_file_name = [os.path.basename(x) for x in self.base_absolute_file_name]
        
        # get absolute file name for release side 
        self.release_file_dict = friday_reusable.get_all_files_dict(files=self.release_files, 
                                                                    file_location=self.release_location, 
                                                                    file_type="Release")
        
        self.release_absolute_file_name = list(self.release_file_dict)
        self.release_file_name = [os.path.basename(x) for x in self.release_absolute_file_name]

        # get the count for base and release
        # self.base_record_count = [len(self.base_file_name), ]
        # self.release_record_count = [len(self.release_file_name), ]
        
        # divide the files into multiple chunks 
        self.base_file_sublist = friday_reusable.create_chunks_dict(dict_data=self.base_file_dict, 
                                                                    number_of_chunks=self.compare_processor_limit)
        self.release_file_sublist = friday_reusable.create_chunks_dict(dict_data=self.release_file_dict,
                                                                       number_of_chunks=self.compare_processor_limit)          
                

    def _merge_temp_files(self) -> None:
        """Merge all temporary files generated in the output location"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Merging Outputs")

        tasks = []

        # start merging datasets
        merge_mismatch_process = Thread(target=CompareFiles._consolidate_unmatch_files, args=(self.output_generate_summary_flag,
                                                                                       self.output_file_code_page,
                                                                                       self.output_file_delimiter,
                                                                                       self.output_retain_temp_files,
                                                                                       self.output_location,
                                                                                       self.unmatch_file_name,
                                                                                       self.output_header))
        merge_mismatch_process.start()
        tasks.append(merge_mismatch_process)

        # check if matching records needed
        if self.compare_match_flag:
            merge_match_process = Thread(target=CompareFiles._consolidate_match_files, args=(self.output_generate_summary_flag,
                                                                                      self.output_file_code_page,
                                                                                      self.output_file_delimiter,
                                                                                      self.output_retain_temp_files,
                                                                                      self.output_location,
                                                                                      self.match_file_name,
                                                                                      self.output_header))
            merge_match_process.start()
            tasks.append(merge_match_process)

        # merge out of sequence files
        merge_oos_process = Thread(target=CompareFiles._consolidate_oos_files, args=(self.output_file_code_page,
                                                                              self.output_retain_temp_files,
                                                                              self.output_location,
                                                                              self.out_of_seq_file_name))
        merge_oos_process.start()
        tasks.append(merge_oos_process)

        # merge exception files
        merge_exp_process = Thread(target=CompareFiles._consolidate_exception_files, args=(self.output_file_code_page,
                                                                                    self.output_retain_temp_files,
                                                                                    self.output_location,
                                                                                    self.exception_file_name))
        merge_exp_process.start()
        tasks.append(merge_exp_process)


        # join the threads 
        for t in tasks:
            t.join()

        
        # check if merge of match and unmatch flag is set
        if self.__merge_match_unmatch:
            output_file = open(os.path.join(self.output_location, self.merged_file_name), "w", newline="", encoding=self.output_file_code_page)
            output_csv = csv.writer(output_file, delimiter=self.output_file_delimiter)

            # open file and write them to merged file
            with open(os.path.join(self.output_location, self.unmatch_file_name), "r", newline="", encoding=self.output_file_code_page) as unmatch_file:
                unmatch_csv = csv.reader(unmatch_file, delimiter=self.output_file_delimiter)

                header = next(unmatch_csv)
                header.append("Status")
                output_csv.writerow(header)

                # iterate over row and add the status 
                for row in unmatch_csv:
                    row.append("Failed")
                    output_csv.writerow(row)

            # check if match file flag is set 
            if self.compare_match_flag:
                with open(os.path.join(self.output_location, self.match_file_name), "r", newline="", encoding=self.output_file_code_page) as match_file:
                    match_csv = csv.reader(match_file, delimiter=self.output_file_delimiter)
                    header = next(match_csv)

                    # iterate over row and add the status 
                    for row in match_csv:
                        row.append("Pass")
                        output_csv.writerow(row)

            output_file.close()


    # def _consolidate_master_db(self):
    #     mylogger.info(friday_reusable.get_function_name())

    #     self._create_master_database() 
    #     self._update_mismatch_database()

    #     if self.compare_match_flag:
    #         self._update_match_database()


    
    # def _create_master_database(self):
    #     mylogger.info(friday_reusable.get_function_name())

    #     # create database object for connection 
    #     database_object = CreateCompareDatabase(self.master_db_file) 

    #     # check if file exists 
    #     if not os.path.exists(self.master_db_file):
    #         create_table_ddl = f""" CREATE TABLE MASTER_TABLE (
    #                                         RUN_ID                  VARCHAR(30),
    #                                         KEY_ID                  TEXT, 
    #                                         FIELD_NAME              TEXT,
    #                                         BASE_VALUE              TEXT,
    #                                         RELEASE_VALUE           TEXT,
    #                                         STATUS                  TEXT,
    #                                         CYCLE_NAME              TEXT,
    #                                         REMARKS                 TEXT
    #                                     ); """
    #         create_key_index_ddl = f""" CREATE INDEX KEY_ID_INDEX ON MASTER_TABLE (KEY_ID); """
    #         create_runid_index_ddl = f""" CREATE INDEX RUN_ID_INDEX ON MASTER_TABLE (RUN_ID); """

    #         # Execute the DDLs 
    #         database_object.run_ddl(create_table_ddl)
    #         database_object.run_ddl(create_key_index_ddl)
    #         database_object.run_ddl(create_runid_index_ddl)

    #     # disconnect from the database 
    #     database_object.disconnect()


    # def _update_mismatch_database(self):
    #     mylogger.info(friday_reusable.get_function_name())

    #     # create database object for connection 
    #     database_object = CreateCompareDatabase(os.path.join(self.output_location, "mismatch_data.db"))

    #     # attach mistmatch table and insert the data to master database 
    #     get_unmatch_rows = f""" INSERT INTO MASTER_DB.MASTER_TABLE (RUN_ID, KEY_ID, FIELD_NAME, BASE_VALUE, RELEASE_VALUE, STATUS, CYCLE_NAME, REMARKS)
    #     SELECT {os.path.basename(self.__mypath)} AS RUN_ID, KEY_ID, FIELD_NAME, BASE_VALUE, RELEASE_VALUE, 'FAILED', '{self.__environment_name}' AS CYCLE_NAME, REMARKS FROM MAIN.MISMATCH_TABLE;"""

    #     # Run the Attached SQL 
    #     unmatch_records = database_object.run_attach_insert_sql((self.master_db_file, ), 
    #                                                             get_unmatch_rows, "MASTER_DB")
    #     # close the database 
    #     database_object.disconnect()


    # def _update_match_database(self):
    #     mylogger.info(friday_reusable.get_function_name())

    #     # create database object for connection 
    #     database_object = CreateCompareDatabase(os.path.join(self.output_location, "match_data.db"))

    #     # attach match table and insert the data to master database 
    #     get_match_rows = f""" INSERT INTO MASTER_DB.MASTER_TABLE (RUN_ID, KEY_ID, FIELD_NAME, BASE_VALUE, RELEASE_VALUE, STATUS, CYCLE_NAME, REMARKS)
    #     SELECT {os.path.basename(self.__mypath)} AS RUN_ID, KEY_ID, FIELD_NAME, BASE_VALUE, RELEASE_VALUE, 'PASSED', '{self.__environment_name}' AS CYCLE_NAME, REMARKS FROM MAIN.MATCH_TABLE;"""

    #     # Run the Attached SQL 
    #     match_records = database_object.run_attach_insert_sql((self.master_db_file, ), 
    #                                                           get_match_rows, "MASTER_DB")
    #     # close the connection
    #     database_object.disconnect()

        
    def _generate_data_compare_summary_report(self):
        """Generate HTML Compare report"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Generating Report")

        self.end_time = datetime.now()

        # populate compare configuration
        table_description_0, table_description_0_columns,table_description_0_contents = self.__get_data_compare_details()

        # populate base configuration
        table_description_1, table_description_1_columns, table_description_1_contents = self.__get_data_base_config_details()

        # populate release configurations
        table_description_2, table_description_2_columns, table_description_2_contents = self.__get_data_release_config_details()

        # populate compare configurations
        table_description_3, table_description_3_columns, table_description_3_contents = self.__get_data_compare_config_details()

        # create mismatch daabase object and extract data from the mismatch table
        table_description_4, table_description_4_columns, table_description_4_contents = self.__get_mismatch_summary_count()
        table_description_5, table_description_5_columns, table_description_5_contents = self.__get_mismatch_data_count()

        image_data = self.gui_config.get("image_settings")

        # render static html file
        html = self.template.render(page_title_text="Compare Report for " + str(self.sub_option),
                                    img_logo="data:image/png;base64," + image_data.get("deloitte_logo"),
                                    travis_logo="data:image/png;base64," + image_data.get("travis_logo"),
                                    title_text_1="Summary Report",
                                    date_time=str(datetime.now()),
                                    
                                    # first section table
                                    section_header_0=table_description_0,
                                    column_name_0=table_description_0_columns,
                                    summary_data_0=table_description_0_contents,
            
                                    # second section table
                                    section_header_1=table_description_1,
                                    column_name_1=table_description_1_columns,
                                    summary_data_1=table_description_1_contents,

                                    # third section table
                                    section_header_2=table_description_2,
                                    column_name_2=table_description_2_columns,
                                    summary_data_2=table_description_2_contents,

                                    # four section table
                                    section_header_3=table_description_3,
                                    column_name_3=table_description_3_columns,
                                    summary_data_3=table_description_3_contents,

                                    # fifth section table
                                    section_header_4=table_description_4,
                                    column_name_4=table_description_4_columns,
                                    summary_data_4=table_description_4_contents,

                                    # sixth section table
                                    section_header_5=table_description_5,
                                    column_name_5=table_description_5_columns,
                                    summary_data_5=table_description_5_contents)

        output_name = self.sub_option + "_summary.html"
        with open(os.path.join(self.output_location, output_name), "w") as html_report:
            html_report.write(html)

        if self.__open_browser:
            webbrowser.open(url=os.path.join(self.output_location, output_name), new=2)


    def __get_data_compare_details(self) -> 'tuple[str, tuple, list]':
        """get compare configurations"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Generating Compare")

        table_description = "Compare Configurations"
        table_description_columns = ("Item", "Value")
        table_description_contents = [
            ("Option Selected", self.root_option),
            ("Sub-Option Selected", self.sub_option),
            ("Application Name", self.application_name),
            ("Environment Name", self.environment_name),
            ("Keys Used for Compare", str(",".join(self.compare_file_keys))),
            ("Keys Skipped from Compare", str(",".join(self.compare_skip_fields))),
            ("Case Sensitive Compare", self.compare_case_sensitive),
            ("Get Matchin Records", self.compare_match_flag),
            ("Compare Start Time", self.start_time),
            ("Compare End Time", self.end_time),
            ("Output Location", self.output_location),
        ]

        return table_description, table_description_columns, table_description_contents


    def __get_data_base_config_details(self) -> 'tuple[str, tuple, list]':
        """get base configurations"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Generating Base")

        # get the file names
        base_file_name = []
        if len(self.base_file_name) <= 10:
            base_file_name = self.base_file_name
        else:
            base_file_name = self.base_files

        # populate return variables
        table_description = "Base File(s) Summary Details"
        table_description_columns = ("Remarks", "Result")
        table_description_contents = [
            ("Base Location", str(self.base_location)),
            ("Base Files Picked for Comparison", str(",".join(base_file_name))),
            ("Base Total Number of Records", str(sum(i if i is not None else 0 for i in self.base_record_count))),
            ("Base Total Number of Exception Objects", str(sum(i if i is not None else 0 for i in self.base_exception_count))),
        ]

        return table_description, table_description_columns, table_description_contents


    def __get_data_release_config_details(self) -> 'tuple[str, tuple, list]':
        """get release configurations"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Generating Release")

        # get the file names
        release_file_name = []
        if len(self.release_file_name) <= 10:
            release_file_name = self.release_file_name
        else:
            release_file_name = self.release_files

        # populate return variables
        table_description = "Release File(s) Summary Details"
        table_description_columns = ("Remarks", "Result")
        table_description_contents = [
            ("Release Location", str(self.release_location)),
            ("Release Files Picked for Comparison", str(",".join(release_file_name))),
            ("Release Total Number of Records", str(sum(i if i is not None else 0 for i in self.release_record_count))),
            ("Release Total Number of Exception Objects", str(sum(i if i is not None else 0 for i in self.release_exception_count))),
        ]

        return table_description, table_description_columns, table_description_contents


    def __get_data_compare_config_details(self) -> 'tuple[str, tuple, list]':
        """get compare configurations"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Generating Compare")

        # populate return variables
        table_description = "Compare Summary Details"
        table_description_columns = ("Remarks", "Result")
        table_description_contents = [
            ("Files Dropped from Comparison", str(",".join(self.files_ignored))),
            ("Total Records Qualified for Comparison", str(sum(i if i is not None else 0 for i in self.compare_records_len))),
            ("Total Records Out of Sequence", str(sum(i if i is not None else 0 for i in self.oos_obj_len))),
        ]

        return table_description, table_description_columns, table_description_contents


    def __get_mismatch_summary_count(self) -> 'tuple[str, tuple, list]':
        """get mismatch summary count"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Generating Counts")

        # populate return variables
        table_description = "Compare Summary Details"
        table_description_columns = ("Top 50 Fields with High Variance", "Result")
        table_description_contents = self.__get_mismatch_summary_from_sqlite()

        return table_description, table_description_columns, table_description_contents
    

    def __get_mismatch_data_count(self) -> 'tuple[str, tuple, list]':
        """get mismatch data summary"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Generating Summary")

        # populate return variables
        table_description = "Sample Field Mismatch Data (Top 50)"
        table_description_columns = (
            "Concatenated Key",
            "Field Name",
            "Base Value",
            "Release Value",
            "Remarks",
        )
        table_description_contents = self.__get_mismatch_data_from_sqlite()

        return table_description, table_description_columns, table_description_contents


    def __get_mismatch_summary_from_sqlite(self) -> 'list[tuple]':
        """get all data needed for creating html mismatch report"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Generating Counts")

        database_object = CreateCompareDatabase(os.path.join(self.output_location, "mismatch_data.db"))

        # get mismatch data for HTML report
        get_field_count_sql = """ SELECT CASE WHEN (FIELD_NAME IS NULL OR FIELD_NAME = "") 
                                                          AND (REMARKS IS NOT NULL OR REMARKS <> "")
                                                          THEN REMARKS
                                                     ELSE FIELD_NAME END AS FIELD_NAME, 
                                                     COUNT (*) AS COUNTS 
                                    FROM MISMATCH_TABLE 
                                    GROUP BY FIELD_NAME 
                                    ORDER BY COUNTS DESC 
                                    LIMIT 50 ; """

        count_table = database_object.get_rows(get_field_count_sql)
        database_object.disconnect()

        return count_table


    def __get_mismatch_data_from_sqlite(self) -> 'list[tuple]':

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Generating Summary")

        database_object = CreateCompareDatabase(os.path.join(self.output_location, "mismatch_data.db"))

        # mismatch data SQL
        get_mismatch_summary_fields = """ SELECT KEY_ID, 
                                                 FIELD_NAME, 
                                                 BASE_VALUE, 
                                                 RELEASE_VALUE, 
                                                 REMARKS 
                                            FROM MISMATCH_TABLE LIMIT 50 ; """

        mismatch_table = database_object.get_rows(get_mismatch_summary_fields)
        database_object.disconnect()

        return mismatch_table


    @staticmethod
    def _consolidate_unmatch_files(generate_summary, output_code_page, output_delimiter, retain_temp, output_location, unmatch_file_name, output_header) -> None:
        """Consolidate all unmatch file to one"""

        mylogger.info(friday_reusable.get_function_name())
        # self.__progress_label.config(text="Merging Unmatch")

        table_name = "MISMATCH_TABLE"

        database_object = CreateCompareDatabase(os.path.join(output_location, "mismatch_data.db"))

        # merge all the files together to form single file and remove files created by threads
        output_file = open(os.path.join(output_location, unmatch_file_name), "w", newline="", encoding=output_code_page)
        output_csv = csv.writer(output_file, delimiter=output_delimiter)
        output_csv.writerow(output_header)

        # combine all unmatch files
        unmatch_output_list = fnmatch.filter(os.listdir(output_location), "Thread*unmatch.csv")

        # iterate over each file and merge it to single csv
        for file in unmatch_output_list:
            row_list = []
            output_smaller_file = open(os.path.join(output_location, file), "r", newline="", encoding=output_code_page)
            output_smaller_csv = csv.reader(output_smaller_file, delimiter=output_delimiter)

            for row in output_smaller_csv:
                output_csv.writerow(row)
                # row_list.append((row[0], row[1], row[2][0:4999], row[3][0:4999], row[4])) if generate_summary else None
                row_list.append((row[0], row[1], row[2], row[3], row[4])) if generate_summary else None

            # close and remove smaller file
            output_smaller_file.close()
            os.remove(os.path.join(output_location, file)) if not retain_temp else None

            database_object.insert_data(table_name, row_list)

        # close merged file
        output_file.close()

        # disconnect from the database 
        database_object.disconnect()

    @staticmethod
    def _consolidate_match_files(generate_summary, output_code_page, output_delimiter, retain_temp, output_location, match_file_name, output_header) -> None:
        """Consolidate all matching fields file"""

        mylogger.info(friday_reusable.get_function_name())
        # self.__progress_label.config(text="Merging Match")

        # get_context().process = match_file_name
        table_name = "MATCH_TABLE"

        database_object = CreateCompareDatabase(os.path.join(output_location, "match_data.db"))

        # merge all files together created by threeads
        output_file = open(os.path.join(output_location, match_file_name), "w", newline="", encoding=output_code_page)
        output_csv = csv.writer(output_file, delimiter=output_delimiter)
        output_csv.writerow(output_header)

        match_file_list = fnmatch.filter(os.listdir(output_location), "Thread*_match.csv")

        for file in match_file_list:
            row_list = []
            output_smaller_file = open(os.path.join(output_location, file), "r", newline="", encoding=output_code_page)
            output_smaller_csv = csv.reader(output_smaller_file, delimiter=output_delimiter)

            for row in output_smaller_csv:
                output_csv.writerow(row)
                # row_list.append((row[0], row[1], row[2][0:4999], row[3][0:4999], row[4])) if generate_summary else None
                row_list.append((row[0], row[1], row[2], row[3], row[4])) if generate_summary else None

            # close and remove smaller files
            output_smaller_file.close()
            os.remove(os.path.join(output_location, file)) if not retain_temp else None

            database_object.insert_data(table_name, row_list)

        output_file.close()
        database_object.disconnect()

    @staticmethod
    def _consolidate_oos_files(output_code_page, retain_temp, output_location, oos_file_name) -> None:
        """Consolidate all oos file"""

        mylogger.info(friday_reusable.get_function_name())
        # self.__progress_label.config(text="Merging OOS")

        output_file = open(os.path.join(output_location, oos_file_name), "w", encoding=output_code_page)

        # iterate over all files to create single merged file
        for file in fnmatch.filter(os.listdir(output_location), "Thread*oos.csv"):
            output_smaller_file = open(os.path.join(output_location, file), "r", encoding=output_code_page)
            output_file.write(output_smaller_file.read())
            output_smaller_file.close()
            os.remove(os.path.join(output_location, file)) if not retain_temp else None

        output_file.close()

    @staticmethod
    def _consolidate_exception_files(output_code_page, retain_temp, output_location, excp_file_name):

        mylogger.info(friday_reusable.get_function_name())
        # self.__progress_label.config(text="Merging Exception")

        output_file = open(os.path.join(output_location, excp_file_name), "w", encoding=output_code_page)

        # iterate over all files to create single merged file
        for file in fnmatch.filter(os.listdir(output_location), "Thread*exception.log"):
            output_smaller_file = open(os.path.join(output_location, file), "r", encoding=output_code_page)
            output_file.write(output_smaller_file.read())
            output_smaller_file.close()
            os.remove(os.path.join(output_location, file)) if not retain_temp else None

        output_file.close()

    
    def _create_metadata_database(self, output_location, file_type="Base") -> None:
        """Create database to store base file metadata"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text=f"Creating {file_type} DB")      

        database_object = CreateCompareDatabase(os.path.join(output_location, f"{file_type.lower()}_metadata.db"))
        drop_table_ddl = f""" DROP TABLE IF EXISTS {file_type.upper()}_METADATA_TABLE; """
        create_table_ddl = f""" CREATE TABLE {file_type.upper()}_METADATA_TABLE (
                                        FILE_INDEX              INTEGER,
                                        FILE_NAME               VARCHAR(20),
                                        FILE_TYPE               VARCHAR(10),
                                        FILE_SIZE               INTEGER, 
                                        FILE_CREATE_TIMESTAMP   VARCHAR(20),
                                        FILE_MODIFIED_TIMESTAMP VARCHAR(20),
                                        FILE_CHECKSUM           VARCHAR(5000)
                                    ); """
        create_index_ddl = f""" CREATE INDEX FILE_NAME_INDEX ON {file_type.upper()}_METADATA_TABLE (FILE_NAME); """
        
        # run the ddls
        database_object.run_ddl(drop_table_ddl)
        database_object.run_ddl(create_table_ddl)
        database_object.run_ddl(create_index_ddl)

        # Close the connection
        database_object.disconnect()


    def _compare_base_release_metadb(self, output_location, db_key = "FILE_INDEX") -> None:
        """compare base and release metadata rows by filename"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text=f"Comparing Data")            

        database_object = CreateCompareDatabase(os.path.join(output_location, "base_metadata.db"))
        base_release_compare_by_filename = f""" SELECT T1.FILE_NAME, 
                                                       T1.FILE_TYPE,
                                                       T1.FILE_SIZE,
                                                       T1.FILE_CREATE_TIMESTAMP,
                                                       T1.FILE_MODIFIED_TIMESTAMP,
                                                       T1.FILE_CHECKSUM,
                                                       T2.FILE_NAME,
                                                       T2.FILE_TYPE,
                                                       T2.FILE_SIZE,
                                                       T2.FILE_CREATE_TIMESTAMP,
                                                       T2.FILE_MODIFIED_TIMESTAMP,
                                                       T2.FILE_CHECKSUM
                                                FROM MAIN.BASE_METADATA_TABLE AS T1 
                                                INNER JOIN RELEASE_DB.RELEASE_METADATA_TABLE AS T2
                                                ON T1.{db_key} = T2.{db_key}; """

        base_release_rows = database_object.run_attach_sql((os.path.join(output_location, "release_metadata.db"),), 
                                                           base_release_compare_by_filename)

        # close the connection
        database_object.disconnect()

        return base_release_rows
    

    def _compare_base_not_in_release_metadb(self, output_location, db_key="FILE_INDEX") -> None:
        """get rows preset in base but not in release using file name as key"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text=f"Collecting Base")     

        if db_key.upper() == "FILE_NAME":
            base_not_in_release_sql = f""" SELECT A.{db_key.upper()}, "", "", "", "Base File Not Found in Release"
                                            FROM (
                                                    SELECT T1.{db_key.upper()} FROM MAIN.BASE_METADATA_TABLE AS T1 
                                                                EXCEPT
                                                    SELECT T2.{db_key.upper()} FROM RELEASE_DB.RELEASE_METADATA_TABLE AS T2
                                                ) AS A ; """
        else: 
            base_not_in_release_sql = f""" SELECT A.FILE_NAME, "", "", "", "Base File Not Found in Release"
                                            FROM MAIN.BASE_METADATA_TABLE AS A
                                            INNER JOIN 
                                            (
                                                    SELECT T1.{db_key.upper()} FROM MAIN.BASE_METADATA_TABLE AS T1 
                                                                EXCEPT
                                                    SELECT T2.{db_key.upper()} FROM RELEASE_DB.RELEASE_METADATA_TABLE AS T2
                                            ) AS B 
                                            ON A.{db_key.upper()} = B.{db_key.upper()}; """
            
        database_object = CreateCompareDatabase(os.path.join(output_location, "base_metadata.db"))
        base_not_in_release_rows = database_object.run_attach_sql((os.path.join(output_location, "release_metadata.db"),), 
                                                                  base_not_in_release_sql)

        # close the connection
        database_object.disconnect()

        return base_not_in_release_rows
    
    
    def _compare_release_not_in_base_metadb(self, output_location, db_key="FILE_INDEX") -> None:
        """get rows preset in base but not in release"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text=f"Collecting Release")     

        if db_key.upper() == "FILE_NAME":   
            release_not_in_base_sql = f""" SELECT A.{db_key.upper()}, "", "", "", "Release File not found in Base"
                                            FROM (
                                                    SELECT T2.{db_key.upper()} FROM RELEASE_DB.RELEASE_METADATA_TABLE AS T2
                                                                EXCEPT
                                                    SELECT T1.{db_key.upper()} FROM MAIN.BASE_METADATA_TABLE AS T1                                                        
                                                ) AS A ; """
        else:
            release_not_in_base_sql = f""" SELECT A.FILE_NAME, "", "", "", "Release File Not Found in Base"
                                            FROM RELEASE_DB.RELEASE_METADATA_TABLE AS A
                                            INNER JOIN 
                                            (
                                                    SELECT T2.{db_key.upper()} FROM RELEASE_DB.RELEASE_METADATA_TABLE AS T2
                                                                EXCEPT
                                                    SELECT T1.{db_key.upper()} FROM MAIN.BASE_METADATA_TABLE AS T1    
                                            ) AS B 
                                            ON A.{db_key.upper()} = B.{db_key.upper()};"""

        database_object = CreateCompareDatabase(os.path.join(output_location, "base_metadata.db"))
        release_not_in_base_rows = database_object.run_attach_sql((os.path.join(output_location, "release_metadata.db"),), 
                                                                  release_not_in_base_sql)

        # close the connection
        database_object.disconnect()

        return release_not_in_base_rows


    def _create_data_database(self, output_location, file_type="Base") -> None:
        """Create database to store base file data"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text=f"Creating {file_type} DB")        

        database_object = CreateCompareDatabase(os.path.join(output_location, f"{file_type.lower()}_data.db"))
        drop_table_ddl = f""" DROP TABLE IF EXISTS {file_type.upper()}_TABLE; """
        create_table_ddl = f""" CREATE TABLE {file_type.upper()}_TABLE (
                                        KEY_ID                                 VARCHAR(100), 
                                        {file_type.upper()}_JSON               JSON, 
                                        {file_type.upper()}_FILE_NAME          VARCHAR(100), 
                                        {file_type.upper()}_JSON_POSITION      INTEGER
                                    ); """
        create_index_ddl = f""" CREATE INDEX KEY_ID_INDEX ON {file_type.upper()}_TABLE (KEY_ID); """
        
        # run the DDLs
        database_object.run_ddl(drop_table_ddl)
        database_object.run_ddl(create_table_ddl)
        database_object.run_ddl(create_index_ddl)

        # Close the connection
        database_object.disconnect()


    def _create_output_data_database(self, output_location, file_type="Mismatch") -> None:
        """Create database to store match/mismatch data fields"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text=f"Creating {file_type} DB")              

        # create connection to sqlite release data
        database_object = CreateCompareDatabase(os.path.join(output_location, f"{file_type.lower()}_data.db"))
        drop_table_ddl = f"DROP TABLE IF EXISTS {file_type.upper()}_TABLE;"
        create_table_ddl = f""" CREATE TABLE {file_type.upper()}_TABLE (
                                            KEY_ID              TEXT,
                                            FIELD_NAME          TEXT,
                                            BASE_VALUE          TEXT,
                                            RELEASE_VALUE       TEXT,
                                            REMARKS             TEXT
                                        ); """
        create_index_ddl = f""" CREATE INDEX KEY_ID_INDEX ON {file_type.upper()}_TABLE (KEY_ID); """

        # drop existing table if any
        database_object.run_ddl(drop_table_ddl)
        database_object.run_ddl(create_table_ddl)
        database_object.run_ddl(create_index_ddl)

        # Close the connection
        database_object.disconnect()


    @staticmethod
    def _get_common_base_release_count(output_location) -> None:

        get_context().process = "common_keys"

        # create attach sql
        attach_sql = """ SELECT COUNT(MAIN.BASE_TABLE.KEY_ID) 
                           FROM MAIN.BASE_TABLE
                          INNER JOIN RELEASE_DB.RELEASE_TABLE 
                             ON MAIN.BASE_TABLE.KEY_ID = RELEASE_DB.RELEASE_TABLE.KEY_ID; """

        # create sqlite connection to db
        database_object = CreateCompareDatabase(os.path.join(output_location, "base_data.db"))
        common_key_count = database_object.run_attach_sql((os.path.join(output_location, "release_data.db"),), 
                                                          attach_sql)

        # Close the connection
        database_object.disconnect()

        return common_key_count


    @staticmethod
    def _get_base_data_count(output_location) -> None:

        get_context().process = "base_keys"

        # create attach sql
        base_sql = """ SELECT COUNT(MAIN.BASE_TABLE.KEY_ID) 
                         FROM MAIN.BASE_TABLE; """

        # create sqlite connection to db
        database_object = CreateCompareDatabase(os.path.join(output_location, "base_data.db"))
        base_key_count = database_object.get_rows(base_sql)

        # Close the connection
        database_object.disconnect()

        return base_key_count


    @staticmethod
    def _get_release_data_count(output_location) -> None:

        get_context().process = "release_keys"

        # create attach sql
        release_sql = """ SELECT COUNT(MAIN.RELEASE_TABLE.KEY_ID) 
                         FROM MAIN.RELEASE_TABLE; """

        # create sqlite connection to db
        database_object = CreateCompareDatabase(os.path.join(output_location, "release_data.db"))
        release_key_count = database_object.get_rows(release_sql)

        # Close the connection
        database_object.disconnect()

        return release_key_count


    @staticmethod
    def _get_compare_data_sublist(output_location, limit, base_rid, release_rid) -> None:

        get_context().process = "compare_data_sublist"

        # create attach sql
        attach_sql = f""" SELECT T1.KEY_ID, 
                                 T1.BASE_JSON, 
                                 T2.RELEASE_JSON,
                                 T1.ROWID,
                                 T2.ROWID
                            FROM MAIN.BASE_TABLE AS T1
                           INNER JOIN RELEASE_DB.RELEASE_TABLE AS T2
                              ON T1.KEY_ID = T2.KEY_ID 
                             AND T1.ROWID > {base_rid}
                             AND T2.ROWID > {release_rid}
                           ORDER BY T1.ROWID, T2.ROWID
                           LIMIT {limit} ; """

        # create sqlite connection to db
        database_object = CreateCompareDatabase(os.path.join(output_location, "base_data.db"))
        sub_list_data = database_object.run_attach_sql((os.path.join(output_location, "release_data.db"),), 
                                                          attach_sql)

        # Close the connection
        database_object.disconnect()

        # check if received list of tuples from the database
        if isinstance(sub_list_data, list) and len(sub_list_data) > 0:
            mylogger.info("last rec " + str(sub_list_data[-1]))
            return sub_list_data, sub_list_data[-1][3], sub_list_data[-1][4]
        
        return [], 0, 0
    

    @staticmethod
    def _get_base_not_in_release(output_location):

        get_context().process = "base_not_in_release"

        database_object = CreateCompareDatabase(os.path.join(output_location, "base_data.db"))

        sql = """ SELECT A.KEY_ID, "", "", "", "Key Data Not Found in Release File" 
                    FROM (
                            SELECT T1.KEY_ID FROM MAIN.BASE_TABLE AS T1
                                        EXCEPT 
                            SELECT T2.KEY_ID FROM RELEASE_DB.RELEASE_TABLE AS T2
                        ) AS A """

        base_not_in_release_rows = database_object.run_attach_sql((os.path.join(output_location, "release_data.db"), ), 
                                                                  sql)
        
        database_object.disconnect()

        return base_not_in_release_rows
    

    @staticmethod
    def _get_release_not_in_base(output_location):

        get_context().process = "release_not_in_base"

        database_object = CreateCompareDatabase(os.path.join(output_location, "base_data.db"))

        sql = """ SELECT A.KEY_ID, "", "", "", "Key Data Not Found in Base File" 
                    FROM (
                            SELECT T1.KEY_ID FROM RELEASE_DB.RELEASE_TABLE AS T1
                                        EXCEPT
                            SELECT T2.KEY_ID FROM MAIN.BASE_TABLE AS T2
                        ) AS A """

        release_not_in_base_rows = database_object.run_attach_sql((os.path.join(output_location, "release_data.db"), ), 
                                                                  sql)
        
        database_object.disconnect()

        return release_not_in_base_rows


    def _load_file_metadata(self, file_dict, output_location, db_name="base_metadata.db", thread_name="Thread-z", table_name="BASE_METADATA_TABLE", output_code_page="utf-8", output_delimiter=",") -> None:
        """extract metadata of file and insert into the database"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text=f"Loading Metadata")              

        base_list = []

        exception_file = open(os.path.join(output_location, thread_name + "_exception.log"), "w", encoding=output_code_page)

        for file, file_index in file_dict.items():
            try:
                file_name = os.path.basename(file)
                file_ext = os.path.splitext(file_name)[1][1:]
                file_stat = os.stat(file)
                file_open = open(file, "rb")
                file_text = file_open.read()
                hash_code = hashlib.md5(file_text).hexdigest()
                base_tuple = (file_index, file_name, file_ext, file_stat.st_size, file_stat.st_ctime, file_stat.st_mtime, str(hash_code))
                base_list.append(base_tuple)
            except Exception as e:
                exception_file.write(
                    f"Exception in getting metadata for {file}\n" + str(e)
                )

        database_object = CreateCompareDatabase(os.path.join(output_location, db_name))
        database_object.insert_data(table_name, base_list)
        exception_file.close()
  

    @staticmethod
    def _read_and_store_json_data(base_file_list, base_file_codepage, base_file_delimiter, release_file_list, release_file_codepage, release_file_delimiter, output_location, output_delimiter, key_data, batch_size, parent_child_separator, mongo_extract, file_type):
        """ initiate threads for loading data to database"""

        get_context().process = base_file_list + release_file_list

        tasks = []
       
        # start base thread and load the data to base database
        base_thread = Thread(target=CompareFiles.__load_json_data, args=(base_file_list,
                                                                         base_file_codepage,
                                                                         base_file_delimiter,
                                                                         output_location,
                                                                         output_delimiter,
                                                                         key_data,
                                                                         batch_size,
                                                                         parent_child_separator,
                                                                         "base_data.db",
                                                                         "BASE_TABLE",
                                                                         mongo_extract,
                                                                         file_type))
        base_thread.start()
        tasks.append(base_thread)

        # start release thread and load the data to release database
        release_thread = Thread(target=CompareFiles.__load_json_data, args=(release_file_list,
                                                                            release_file_codepage, 
                                                                            release_file_delimiter,
                                                                            output_location,
                                                                            output_delimiter,
                                                                            key_data,
                                                                            batch_size,
                                                                            parent_child_separator,
                                                                            "release_data.db",
                                                                            "RELEASE_TABLE",
                                                                            mongo_extract,
                                                                            file_type))
        release_thread.start()
        tasks.append(release_thread)

        # join the tasks 
        for t in tasks:
            t.join()        


    @staticmethod
    def __load_json_data(file_list, file_codepage, file_delimiter, output_location, output_delimiter, key_data, batch_size, parent_child_separator, db_name, table_name, mongo_extract, file_type) -> None:
        """ Load file data to base database """
        
        mylogger = logging.getLogger(__name__)
        mylogger.info(friday_reusable.get_function_name())

        # create empty data list
        data_list = []
        
        # create database object for data load
        database_object = CreateCompareDatabase(os.path.join(output_location, db_name))

        # get files from the list and get the file handles 
        for file in file_list:
            stream_file, stream_data = friday_reusable.get_file_handles(file, 
                                                                        file_codepage=file_codepage,
                                                                        file_delimiter=file_delimiter,
                                                                        file_type=file_type)
            header = []

            # if file is csv then get the header record 
            if file_type == "CSV":
                header = next(stream_data)
                header = [i.strip() for i in header]

            index = 0

            # check if json load is performed on json document 
            if isinstance(stream_data, dict):
                stream_data = [stream_data]

            # iterate over the generator object
            for json_data in stream_data:
                flat_json = friday_reusable.get_flat_json(data=json_data, 
                                                          header=header, 
                                                          separator=parent_child_separator,
                                                          file_type=file_type)

                json_key, _ = friday_reusable.get_json_keys(json_data,
                                                            flat_json,
                                                            file,
                                                            key_data,
                                                            mongo_extract)
                
                # append the records in a list
                data_list.append((output_delimiter.join(json_key), 
                                  json.dumps(flat_json, cls=CustomJSONEncoder), 
                                  file, 
                                  index))

                # if length of data list more than the batch size - initiate process to load the database
                if len(data_list) > batch_size:
                    database_object.insert_data(table_name, data_list)
                    data_list = []

                index += 1

            # close stream file 
            stream_file.close()
        
        if data_list:
            database_object.insert_data(table_name, data_list)
        
        database_object.disconnect()            


    @staticmethod
    def _start_batch_compare(thread_name, index, thread_index, compare_list, match_flag, amount_regex, skip_fields, case_flag, output_codepage, output_delimiter, output_location) -> None:
        """ Initiate Batch compare process """

        mylogger = logging.getLogger(__name__)
        mylogger.info(friday_reusable.get_function_name())

        # create files names 
        unmatch_file_name = thread_name + "-" + str(index) + "-"  + str(thread_index) + "_unmatch.csv"
        match_file_name = thread_name + "-" + str(index) + "-"  + str(thread_index) + "_match.csv"

        # create csv writer for unmatch file 
        unmatch_file = open(os.path.join(output_location, unmatch_file_name), "w", encoding=output_codepage, newline="")
        unmatch_csv_writer = csv.writer(unmatch_file, delimiter=output_delimiter)

        # create csv writer for match file 
        match_file = None 
        if match_flag: 
            match_file = open(os.path.join(output_location, match_file_name), "w", encoding=output_codepage, newline="")
            match_csv_writer = csv.writer(match_file, delimiter=output_delimiter)

        # read the list and compare the data 
        for row in compare_list:
            row_id = row[0]
            base_row = row[1]
            release_row = row[2]

            # check the instance of the data 
            if isinstance(base_row, str):
                base_row = json.loads(base_row)

            if isinstance(release_row, str):
                release_row = json.loads(release_row)

            base_cols = list(base_row)
            release_cols = list(release_row)

            # iterate over each field and value pair 
            for base_key, base_value in base_row.items():
                if base_key in skip_fields: 
                    continue 

                if base_key not in release_row.keys():
                    unmatch_csv_writer.writerow([row_id, base_key, "", "", "Field Not Found in Release File"])
                    continue

                base_orig_value = base_value 
                release_orig_value = release_row.get(base_key, "")

                # convert the values to string format 
                base_str_value = str(base_orig_value)
                release_str_value = str(release_orig_value)

                # remove extra spaces 
                base_str_value = base_str_value.strip()
                release_str_value = release_str_value.strip()

                # perform amount validaitons on base string value 
                if base_str_value != "": 
                    base_str_search = amount_regex.match(base_str_value)

                    if base_str_search:
                        base_str_value = base_str_value.strip("0")

                # perform amount validation on release string value 
                if release_str_value != "": 
                    release_str_search = amount_regex.match(release_str_value)

                    if release_str_search:
                        release_str_value = release_str_value.strip("0")
                    
                # validate if data match 
                if case_flag: 
                    if base_str_value != release_str_value:
                        unmatch_csv_writer.writerow([row_id, base_key, base_orig_value, release_orig_value, ""])
                    else:
                        if match_flag:
                            match_csv_writer.writerow([row_id, base_key, base_orig_value, release_orig_value, ""])

                else:
                    if base_str_value.lower() != release_str_value.lower():
                        unmatch_csv_writer.writerow([row_id, base_key, base_orig_value, release_orig_value, ""])
                    else:
                        if match_flag:
                            match_csv_writer.writerow([row_id, base_key, base_orig_value, release_orig_value, ""])

            for col in release_cols:
                if col not in base_cols:
                    unmatch_csv_writer.writerow([row_id, col, "", "", 'Field Not Found in Base File'])

        # close cunmatch file 
        unmatch_file.close()

        # close the match file 
        if match_flag:
            match_file.close()


    @staticmethod
    def _write_exception_json(thread_name, index, exception_list, codepage, output_location, ftype) -> None:
        """ Write exception report """
        
        mylogger = logging.getLogger(__name__)
        mylogger.info(friday_reusable.get_function_name())

        with open(os.path.join(output_location, thread_name + str(index) + ftype + "_exception.log"), "w", encoding=codepage) as output_json:
            json.dump(exception_list, output_json)


    @staticmethod
    def _write_oos(thread_name, index, oos_list, codepage, delimiter, output_location) -> None:
        """ Create OOS file """

        mylogger = logging.getLogger(__name__)
        mylogger.info(friday_reusable.get_function_name())

        with open(os.path.join(output_location, thread_name + str(index) + "_oos.csv"), "w", encoding=codepage, newline="") as oos_file:
            oos_writer = csv.writer(oos_file, delimiter=delimiter)
            oos_writer.writerow(["BASE-FILE-NAME", "BASE-KEY", "RELEASE-FILE-NAME", "RELASE-KEY"])
            oos_writer.writerows(oos_list)

       
    @staticmethod
    def _get_unmatch_rows(output_location, ftype, output_codepage, output_delimiter) -> None:
        """ get unmatching rows """

        mylogger = logging.getLogger(__name__)
        mylogger.info(friday_reusable.get_function_name())

        file_name = f"Thread-{ftype}-unmapped_unmatch.csv"
        file_open = open(os.path.join(output_location, file_name), "w", encoding=output_codepage, newline="")
        unmatch_csv = csv.writer(file_open, delimiter=output_delimiter)
        if ftype == "base":
            rows = CompareFiles._get_base_not_in_release(output_location)
        else:
            rows = CompareFiles._get_release_not_in_base(output_location)
        unmatch_csv.writerows(rows)

        file_open.close()


    @staticmethod
    def _stream_and_compare_objects(base_file_list, release_file_list, file_keys, parent_child_separator, skip_fields, case_flag, match_flag, batch_size, t_name, output_location, stream_process_queue, base_codepage, release_codepage, output_codepage, store_base_release_flag, output_delimiter, processor_limit, amount_regex, base_delimiter, release_delimiter, mongo_extract=False, compare_type="JSON") -> None:
        """ stream both base and release csv files and call compare routine """

        get_context().process = t_name

        # create temporary count variables  
        index = 0
        thread_index = 0 
        base_document_count = 0 
        release_document_count = 0
        comparable_json_count = 0
        oos_json_count = 0
        base_exception_count = 0 
        release_exception_count = 0

        # create base and release exception list 
        base_exception = [] 
        release_exception = []
        oos_list = []
        compare_list = []
        
        # create thread and process tasks 
        tasks = []

        for base_file, release_file in zip(base_file_list, release_file_list):
            thread_name = t_name + "-" + os.path.splitext(os.path.basename(base_file))[0] + "_" + os.path.splitext(os.path.basename(release_file))[0]

            base_stream_file, base_stream = friday_reusable.get_file_handles(base_file, 
                                                                             file_codepage=base_codepage,
                                                                             file_delimiter=base_delimiter,
                                                                             file_type=compare_type)
            
            release_stream_file, release_stream = friday_reusable.get_file_handles(release_file, 
                                                                                   file_codepage=release_codepage,
                                                                                   file_delimiter=release_delimiter,
                                                                                   file_type=compare_type)
            base_header = []
            release_header = []

            # get rid of header record if csv 
            if compare_type == "CSV":
                base_header = next(base_stream)
                release_header = next(release_stream)
                base_header = [i.strip() for i in base_header]
                release_header = [i.strip() for i in release_header]
            
            # check if json load is performed on the json document 
            if isinstance(base_stream, dict):
                base_stream = [base_stream]
            
            if isinstance (release_stream, dict):
                release_stream = [release_stream]

            for base_json, release_json in zip_longest(base_stream, release_stream):

                index += 1
                if base_json is None: 
                    release_document_count += 1 
                    continue

                if release_json is None:
                    base_document_count += 1 
                    continue 

                base_document_count += 1
                release_document_count += 1

                # create flat json file 
                base_flat_json = friday_reusable.get_flat_json(base_json, 
                                                               header=base_header, 
                                                               separator=parent_child_separator, 
                                                               file_type=compare_type)
                
                release_flat_json = friday_reusable.get_flat_json(release_json, 
                                                                  header=release_header,
                                                                  separator=parent_child_separator,
                                                                  file_type=compare_type)

                # get base keys and exception details 
                base_key, base_exception_list = friday_reusable.get_json_keys(base_json, 
                                                                            base_flat_json,
                                                                            base_file,
                                                                            file_keys,
                                                                            mongo_extract)
                # get release key and exception details 
                release_key, release_exception_list = friday_reusable.get_json_keys(release_json,
                                                                                  release_flat_json,
                                                                                  release_file,
                                                                                  file_keys,
                                                                                  mongo_extract)
                # add base and release exception details
                base_exception = base_exception + base_exception_list
                release_exception = release_exception + release_exception_list

                # get the count 
                base_exception_count = base_exception_count + len(base_exception_list)
                release_exception_count = release_exception_count + len(release_exception_list)                

                # if size has reached to batch size limit -> write data to exception file list 
                if len(base_exception) > batch_size:
                    t = Thread(target=CompareFiles._write_exception_json, args=(thread_name, 
                                                                                index, 
                                                                                base_exception, 
                                                                                base_codepage, 
                                                                                output_location, 
                                                                                "base"))
                    t.start() 
                    tasks.append(t)
                    base_exception = [] 
                
                # check if release exception has reached batch limit -> write data to exception file 
                if len(release_exception) > batch_size:
                    t = Thread(target=CompareFiles._write_exception_json, args=(thread_name, 
                                                                                index, 
                                                                                release_exception, 
                                                                                release_codepage, 
                                                                                output_location, 
                                                                                "release"))
                    t.start()
                    tasks.append(t)
                    release_exception = []


                # say there is no base and release key found, -> continue the iteration instead of going forward 
                if not base_key and not release_key:
                    continue

                # create base and release key strings 
                base_key_str = output_delimiter.join(base_key)
                release_key_str = output_delimiter.join(release_key)

                # check if data is out of sequence 
                if base_key_str != release_key_str:
                    oos_json_count += 1
                    oos_list.append([index, base_key_str, base_file, release_key_str, release_file])

                    # check the size of oos has exceeded
                    if len(oos_list) > batch_size:
                        t = Thread(target=CompareFiles._write_oos, args=(thread_name, 
                                                                         index, 
                                                                         oos_list, 
                                                                         output_codepage, 
                                                                         output_delimiter, 
                                                                         output_location))
                        t.start()
                        tasks.append(t)
                        oos_list = []

                    continue 

                # add the current to compare row
                compare_list.append([base_key_str, base_flat_json, release_flat_json])
                comparable_json_count += 1 

                # start a process once compare list grows to batch_size 
                if len(compare_list) > batch_size:

                    # divide the file into chunks and start mutliple threads for parallel processing 
                    compare_sublist_list = friday_reusable.create_chunks(list_data=compare_list, 
                                                                         number_of_chunks=processor_limit)
                    
                    # start parallel threads for compare 
                    for compare_sublist in compare_sublist_list:
                        t = Thread(target=CompareFiles._start_batch_compare, args=(thread_name, 
                                                                                   index, 
                                                                                   thread_index,
                                                                                   compare_sublist, 
                                                                                   match_flag,
                                                                                   amount_regex,
                                                                                   skip_fields,
                                                                                   case_flag,
                                                                                   output_codepage, 
                                                                                   output_delimiter, 
                                                                                   output_location))
                        t.start()
                        tasks.append(t)
                        thread_index += 1

                    # initialize the compare list
                    compare_list = [] 

            # close the files 
            base_stream_file.close()
            release_stream_file.close()         
                
        # check if still data present in base exception list  
        if base_exception:
            t = Thread(target=CompareFiles._write_exception_json, args=(thread_name, 
                                                                        index, 
                                                                        base_exception, 
                                                                        base_codepage, 
                                                                        output_location, 
                                                                        "base"))
            t.start()
            tasks.append(t)

        # check if still data present in base exception list  
        if release_exception:
            t = Thread(target=CompareFiles._write_exception_json, args=(thread_name, 
                                                                        index, 
                                                                        release_exception, 
                                                                        release_codepage, 
                                                                        output_location, 
                                                                        "release"))
            t.start()
            tasks.append(t)            
        
        # check if oos list present
        if oos_list:
            t = Thread(target=CompareFiles._write_oos, args=(thread_name, 
                                                             index, 
                                                             oos_list, 
                                                             output_codepage, 
                                                             output_delimiter, 
                                                             output_location))
            t.start()
            tasks.append(t)

        # check if compare list present
        if compare_list:
            t = Thread(target=CompareFiles._start_batch_compare, args=(thread_name, 
                                                                       index, 
                                                                       thread_index,
                                                                       compare_list, 
                                                                       match_flag,
                                                                       amount_regex,
                                                                       skip_fields,
                                                                       case_flag,
                                                                       output_codepage, 
                                                                       output_delimiter, 
                                                                       output_location))
            t.start()
            tasks.append(t)            

        # join the tasks 
        for t in tasks:
            t.join()

        # write the data to json compare queue 
        stream_process_queue.put([os.getpid(), 
                                  comparable_json_count,
                                  oos_json_count,
                                  base_exception_count, 
                                  release_exception_count,
                                  base_document_count,
                                  release_document_count])


class JsonStreamCompare(CompareFiles):
    """This is Quick Json Compare routine.
       Assumption is both Json has equal number of objects and in same sequence only field values may differ.
       Uses ijson package with C Parser backend. This will enable to stream json file and objects from both files can be captured for comparison.
    """

    def __init__(self, config: dict = {}, root_option: str = "", sub_option: str = "", mypath: str = "", progress_label: tkinter.Label = None, gui_config: dict = {}, application_name: str = "", environment_name: str = "", open_browser:bool = True, merge_match_unmatch:bool=False) -> None:
        super().__init__(config, root_option, sub_option, mypath, progress_label, gui_config, application_name, environment_name, open_browser, merge_match_unmatch)


    def compare_json_streams(self):
        """ stream json files and compare fields """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Initiating")

        try:
            # validate input parameters 
            self.validate_input_parameters()

            # get mongo extract flag 
            self._set_mongo_extract_flag()            

            # create match & mismatch table
            self._create_output_data_database(self.output_location, "Mismatch")
            if self.compare_match_flag:
                self._create_output_data_database(self.output_location, "Match")            

            # get base and release file list
            self._get_base_release_files()

            # Stream compare must not be used if files are unbalanced 
            if len(self.base_file_name) != len(self.release_file_name):
                self.message = "Unbalanced File Numbers. Use JSON Dynamic Compare for Comparison"
                raise ProcessingException(self.message)

            # start compare process 
            self.initiate_compare_process()

            # consolidate all temporary files 
            self._merge_temp_files()

            # update master database
            # self._consolidate_master_db()

            # create summary report
            if self.output_generate_summary_flag:
                self._generate_data_compare_summary_report()
            
        except Exception as e:
            mylogger.critical(str(e))
            self.message = "ERROR OCURRED PLEASE CHECK LOG FILE"
            raise ProcessingException(self.message)

        self.message = (
            "JSON Stream Compare is Complete. Output in: " + self.output_location
        )

        return self.message



    def validate_input_parameters(self) -> None:
        """ validate all input parameters """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Validating Inputs")       

        # validate input files and folders 
        self._validate_base_folder_and_files()
        self._validate_release_folder_and_files()

        # validate base & release code page
        self._validate_base_details()
        self._validate_release_details()

        # validate compare configurations 
        self._validate_compare_details()
        self._validate_compare_skip_details()
        self._validate_compare_case_details()
        self._validate_compare_match_details()
        self._validate_compare_process_limit()
        self._validate_compare_batch_limit()
        self._validate_compare_parent_child_sep()

        # validate output config 
        self._validate_output_details()
        self._validate_output_csv_file_details()
        self._validate_output_generate_report()
        self._validate_output_store_base_release_details()

        # validate if master db provided 
        # self._validate_master_db_details()

   
    def initiate_compare_process(self) -> None:
        """ Initiate compare process for json files """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Initiating Compare")        

        # iterate and pass the sublist for processing 
        index = 0 
        tasks = []

        # check if store base and release is database is set
        if self.output_store_base_release:
            self._create_data_database(self.output_location, "Base")
            self._create_data_database(self.output_location, "Release")
            process = Process(target=CompareFiles._read_and_store_json_data, args=(self.base_absolute_file_name, 
                                                                                   self.base_file_code_page,
                                                                                   self.base_file_delimiter,
                                                                                   self.release_absolute_file_name,
                                                                                   self.release_file_code_page,
                                                                                   self.release_file_delimiter,
                                                                                   self.output_location, 
                                                                                   self.output_file_delimiter,
                                                                                   self.compare_file_keys,
                                                                                   self.compare_batch_size,
                                                                                   self.compare_parent_child_sep,
                                                                                   self.mongo_extract,
                                                                                   "JSON"))
            process.start()
            tasks.append(process)

        # create multiprocessing queue 
        stream_queue_mgr = multiprocessing.Manager()
        stream_process_queue = stream_queue_mgr.Queue()

        # iterate on sublist and initiate process to start comparison 
        for base_sublist, release_sublist in zip(self.base_file_sublist, self.release_file_sublist):
            thread_name = 'Thread-' + str(index)

            base_list = base_sublist
            release_list = release_sublist

            if isinstance(base_sublist, dict):
                base_list = base_sublist.keys()
            
            if isinstance(release_sublist, dict):
                release_list = release_sublist.keys()

            process = Process(target=CompareFiles._stream_and_compare_objects, args=(base_list, 
                                                                                     release_list,
                                                                                     self.compare_file_keys,
                                                                                     self.compare_parent_child_sep,
                                                                                     self.compare_skip_fields,
                                                                                     self.compare_case_sensitive,
                                                                                     self.compare_match_flag,
                                                                                     self.compare_batch_size, 
                                                                                     thread_name,
                                                                                     self.output_location, 
                                                                                     stream_process_queue,
                                                                                     self.base_file_code_page,
                                                                                     self.release_file_code_page,
                                                                                     self.output_file_code_page,
                                                                                     self.output_store_base_release,
                                                                                     self.output_file_delimiter,
                                                                                     self.compare_processor_limit,
                                                                                     self.amount_regex,
                                                                                     self.base_file_delimiter,
                                                                                     self.release_file_delimiter,
                                                                                     self.mongo_extract, 
                                                                                     "JSON"))
            process.start()
            tasks.append(process)

        # join the processes 
        for p in tasks:
            p.join()

        # put a sentinel message on queue 
        stream_process_queue.put("DONE")

        # get the details from the process queue 
        while True:
            item = stream_process_queue.get()

            if item == "DONE":
                break 

            self.compare_records_len.append(item[1])
            self.oos_obj_len.append(item[2])
            self.base_exception_count.append(item[3])
            self.release_exception_count.append(item[4])
            self.base_record_count.append(item[5])
            self.release_record_count.append(item[6])


class JsonDynamicCompare(CompareFiles):
    def __init__(self, config: dict = {}, root_option: str = "", sub_option: str = "", mypath: str = "", progress_label: tkinter.Label = None, gui_config: dict = {}, application_name: str = "", environment_name: str = "", open_browser:bool = True, merge_match_unmatch:bool=False) -> None:
        super().__init__(config, root_option, sub_option, mypath, progress_label, gui_config, application_name, environment_name, open_browser, merge_match_unmatch)


    def compare_json_data(self) -> str:
        """ stream data and load to database for comparison """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Initiating")

        try:
            # validate input parameters 
            self.validate_input_parameters()

            # get mongo_extract flag
            self._set_mongo_extract_flag()

            # create match & mismatch table
            self._create_output_data_database(self.output_location, "Mismatch")
            if self.compare_match_flag:
                self._create_output_data_database(self.output_location, "Match")         
        
            # get base and release files 
            self._get_base_release_files()

            # create base and release database 
            self._create_data_database(self.output_location, "Base")
            self._create_data_database(self.output_location, "Release")

            # create a process to load base and release database 
            self.initiate_compare_process()

            # consolidate all temporary files 
            self._merge_temp_files()

            # update master database
            # self._consolidate_master_db()            

            # create summary report
            if self.output_generate_summary_flag:
                self._generate_data_compare_summary_report()            

        except Exception as e:
            mylogger.critical(str(e))
            self.message = "ERROR OCURRED PLEASE CHECK LOG FILE"
            raise ProcessingException(self.message)

        self.message = (
            "JSON Dynamic Compare is Complete. Output in: " + self.output_location
        )

        return self.message



    def validate_input_parameters(self) -> None:
        """ validate all input parameters """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Validating Inputs")       

        # validate input files and folders 
        self._validate_base_folder_and_files()
        self._validate_release_folder_and_files()

        # validate base & release code page
        self._validate_base_details()
        self._validate_release_details()

        # validate compare configurations 
        self._validate_compare_details()
        self._validate_compare_skip_details()
        self._validate_compare_case_details()
        self._validate_compare_match_details()
        self._validate_compare_process_limit()
        self._validate_compare_batch_limit()
        self._validate_compare_parent_child_sep()

        # validate output config 
        self._validate_output_details()
        self._validate_output_csv_file_details()
        self._validate_output_generate_report()

        # validate if master db provided 
        # self._validate_master_db_details()


    def initiate_compare_process(self) -> None:

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Initiating Compare")        

        # iterate and pass the sublist for processing 
        tasks = []

        # check if store base and release is database is set
        process = Process(target=CompareFiles._read_and_store_json_data, args=(self.base_absolute_file_name, 
                                                                               self.base_file_code_page,
                                                                               self.base_file_delimiter,
                                                                               self.release_absolute_file_name,
                                                                               self.release_file_code_page,
                                                                               self.release_file_delimiter,
                                                                               self.output_location, 
                                                                               self.output_file_delimiter,
                                                                               self.compare_file_keys,
                                                                               self.compare_batch_size,
                                                                               self.compare_parent_child_sep,
                                                                               self.mongo_extract,
                                                                               "JSON"))
        process.start()
        process.join()

        # once data is loaded to database get the common keys 
        common_key_count = CompareFiles._get_common_base_release_count(self.output_location)
        number_of_batches = math.ceil(common_key_count[0][0] / self.compare_batch_size)
        self.compare_records_len.append(common_key_count[0][0])

        # get base and release data count
        base_key_count = CompareFiles._get_base_data_count(self.output_location)
        self.base_record_count.append(base_key_count[0][0])

        release_key_count = CompareFiles._get_release_data_count(self.output_location)
        self.release_record_count.append(release_key_count[0][0])

        # default number of batches to 1 
        if number_of_batches < 1: 
            number_of_batches = 1

        # set offset fields 
        base_rid = 0 
        release_rid = 0 

        # iterate and create chunk for comparison 
        for i in range(number_of_batches):
            compare_list, base_rid, release_rid = CompareFiles._get_compare_data_sublist(self.output_location,
                                                                                             self.compare_batch_size,
                                                                                             base_rid,
                                                                                             release_rid)
            # compare list has some data
            if compare_list:
                # create compare sub list 
                compare_list_sublist = friday_reusable.create_chunks(list_data=compare_list, 
                                                                     number_of_chunks=self.compare_processor_limit)
                thread_index = 0
                for compare_sublist in compare_list_sublist:
                    t = Thread(target=CompareFiles._start_batch_compare, args=("Thread-", 
                                                                               i, 
                                                                               thread_index, 
                                                                               compare_sublist,
                                                                               self.compare_match_flag,
                                                                               self.amount_regex,
                                                                               self.compare_skip_fields,
                                                                               self.compare_case_sensitive,
                                                                               self.output_file_code_page,
                                                                               self.output_file_delimiter,
                                                                               self.output_location))
                    t.start()
                    tasks.append(t)
                    thread_index += 1
        
        # get the unmapped rows
        for i in ["base", "release"]:
            t = Thread(target=CompareFiles._get_unmatch_rows, args=(self.output_location, 
                                                                         i,
                                                                         self.output_file_code_page, 
                                                                         self.output_file_delimiter))
            t.start() 
            tasks.append(t)

        # join all threads 
        for t in tasks:
            t.join()


### Create Custom compare for 4 files 
class JsonCrossCompare:
    """ Perform JSON Cross Compare"""

    def __init__(self, config:dict={}, root_option:str="", sub_option:str="", mypath="", progress_label: tkinter.Label=None, gui_config: dict={}, application_name: str="", environment_name: str="", app_config:dict={}) -> None:

        # initialize variables
        self.__config = config
        self.__root_option = root_option
        self.__sub_option = sub_option
        self.__mypath = mypath
        self.__progress_label = progress_label
        self.__gui_config = gui_config
        self.__application_name = application_name
        self.__environment_name = environment_name
        self.__app_config = app_config

        # create separate configurations 
        self.input_config = self.__config.get("InputConfig")
        self.compare_config = self.__config.get("CompareConfig")
        self.output_config = self.__config.get("OutputConfig")

        # add a variable for debugging
        self.output_retain_temp_files = True
        if "Output_Retain_Temp_Files" not in self.output_config.keys():
            self.output_retain_temp_files = False 

        # create workspace directory
        self.output_location = friday_reusable.perform_data_setup(
            self.output_config.get("Output_Location", ""),
            self.__mypath,
            str(self.__sub_option),
            self.__application_name,
            self.__environment_name,
            rename_existing=False,
        )

        # Create json compare keys (set up constants)
        self.json_compare_root_option = 'JSON_COMPARE'
        self.json_compare_sub_option = None
        self.csv_compare_root_option = 'CSV_COMPARE'
        self.csv_compare_sub_option = 'CSV_Dynamic_Compare'        



    # set the getter setter property for config
    @property
    def config(self):
        """ getter and setter property """
        return self.__config

    @config.setter
    def config(self, config):
        if bool(config): 
            self.message = "Invalid Configuration Settings"
            raise ValidationException(self.message)
        
        if not bool(config):
            self.__config = config

    # set the getter setter property for root_option
    @property
    def root_option(self):
        """ getter and setter property """
        return self.__root_option

    @root_option.setter
    def root_option(self, root_option):
        if root_option == "": 
            self.message = "First option cannot be spaces"
            raise ValidationException(self.message)
        
        if not bool(root_option) and root_option != "":
            self.__root_option = root_option

    # set the getter setter property for sub_option
    @property
    def sub_option(self):
        """ getter and setter property """
        return self.__sub_option

    @sub_option.setter
    def sub_option(self, sub_option):

        if sub_option == "": 
            self.message = "Sub option cannot be spaces"
            raise ValidationException(self.message)
        
        if not bool(sub_option) and sub_option != "":
            self.__sub_option = sub_option

    # set the getter setter property for mypath
    @property
    def mypath(self):
        """ getter and setter property """
        return self.__mypath

    @mypath.setter
    def mypath(self, mypath):
        if not bool(mypath) and mypath != "":
            self.__mypath = mypath

    # set the getter setter property for progress_label
    @property
    def progress_label(self):
        """ getter and setter property """
        return self.__progress_label

    @progress_label.setter
    def progress_label(self, progress_label):
        if progress_label is None: 
            pass

        if not bool(progress_label):
            self.__progress_label = progress_label

    # set the getter setter property for gui_config
    @property
    def gui_config(self):
        """ getter and setter property """
        return self.__gui_config

    @gui_config.setter
    def gui_config(self, gui_config):
        if not bool(gui_config):
            self.__gui_config = gui_config

    # set the getter setter property for application_name
    @property
    def application_name(self):
        """ getter and setter property """
        return self.__application_name

    @application_name.setter
    def application_name(self, application_name):
        if application_name == "": 
            self.message = "Application name cannot be spaces"
            raise ValidationException(self.message)

        if not bool(application_name):
            self.__application_name = application_name

    # set the getter setter property for environment_name
    @property
    def environment_name(self):
        """ getter and setter property """
        return self.__environment_name

    @environment_name.setter
    def environment_name(self, environment_name):
        if environment_name == "": 
            self.message = "Environment name cannot be spaces"
            raise ValidationException(self.message)

        if not bool(environment_name):
            self.__environment_name = environment_name

    # set the getter setter property for environment_name
    @property
    def app_config(self):
        """ getter and setter property """
        return self.__app_config

    @app_config.setter
    def app_config(self, app_config):
        if app_config == "": 
            self.message = "App name cannot be spaces"
            raise ValidationException(self.message)

        if not bool(app_config):
            self.__app_config = app_config
    

    def perform_json_cross_compare(self):
        """ Perform Json cross compare """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Initiating")

        try:
            # get the compare type 
            compare_type = self.compare_config.get("Dynamic_Compare_Flag", True)
            self.json_compare_sub_option = "JSON_Stream_Compare"
            if compare_type:
                self.json_compare_sub_option = "JSON_Dynamic_Compare"

            # get file locations 
            first_location, first_files, first_codepage = self.populate_stage1_details()
            second_location, second_files, second_codepage = self.populate_stage2_details()
            third_location, third_files, third_codepage = self.populate_stage3_details()
            fourth_location, fourth_files, fourth_codepage = self.populate_stage4_details()

            if first_location == "":
                self.message = "First Location cannot be None. Please correct the parameters"
                raise ProcessingException(self.message)

            if second_location == "" and third_location == "" and fourth_location == "":
                self.message = "Compare needs atleast second, third or fourth location. Plese correct the parameters"
                raise ProcessingException(self.message)

            if second_location != "":
                # run compare 1 with 2
                self.compare_json_file_1_and_2(first_location, 
                                            first_files,
                                            first_codepage, 
                                            second_location, 
                                            second_files, 
                                            second_codepage, 
                                            'FirstVsSecond', 
                                            "")
                
            if third_location != "":
                # run compare 1 with 3
                self.compare_json_file_1_and_2(first_location, 
                                            first_files,
                                            first_codepage, 
                                            third_location, 
                                            third_files, 
                                            third_codepage, 
                                            'FirstVsThird', 
                                            "")
                
            if fourth_location != "":
                # run compare 1 with 4
                self.compare_json_file_1_and_2(first_location, 
                                            first_files,
                                            first_codepage, 
                                            fourth_location, 
                                            fourth_files, 
                                            fourth_codepage, 
                                            'FirstVsFourth', 
                                            "")
                
            if second_location != "" and third_location != "":
                # run compare 2 with 3
                self.compare_json_file_1_and_2(second_location, 
                                            second_files,
                                            second_codepage, 
                                            third_location, 
                                            third_files, 
                                            third_codepage, 
                                            'SecondVsThird', 
                                            "")
                
            if second_location != "" and fourth_location != "":
                # run compare 2 with 4
                self.compare_json_file_1_and_2(second_location, 
                                            second_files,
                                            second_codepage, 
                                            fourth_location, 
                                            fourth_files, 
                                            fourth_codepage, 
                                            'SecondVsFourth', 
                                            "")
                
            if third_location != "" and fourth_location != "":
                # run compare 3 with 4
                self.compare_json_file_1_and_2(third_location, 
                                            third_files, 
                                            third_codepage, 
                                            fourth_location, 
                                            fourth_files, 
                                            fourth_codepage, 
                                            'ThirdVsFourth', 
                                            "")

        except Exception as e:
            mylogger.critical(str(e))
            self.message = "ERROR OCURRED PLEASE CHECK LOG FILE"
            raise ProcessingException(self.message)

        self.message = (
            "Json Cross Compare is Complete. Output in: " + self.output_location
        )
        return self.message


    def populate_stage1_details(self):

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Getting Stage1")

        # initiate compare for Json 1 & 2 
        first_location = self.input_config.get("Stage_1_Location", None)
        first_files = self.input_config.get("Stage_1_Files", ['*', ])
        first_codepage = self.input_config.get("Stage_1_Code_Page", "utf-8")

        return first_location, first_files, first_codepage

    def populate_stage2_details(self):

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Getting Stage2")
        # initiate compare for Json 1 & 2 
        second_location = self.input_config.get("Stage_2_Location", None)
        second_files = self.input_config.get("Stage_2_Files", ['*', ])
        second_codepage = self.input_config.get("Stage_2_Code_Page", "utf-8")

        return second_location, second_files, second_codepage
    
    def populate_stage3_details(self):

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Getting Stage3")
        # initiate compare for Json 1 & 2 
        third_location = self.input_config.get("Stage_3_Location", None)
        third_files = self.input_config.get("Stage_3_Files", ['*', ])
        third_codepage = self.input_config.get("Stage_3_Code_Page", "utf-8")

        return third_location, third_files, third_codepage
    
    def populate_stage4_details(self):

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Getting Stage4")
        # initiate compare for Json 1 & 2 
        fourth_location = self.input_config.get("Stage_4_Location", None)
        fourth_files = self.input_config.get("Stage_4_Files", ['*', ])
        fourth_codepage = self.input_config.get("Stage_4_Code_Page", "utf-8")

        return fourth_location, fourth_files, fourth_codepage    


    def compare_json_file_1_and_2(self, first_location, first_files, first_codepage, second_location, second_files, second_codepage, app_name="", env_name=""):
        """ Initiate JSON Compare for first and second file """

        json_config = self.app_config[self.json_compare_root_option][self.json_compare_sub_option]

        # set up base and release for json_config 
        json_config['BaseConfig']['Base_Location'] = first_location 
        json_config['BaseConfig']['Base_Files'] = first_files 
        json_config['BaseConfig']['Base_File_Code_Page'] = first_codepage 

        json_config['ReleaseConfig']['Release_Location'] = second_location
        json_config['ReleaseConfig']['Release_Files'] = second_files
        json_config['ReleaseConfig']['Release_File_Code_Page'] = second_codepage

        # setup compare configuration 
        json_config['CompareConfig']['File_Keys'] = self.compare_config.get("File_Keys")
        json_config['CompareConfig']['Parent_Child_Separator'] = self.compare_config.get("Parent_Child_Separator")
        json_config['CompareConfig']['Skip_Fields'] = self.compare_config.get("Skip_Fields")
        json_config['CompareConfig']['Case_Sensitive_Compare'] = self.compare_config.get("Case_Sensitive_Compare")
        json_config['CompareConfig']['Include_Matching_Records'] = self.compare_config.get("Include_Matching_Records")
        json_config['CompareConfig']['Processor_Limit'] = self.compare_config.get("Processor_Limit")
        json_config['CompareConfig']['Batch_Size'] = self.compare_config.get("Batch_Size")

        # set up output config 
        json_config['OutputConfig']['Output_Location'] = self.output_location
        json_config['OutputConfig']['Output_File_Delimiter'] = self.output_config.get("Output_File_Delimiter")
        json_config['OutputConfig']['Output_File_Code_Page'] = self.output_config.get("Output_File_Code_Page")
        json_config['OutputConfig']['Output_Generate_Summary'] = self.output_config.get("Output_Generate_Summary")

        # create Json compare Object 
        if self.json_compare_sub_option == "JSON_Stream_Compare":
            json_compare = JsonStreamCompare(json_config, 
                                             root_option=self.json_compare_root_option, 
                                             sub_option=self.json_compare_sub_option, 
                                             mypath=self.mypath, 
                                             progress_label=self.progress_label, 
                                             gui_config=self.gui_config, 
                                             application_name=app_name, 
                                             environment_name=env_name,
                                             open_browser=False, 
                                             merge_match_unmatch=True)
            json_compare.compare_json_streams()
        else: 
            json_compare = JsonDynamicCompare(json_config, 
                                              root_option=self.json_compare_root_option, 
                                              sub_option=self.json_compare_sub_option, 
                                              mypath=self.mypath, 
                                              progress_label=self.progress_label, 
                                              gui_config=self.gui_config, 
                                              application_name=app_name, 
                                              environment_name=env_name,
                                              open_browser=False, 
                                              merge_match_unmatch=True)
            json_compare.compare_json_data()



class CsvStreamCompare(CompareFiles):
    def __init__(self, config: dict = {}, root_option: str = "", sub_option: str = "", mypath: str = "", progress_label: tkinter.Label = None, gui_config: dict = {}, application_name: str = "", environment_name: str = "", open_browser:bool=True, merge_match_unmatch:bool=False) -> None:
        super().__init__(config, root_option, sub_option, mypath, progress_label, gui_config, application_name, environment_name, open_browser, merge_match_unmatch)

    def compare_csv_streams(self): 
        """ Compare Base and Release CSV Files in streaming mode """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Initiating")

        try:
            # validate input parameters 
            self.validate_input_parameters()

            # create mismatch and match tables
            self._create_output_data_database(self.output_location, "Mismatch")
            if self.compare_match_flag:
                self._create_output_data_database(self.output_location, "Match")                    

            # get all base and release files 
            self._get_base_release_files()

            # Stream compare must not be used if files are unbalanced 
            if len(self.base_file_name) != len(self.release_file_name):
                self.message = "Unbalanced File Numbers. Use CSV Dynamic Compare for Comparison"
                raise ProcessingException(self.message)

            # start csv compare process
            self.initiate_compare_process()

            # consolidate all temporary files 
            self._merge_temp_files()

            # create summary report
            if self.output_generate_summary_flag:
                self._generate_data_compare_summary_report()            

        except Exception as e:
            mylogger.critical(str(e))
            self.message = "ERROR OCURRED PLEASE CHECK LOG FILE"
            raise ProcessingException(self.message)

        self.message = (
            "CSV Stream Compare is Complete. Output in: " + self.output_location
        )

        return self.message
    

    def validate_input_parameters(self):

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Validating Inputs")       

        # validate base input files and folders 
        self._validate_base_folder_and_files()
        self._validate_base_details()
        self._validate_base_csv_details()

        # validate release input files and folders
        self._validate_release_folder_and_files()
        self._validate_release_details()
        self._validate_release_csv_details()

        # validate compare configurations 
        self._validate_compare_details()
        self._validate_compare_skip_details()
        self._validate_compare_case_details()
        self._validate_compare_match_details()
        self._validate_compare_process_limit()
        self._validate_compare_batch_limit()

        # validate output configurations 
        self._validate_output_details()
        self._validate_output_csv_file_details()
        self._validate_output_store_base_release_details()
        self._validate_output_generate_report()


    def initiate_compare_process(self) -> None:
        """ Initiate compare process for json files """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Initiating Compare")

        # iterate and pass the sublist for processing 
        index = 0
        tasks = []

        # check if store base and release is database is set
        if self.output_store_base_release:
            self._create_data_database(self.output_location, "Base")
            self._create_data_database(self.output_location, "Release")
            process = Process(target=CompareFiles._read_and_store_json_data, args=(self.base_absolute_file_name, 
                                                                                   self.base_file_code_page,
                                                                                   self.base_file_delimiter,
                                                                                   self.release_absolute_file_name,
                                                                                   self.release_file_code_page,
                                                                                   self.release_file_delimiter,
                                                                                   self.output_location, 
                                                                                   self.output_file_delimiter,
                                                                                   self.compare_file_keys,
                                                                                   self.compare_batch_size,
                                                                                   self.compare_parent_child_sep,
                                                                                   self.mongo_extract,
                                                                                   "CSV"))
            process.start()
            tasks.append(process)

        # create multiprocessing queue 
        stream_queue_mgr = multiprocessing.Manager()
        stream_process_queue = stream_queue_mgr.Queue()

        # iterate on sublist and initiate process to start comparison 
        for base_sublist, release_sublist in zip(self.base_file_sublist, self.release_file_sublist):
            thread_name = 'Thread-' + str(index)
            base_list = base_sublist
            release_list = release_sublist
            
            if isinstance(base_sublist, dict):
                base_list = base_sublist.keys()
            
            if isinstance(release_sublist, dict):
                release_list = release_sublist.keys()            
            
            process = Process(target=CompareFiles._stream_and_compare_objects, args=(base_list, 
                                                                                     release_list,
                                                                                     self.compare_file_keys,
                                                                                     self.compare_parent_child_sep,
                                                                                     self.compare_skip_fields,
                                                                                     self.compare_case_sensitive,
                                                                                     self.compare_match_flag,
                                                                                     self.compare_batch_size, 
                                                                                     thread_name,
                                                                                     self.output_location, 
                                                                                     stream_process_queue,
                                                                                     self.base_file_code_page,
                                                                                     self.release_file_code_page,
                                                                                     self.output_file_code_page,
                                                                                     self.output_store_base_release,
                                                                                     self.output_file_delimiter,
                                                                                     self.compare_processor_limit,
                                                                                     self.amount_regex,
                                                                                     self.base_file_delimiter,
                                                                                     self.release_file_delimiter,
                                                                                     self.mongo_extract,
                                                                                     "CSV"))
            process.start()
            tasks.append(process)

        # join the processes 
        for p in tasks:
            p.join()

        # put a sentinel message on queue 
        stream_process_queue.put("DONE")

        # get the details from the process queue 
        while True:
            item = stream_process_queue.get()

            if item == "DONE":
                break

            self.compare_records_len.append(item[1])
            self.oos_obj_len.append(item[2])
            self.base_exception_count.append(item[3])
            self.release_exception_count.append(item[4])
            self.base_record_count.append(item[5])
            self.release_record_count.append(item[6])


class CsvDynamicCompare(CompareFiles):

    def __init__(self, config: dict = {}, root_option: str = "", sub_option: str = "", mypath: str = "", progress_label: tkinter.Label = None, gui_config: dict = {}, application_name: str = "", environment_name: str = "", open_browser:bool=True, merge_match_unmatch:bool=False) -> None:
        super().__init__(config, root_option, sub_option, mypath, progress_label, gui_config, application_name, environment_name, open_browser, merge_match_unmatch)

    def compare_csv_data(self):
        """ Compare Base and Release CSV Files in streaming mode """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Initiating")

        try:
            # validate input parameters
            self.validate_input_parameters()

            # create mismatch and match tables
            self._create_output_data_database(self.output_location, "Mismatch")
            if self.compare_match_flag:
                self._create_output_data_database(self.output_location, "Match")                    

            # get all base and release files 
            self._get_base_release_files()

            # create base and release database 
            self._create_data_database(self.output_location, "Base")
            self._create_data_database(self.output_location, "Release")            

            # start csv compare process
            self.initiate_compare_request()

            # consolidate all temporary files 
            self._merge_temp_files()

            # create summary report
            if self.output_generate_summary_flag:
                self._generate_data_compare_summary_report()

        except Exception as e:
            mylogger.critical(str(e))
            self.message = "ERROR OCURRED PLEASE CHECK LOG FILE"
            raise ProcessingException(self.message)

        self.message = (
            "CSV Dynamic Compare is Complete. Output in: " + self.output_location
        )

        return self.message
    

    def validate_input_parameters(self):
        """ Validate Input parameters """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Validating Inputs")       

        # validate base input files and folders 
        self._validate_base_folder_and_files()
        self._validate_base_details()
        self._validate_base_csv_details()

        # validate release input files and folders
        self._validate_release_folder_and_files()
        self._validate_release_details()
        self._validate_release_csv_details()

        # validate compare configurations 
        self._validate_compare_details()
        self._validate_compare_skip_details()
        self._validate_compare_case_details()
        self._validate_compare_match_details()
        self._validate_compare_process_limit()
        self._validate_compare_batch_limit()

        # get side by side report indicator 
        self.validate_side_by_side_report()

        # validate output configurations 
        self._validate_output_details()
        self._validate_output_csv_file_details()
        self._validate_output_generate_report()


    def validate_side_by_side_report(self):

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Compare")

        self.side_by_side_report = self.output_config.get("Output_Side_By_Side_Report", False)

       
    def initiate_compare_request(self):

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Initiating Compare")        

        # iterate and pass the sublist for processing 
        tasks = []        

        # check if store base and release is database is set
        process = Process(target=CompareFiles._read_and_store_json_data, args=(self.base_absolute_file_name, 
                                                                               self.base_file_code_page,
                                                                               self.base_file_delimiter,
                                                                               self.release_absolute_file_name,
                                                                               self.release_file_code_page,
                                                                               self.release_file_delimiter,
                                                                               self.output_location, 
                                                                               self.output_file_delimiter,
                                                                               self.compare_file_keys,
                                                                               self.compare_batch_size,
                                                                               self.compare_parent_child_sep,
                                                                               self.mongo_extract,
                                                                               "CSV"))
        process.start()
        process.join()        

        # once data is loaded to database get the common keys 
        common_key_count = CompareFiles._get_common_base_release_count(self.output_location)
        number_of_batches = math.ceil(common_key_count[0][0] / self.compare_batch_size)
        self.compare_records_len.append(common_key_count[0][0])

        # get base and release data count
        base_key_count = CompareFiles._get_base_data_count(self.output_location)
        self.base_record_count.append(base_key_count[0][0])

        release_key_count = CompareFiles._get_release_data_count(self.output_location)
        self.release_record_count.append(release_key_count[0][0])

        # default number of batches to 1 
        if number_of_batches < 1: 
            number_of_batches = 1

        # set offset fields 
        base_rid = 0 
        release_rid = 0 

        # iterate and create chunk for comparison 
        for i in range(number_of_batches):
            compare_list, base_rid, release_rid = CompareFiles._get_compare_data_sublist(self.output_location,
                                                                                             self.compare_batch_size,
                                                                                             base_rid,
                                                                                             release_rid)
            # compare list has some data
            if compare_list:
                # create compare sub list 
                compare_list_sublist = friday_reusable.create_chunks(list_data=compare_list, 
                                                                     number_of_chunks=self.compare_processor_limit)
                thread_index = 0
                for compare_sublist in compare_list_sublist:
                    t = Thread(target=CompareFiles._start_batch_compare, args=("Thread-", 
                                                                               i, 
                                                                               thread_index, 
                                                                               compare_sublist,
                                                                               self.compare_match_flag,
                                                                               self.amount_regex,
                                                                               self.compare_skip_fields,
                                                                               self.compare_case_sensitive,
                                                                               self.output_file_code_page,
                                                                               self.output_file_delimiter,
                                                                               self.output_location))
                    t.start()
                    tasks.append(t)
                    thread_index += 1
        
        # get the unmapped rows
        for i in ["base", "release"]:
            t = Thread(target=CompareFiles._get_unmatch_rows, args=(self.output_location, 
                                                                         i,
                                                                         self.output_file_code_page, 
                                                                         self.output_file_delimiter))
            t.start() 
            tasks.append(t)

        # join all threads 
        for t in tasks:
            t.join()



class ManipulateData(ABC):
    """ Manipulation interface """

    def __init__(self, config:dict={}, root_option:str="", sub_option:str="", mypath="", progress_label: tkinter.Label=None, gui_config: dict={}, application_name: str="", environment_name: str="") -> None:

        # initialize variables
        self.__config = config
        self.__root_option = root_option
        self.__sub_option = sub_option
        self.__mypath = mypath
        self.__progress_label = progress_label
        self.__gui_config = gui_config
        self.__application_name = application_name
        self.__environment_name = environment_name

        # get the details from yaml 
        self.input_config = self.__config.get("InputConfig", None)
        self.output_config = self.__config.get("OutputConfig", None)
        self.run_config = None 

        # create workspace directory
        self.output_location = friday_reusable.perform_data_setup(
            self.output_config.get("Output_Location", ""),
            self.__mypath,
            str(self.__sub_option),
            self.__application_name,
            self.__environment_name,
            rename_existing=False,
        )

        # input configuration details 
        self.input_location = None 
        self.input_files = None 
        self.input_file_code_page = None 
        self.input_file_delimiter = None 
        
        # output configuration details 
        self.output_file_code_page = None 
        self.output_file_delimiter = None 

        # input variables 
        self.input_file_dict = None
        self.input_absolute_file_name = None
        self.input_file_name = None

        # some variables 
        self.message = ""


    # set the getter setter property for config
    @property
    def config(self):
        """ getter and setter property """
        return self.__config

    @config.setter
    def config(self, config):
        if bool(config): 
            self.message = "Invalid Configuration Settings"
            raise ValidationException(self.message)
        
        if not bool(config):
            self.__config = config

    # set the getter setter property for root_option
    @property
    def root_option(self):
        """ getter and setter property """
        return self.__root_option

    @root_option.setter
    def root_option(self, root_option):
        if root_option == "": 
            self.message = "First option cannot be spaces"
            raise ValidationException(self.message)
        
        if not bool(root_option) and root_option != "":
            self.__root_option = root_option

    # set the getter setter property for sub_option
    @property
    def sub_option(self):
        """ getter and setter property """
        return self.__sub_option

    @sub_option.setter
    def sub_option(self, sub_option):

        if sub_option == "": 
            self.message = "Sub option cannot be spaces"
            raise ValidationException(self.message)
        
        if not bool(sub_option) and sub_option != "":
            self.__sub_option = sub_option

    # set the getter setter property for mypath
    @property
    def mypath(self):
        """ getter and setter property """
        return self.__mypath

    @mypath.setter
    def mypath(self, mypath):
        if not bool(mypath) and mypath != "":
            self.__mypath = mypath

    # set the getter setter property for progress_label
    @property
    def progress_label(self):
        """ getter and setter property """
        return self.__progress_label

    @progress_label.setter
    def progress_label(self, progress_label):
        if progress_label is None: 
            pass

        if not bool(progress_label):
            self.__progress_label = progress_label

    # set the getter setter property for gui_config
    @property
    def gui_config(self):
        """ getter and setter property """
        return self.__gui_config

    @gui_config.setter
    def gui_config(self, gui_config):
        if not bool(gui_config):
            self.__gui_config = gui_config

    # set the getter setter property for application_name
    @property
    def application_name(self):
        """ getter and setter property """
        return self.__application_name

    @application_name.setter
    def application_name(self, application_name):
        if application_name == "": 
            self.message = "Application name cannot be spaces"
            raise ValidationException(self.message)

        if not bool(application_name):
            self.__application_name = application_name

    # set the getter setter property for environment_name
    @property
    def environment_name(self):
        """ getter and setter property """
        return self.__environment_name

    @environment_name.setter
    def environment_name(self, environment_name):
        if environment_name == "": 
            self.message = "Environment name cannot be spaces"
            raise ValidationException(self.message)

        if not bool(environment_name):
            self.__environment_name = environment_name


    def _validate_input_details(self):
        """ validate input file folder and files """

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text='Validating Inputs')

        self.input_location = self.input_config.get("Input_Location", None)
        self.input_files = self.input_config.get("Input_Files", None)

        # Check other keys present for horizontal cut
        if self.input_location is None:
            self.message = 'Corrupted Request Set up for input location. Please correct the configurations'
            raise ValidationException(self.message)

        # check if base location is present
        validInd, self.message = friday_reusable.validate_folder_location(self.input_location)
        if not validInd:
            raise ValidationException(self.message)

        # Check if selective input flag key is present
        if self.input_files is None:
            self.message = 'Corrupted Request Set up for input files. Please correct the configurations'
            raise ValidationException(self.message)
        
        # check if input is empty or has some file 
        validInd, self.message  = friday_reusable.validate_file_location(self.input_location, 
                                                                         self.input_files)
        if not validInd:
            raise ValidationException(self.message)
        

    def _validate_input_code_page(self):
        """ Validate input code page """

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Inputs")

        self.input_file_code_page = self.input_config.get("Input_File_Code_Page", None)

        # check if input Location key is present
        if self.input_file_code_page is None:
            self.message = 'Corrupted Request Set up for input codepage. Please correct the configurations'
            raise ValidationException(self.message)
        

    def _validate_input_csv_delimiter(self):
        """ Validate input csv delimiters """

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Inputs")

        self.input_file_delimiter = self.input_config.get("Input_Delimiter", None)

        if self.input_file_delimiter is None:
            self.message = 'Corrupted Request Set up for input delimiter. Please correct the configurations'
            raise ValidationException(self.message)
        

    def _validate_output_details(self):
        """ Validate output details """

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Output")

        self.output_location_dir = self.output_config.get("Output_Location", None)
        self.output_file_code_page = self.output_config.get("Output_File_Code_Page", None)

        # output config fields
        if self.output_location_dir is None:
            self.message = "Corrupted Request Set up for output location. Please correct the configurations"
            raise ValidationException(self.message)

        # output config fields
        if self.output_file_code_page is None:
            self.message = "Corrupted Request Set up for output code page. Please correct the configurations"
            raise ValidationException(self.message)

        # check if output loation is present
        if self.output_location_dir != "":
            validInd, self.message = friday_reusable.validate_folder_location(self.output_config.get("Output_Location"))
            if not validInd:
                raise ValidationException(self.message)
            

    def _validate_output_csv_file_details(self) -> None:
        """Validate Output file delimiter"""

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating Output")

        self.output_file_delimiter = self.output_config.get("Output_Delimiter", None)

        # output config fields
        if self.output_file_delimiter is None:
            self.message = "Corrupted Request Set up for output file delimiter. Please correct the configurations"
            raise ValidationException(self.message)

        # default output delimiter if not populated
        if self.output_file_delimiter == "":
            self.output_file_delimiter = ","


    def _validate_run_details(self) -> None:
        """ Validate run parameters """
        pass 


    def _validate_input_parameters(self) -> None:
        """ validate all input parameters """

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Validating")

        self._validate_input_details()
        self._validate_input_code_page()
        self._validate_input_csv_delimiter()

        self._validate_output_details()
        self._validate_output_csv_file_details()


    def _get_input_file_details(self) -> None:
        """ get all input files """

        mylogger.info(friday_reusable.get_function_name())
        self.__progress_label.config(text="Getting Files")

        self.input_file_dict = friday_reusable.get_all_files_dict(files=self.input_files, 
                                                                  file_location=self.input_location,
                                                                  file_type="input")
        
        self.input_absolute_file_name = list(self.input_file_dict)
        self.input_file_name = [os.path.basename(x) for x in self.input_absolute_file_name]


class TokenizeBase64Csv(ManipulateData):
    """ Manipulate CSV files """

    def __init__(self, config:dict={}, root_option:str="", sub_option:str="", mypath:str="", progress_label:tkinter.Label=None, gui_config:dict={}, application_name:str="", environment_name:str="") -> None:
        super().__init__(config, root_option, sub_option, mypath, progress_label, gui_config, application_name, environment_name)

        # run the configurations
        self.run_config = self.config.get("TokenConfig", None)
        
        # run configurations 
        self.target_column_list = None 
        self.target_condition = None 
        self.thread_limit = None

        # other variables 
        self.condition_list = None 
        self.encryption_flag = None


    def perform_csv_tokenization(self):
        """ perform CSV tokenization """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Processing")

        try:
            # validate and get all input files 
            self._validate_input_parameters()
            self._get_input_file_details()            

            # break the list into smaller chunks 
            self.input_file_sublist = friday_reusable.create_chunks(list_data=self.input_absolute_file_name, 
                                                                    number_of_chunks=self.thread_limit)
            self.condition_list = shlex.split(self.target_condition)

            # perform data tokenization
            if self.sub_option == "CSV_Base64_Tokenization":
                self.encryption_flag = True           
            elif self.sub_option == "CSV_Base64_Detokenization":
                self.encryption_flag = False

            self.perform_base64_csv_tokenization()

            # populate message and return control back
            self.message = (
                "CSV Tokenization Completed Successfully. Output in: " + self.output_location
            )
            return self.message
        
        except Exception as e:
            mylogger.critical(str(e))
            self.message = "ERROR OCURRED PLEASE CHECK LOG FILE"
            raise ProcessingException(self.message)


    def _validate_input_parameters(self):
        """ Validate input parameters """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Validating")

        # validate input parameters 
        self._validate_input_details()
        self._validate_input_code_page()
        self._validate_input_csv_delimiter()

        # validate output parameters
        self._validate_output_details()
        self._validate_output_csv_file_details()

        # validate run parameters
        self._validate_run_details()


    def _validate_run_details(self):
        """Validate Run details """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Validating Config")

        # get the run details variable
        self.target_column_list = self.run_config.get("Target_Columns", None)
        self.target_condition = self.run_config.get("Condition", None)
        self.thread_limit = self.run_config.get("Thread_Limit", None)

        # validate target columns 
        if self.target_column_list is None:
            self.message = "Corrupted Request Set up for target column list. Please correct the configurations"
            raise ValidationException(self.message)

        # validate target condition 
        if self.target_condition is None:
            self.message = "Corrupted Request Set up for target condition. Please correct the configurations"
            raise ValidationException(self.message)
        
        # validate target condition 
        if self.thread_limit is None:
            self.message = "Corrupted Request Set up for thread limit. Please correct the configurations"
            raise ValidationException(self.message)

        if len(self.target_column_list) <= 0:
            self.target_column_list = ['*', ]

        if not isinstance(self.thread_limit, int):
            self.thread_limit = 10
        

    def perform_base64_csv_tokenization(self):
        """ Perform CSV Tokenization using base64 """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text="Tokenizing")

        tasks = [] 

        # iterate over each sublist in list 
        for index, input_sublist in enumerate(self.input_file_sublist):
            thread = Thread(target=TokenizeBase64Csv.run_tokenization_process, args=(input_sublist, 
                                                                                     self.input_file_code_page, 
                                                                                     self.input_file_delimiter, 
                                                                                     self.target_column_list,
                                                                                     self.condition_list,
                                                                                     self.encryption_flag,
                                                                                     self.output_location, 
                                                                                     self.output_file_code_page, 
                                                                                     self.output_file_delimiter))
            thread.start()
            tasks.append(thread)

        # join all threads
        for t in tasks:            t.join()

    
    @staticmethod
    def run_tokenization_process(file_list, input_codepage, input_delimiter, target_column_list, condition_list, encryption_flag, output_location, output_file_code_page, output_file_delimiter):
        """ run tokenization process """
        
        mylogger = logging.getLogger(__name__)
        mylogger.info(friday_reusable.get_function_name())

        # get file from the file list 
        for f in file_list:

            file_name, ext = os.path.splitext(os.path.basename(f))
            if encryption_flag:
                output_file_name = os.path.join(output_location, file_name + "_Tokenized" + ext)
            else:
                output_file_name = os.path.join(output_location, file_name + "_Detokenized" + ext)

            # create csv reader handle 
            input_file_open = open(f, "r", newline="", encoding=input_codepage)
            input_csv = csv.reader(input_file_open, delimiter=input_delimiter)
            input_header = next(input_csv, None)

            # create csv writer handle 
            output_file_open = open(output_file_name, "w", newline="", encoding=output_file_code_page)
            output_csv = csv.writer(output_file_open, delimiter=output_file_delimiter)
            output_csv.writerow(input_header)

            # clean the header data 
            input_header = [i.strip() for i in input_header]

            # iterate over each input row 
            for row in input_csv:
                if len(row) == 0:
                    continue 

                tokenized_row = row 
                condition_flag = TokenizeBase64Csv.validate_token_condition(row, 
                                                                            input_header, 
                                                                            condition_list)

                if condition_flag:
                    tokenized_row = TokenizeBase64Csv.perform_row_tokenization(row, 
                                                                               input_header, 
                                                                               encryption_flag, 
                                                                               target_column_list)
                output_csv.writerow(tokenized_row)

            input_file_open.close()
            output_file_open.close()


    @staticmethod
    def validate_token_condition(row, header, condition_list):
        """ validate token condition """

        mylogger = logging.getLogger(__name__)
        mylogger.info(friday_reusable.get_function_name())

        is_valid = False 
        prev_bool = '' 
        condition_index = 0

        if '*' in condition_list:
            return True 

        elif len(condition_list) < 3: 
            return False 

        # iterate over condition list
        for i, condition in enumerate(condition_list):

            if condition_index == 0 and not is_valid and len(condition_list) > (i+2):
                condition_index = condition_index + 1 

                colname = condition_list[i].strip("'")
                colname = colname.strip('"')
                colname = colname.strip()

                operator = condition_list[i+1].strip("'")
                operator = operator.strip('"')
                operator = operator.strip()

                value = condition_list[i+2].strip("'")
                value = value.strip('"')
                value = value.strip()

                # check if column name in the header
                if colname in header:
                    rowvalue = row[header.index(colname)].strip()

                is_valid = friday_reusable.perform_condition_operation(rowvalue.strip(), value, operator)

            elif condition_index > 0 and (condition.strip()).upper() in ('AND', 'OR'):
                prev_bool = condition

            # for joining conditions 
            elif condition_index > 0 and len(condition_list) > (i+2) and i % 4 == 0: 

                # populate condition value
                colname = condition_list[i].strip("'")
                colname = colname.strip('"')
                colname = colname.strip() 

                operator = condition_list[i+1].strip("'")
                operator = operator.strip('"')
                operator = operator.strip()

                value = condition_list[i+2].strip("'")
                value = value.strip('"')
                value = value.strip()

                if colname in header:
                    rowvalue = row[header.index(colname)].strip()

                if prev_bool.upper() == 'AND' :
                    is_valid = is_valid and friday_reusable.perform_condition_operation(rowvalue.strip(), value, operator)
                elif prev_bool.upper() == 'OR': 
                    is_valid = is_valid or friday_reusable.perform_condition_operation(rowvalue.strip(), value, operator)

        return is_valid
    

    @staticmethod
    def perform_row_tokenization(row, header, encryption_flag, target_column_list):
        """ perform row tokenization """

        mylogger = logging.getLogger(__name__)
        mylogger.info(friday_reusable.get_function_name())

        to_put = row

        # try to encrypt or decrypt data based on flag received as input. In case of exception return the original row
        try:
            if encryption_flag:
                if '*' in target_column_list:
                    for index, word in enumerate(row):
                        to_put[index] = (base64.b64encode((word).encode("utf-8"))).decode('utf-8')
                else:
                    for target in target_column_list:
                        to_put[header.index(target)] = (base64.b64encode((row[header.index(target)]).encode("utf-8"))).decode("utf-8")                       
            else:
                if '*' in target_column_list:
                    for index, word in enumerate(row):
                        to_put[index] = (base64.b64decode((word).encode("utf-8"))).decode('utf-8')
                else:
                    for target in target_column_list:
                        to_put[header.index(target)] = (base64.b64decode((row[header.index(target)]).encode("utf-8"))).decode("utf-8")
                        
            return to_put
        
        except:
            return row


class JsonManipulation(ManipulateData):
    def __init__(self, config: dict = {}, root_option: str = "", sub_option: str = "", mypath="", progress_label: tkinter.Label = None, gui_config: dict = {}, application_name: str = "", environment_name: str = "") -> None:
        super().__init__(config, root_option, sub_option, mypath, progress_label, gui_config, application_name, environment_name)

        self.run_config = self.config.get("RunConfig", None)
        self.thread_limit = None 
        self.batch_size = None

    def perform_json_manipulation(self):
        """ perform json file split by calling sub routines """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text='Processing Request')

        # validate input parameters and get input file details 
        self._validate_input_parameters()
        self._get_input_file_details()

        # process Split Json File option request
        if self.sub_option == "Convert_JSON_To_CSV":
            try:
                self._validate_output_csv_file_details()
                
                # get parent child separator for flattened json 
                self.parent_child_separator = self.output_config.get("Output_Child_Separator", None)
                if self.parent_child_separator == "" or self.parent_child_separator is None:
                    self.parent_child_separator = "."

                # get thread limit and batch count 
                self._validate_run_details()

                # break the list into smaller chunks 
                self.input_file_sublist = friday_reusable.create_chunks(list_data=self.input_absolute_file_name, 
                                                                        number_of_chunks=self.thread_limit)
                self.perform_json_conversion()

                # populate message and return control back
                self.message = (
                    "JSON to CSV Completed Successfully. Output in: " + self.output_location
                )
                return self.message
            
            except Exception as e:
                mylogger.critical(str(e))
                self.message = "ERROR OCURRED PLEASE CHECK LOG FILE"
                raise ProcessingException(self.message)

        # process split json file request
        elif self.sub_option == "Split_JSON_File":
            try:
                self._validate_run_details()                      
                self.input_file_sublist = friday_reusable.create_chunks(list_data=self.input_absolute_file_name,
                                                                        number_of_chunks=self.thread_limit)
                self.process_json_split()

                # populate message and return control back
                self.message = (
                    "Split JSON Completed Successfully. Output in: " + self.output_location
                )
                return self.message
            
            except Exception as e:
                mylogger.critical(str(e))
                self.message = "ERROR OCURRED PLEASE CHECK LOG FILE"
                raise ProcessingException(self.message)

        # process json stream merge request
        elif self.sub_option == "Merge_JSON_Stream_Files":
            try:
                self.process_json_stream_merge()

                # populate message and return control back
                self.message = (
                    "Merge Streaming JSON Completed Successfully. Output in: " + self.output_location
                )
                return self.message
            
            except Exception as e:
                mylogger.critical(str(e))
                self.message = "ERROR OCURRED PLEASE CHECK LOG FILE"
                raise ProcessingException(self.message)         

        # process JSON Load and merge request
        elif self.sub_option == "Merge_JSON_Load_Files":
            try:
                self.process_json_load_merge()

                # populate message and return control back
                self.message = (
                    "Merge Load JSON Completed Successfully. Output in: " + self.output_location
                )
                return self.message
            
            except Exception as e:
                mylogger.critical(str(e))
                self.message = "ERROR OCURRED PLEASE CHECK LOG FILE"
                raise ProcessingException(self.message)          


    def _validate_input_parameters(self) -> None:
        """ validate input parameters  """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text='Validating Inputs')        
        
        # validate input parameters
        self._validate_input_details()
        self._validate_input_code_page()

        # validate output parameters
        self._validate_output_details()


    def _validate_run_details(self):
        """ validation json split run details """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text='Validating Inputs')

        # thread limit and batch size
        self.thread_limit = self.run_config.get("Thread_Limit", None)
        self.batch_size = self.run_config.get("Batch_Size", None)

        # validate target condition 
        if self.thread_limit is None or not isinstance(self.thread_limit, int):
            self.thread_limit = 10

        # validate the batch size 
        if self.batch_size is None or not isinstance(self.batch_size, int):
            self.batch_size = 50000


    def perform_json_conversion(self):
        """ Perform json conversion to csv"""
        
        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text='Processing Request')

        # get each file and perform encryption in threads 
        tasks = []

        for file_sublist in self.input_file_sublist:
            thread = Thread(target=JsonManipulation.run_json_convert_process, args=(file_sublist, 
                                                                                    self.input_file_code_page, 
                                                                                    self.parent_child_separator, 
                                                                                    self.output_location, 
                                                                                    self.output_file_delimiter, 
                                                                                    self.output_file_code_page))
            thread.start()
            tasks.append(thread)

        # join the threads 
        for t in tasks:
            t.join()


    def process_json_split(self):
        """ perform json file split """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text='Splitting JSON')

        tasks = []

        # iterate on file sublist and run the split json process
        for index, file_sublist in enumerate(self.input_file_sublist):
            thread_name = "Thread-" + str(index) + "-"
            thread = Thread(target=JsonManipulation.run_split_process, args=(thread_name, 
                                                                             file_sublist, 
                                                                             self.batch_size,
                                                                             self.output_location,
                                                                             self.output_file_code_page))
            
            thread.start()
            tasks.append(thread)

        # join all threads 
        for t in tasks:
            t.join()


    @staticmethod
    def run_json_convert_process(file_sublist, input_codepage, parent_child_sep, output_location, output_delimiter, output_codepage):
        """ run json to csv process """

        mylogger = logging.getLogger(__name__)
        mylogger.info(friday_reusable.get_function_name())

        for file_name in file_sublist:
            input_name, _ = os.path.splitext(os.path.basename(file_name))

            # get file handles 
            input_file, input_stream = friday_reusable.get_file_handles(file_name=file_name, 
                                                                        file_codepage=input_codepage,
                                                                        file_type="JSON")
            
            flat_dic = [flatten_json.flatten(d, parent_child_sep) for d in input_stream]
            normal_df = pd.json_normalize(flat_dic)

            normal_df.to_csv(os.path.join(output_location, input_name + ".csv"), 
                             index=None,
                             sep=output_delimiter,
                             encoding=output_codepage)
            input_file.close() 


    @staticmethod
    def run_split_process(thread_name, file_sublist, batch_size, output_location, output_codepage):
        
        mylogger = logging.getLogger(__name__)
        mylogger.info(friday_reusable.get_function_name())

        # run the split json processing
        for file_name in file_sublist:

            input_file, input_file_stream = friday_reusable.get_file_handles(file_name=file_name, 
                                                                             file_type="JSON")
            input_file_name, input_file_ext = os.path.splitext(os.path.basename(file_name))

            json_list = [] 
            idx = 1

            for json_data in input_file_stream:
                json_list.append(json_data)

                # if reaches batch size, simply dump json data to thread files 
                if len(json_list) > batch_size:
                    output_file_name = input_file_name + "_" + str(idx) + input_file_ext
                    idx = JsonManipulation.dump_json_data(json_list, 
                                                          output_location, 
                                                          output_file_name, 
                                                          output_codepage, 
                                                          idx)
                    json_list = []
            
            # if reaches batch size, simply dump json data to thread files 
            if json_list:
                output_file_name = input_file_name + "_" + str(idx) + input_file_ext
                idx = JsonManipulation.dump_json_data(json_list, 
                                                      output_location, 
                                                      output_file_name, 
                                                      output_codepage, 
                                                      idx)
                json_list = [] 

            # close the input files
            input_file.close()


    @staticmethod
    def dump_json_data(json_list, output_location, output_file_name, output_codepage, idx):
        """ Dump json data """
        
        mylogger = logging.getLogger(__name__)
        mylogger.info(friday_reusable.get_function_name())

        with open(os.path.join(output_location, output_file_name), "w", encoding=output_codepage) as fp:
            json.dump(json_list, fp, cls=CustomJSONEncoder)
        
        idx += 1

        return idx


    def process_json_stream_merge(self):
        """ merge multiple json files to one with streaming feature """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text='Merging JSON')        

        output_file = open(os.path.join(self.output_location, 'Merged.json'), 'w', encoding=self.output_file_code_page)

        # write first record 
        output_file.write("[\n")
        first_record = True 

        # iterate over each file 
        for file_name in self.input_absolute_file_name:
            input_file, input_file_stream = friday_reusable.get_file_handles(file_name=file_name, 
                                                                             file_codepage=self.input_file_code_page,
                                                                             file_type="JSON")

            for input_json in input_file_stream:
                if first_record:
                    first_record = False 
                else: 
                    output_file.write(",\n")

                json.dump(input_json, output_file, cls=CustomJSONEncoder)
            
            input_file.close()

        # write the ouput file and close 
        output_file.write("\n]")
        output_file.close()        


    def process_json_load_merge(self):
        """ process json load merge process """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text='Merging JSON')        

        output_file = open(os.path.join(self.output_location, 'Merged.json'), 'w', encoding=self.output_file_code_page)
        output_json_list = [] 

        # iterate over each file 
        for input_file in self.input_absolute_file_name:
            with open(input_file, "r", encoding=self.input_config['Input_File_Code_Page']) as input_file_stream:
                output_json_list.extend(json.loads(input_file_stream.read()))
            
        json.dump(output_json_list, output_file)

        output_file.close()


class CsvManipulation(ManipulateData):
    """ Manipulate CSV Files """

    def __init__(self, config: dict = {}, root_option: str = "", sub_option: str = "", mypath="", progress_label: tkinter.Label = None, gui_config: dict = {}, application_name: str = "", environment_name: str = "") -> None:
        super().__init__(config, root_option, sub_option, mypath, progress_label, gui_config, application_name, environment_name)

        self.run_config = self.config.get("SplitConfig", None)
        self.thread_limit = None 
        self.batch_size = None 
        self.input_header_flag = None 
        self.output_header_flag = None
        self.output_mapped_flag = None 
        self.header_mapping = None
        self.output_columns = None
        self.output_condition = None

    def perform_csv_manipulation(self):
        """ perform csv manipulation """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text='Processing Request')

        # validate input parameters and get input file details 
        self._validate_input_parameters()
        self._get_input_file_details()

        # horizontal slicing 
        if self.sub_option == "CSV_Horizontal_Slice":
            try: 
                # validate run details 
                self._validate_hsplit_run_details()

                # break the list into smaller chunks 
                self.input_file_sublist = friday_reusable.create_chunks(list_data=self.input_absolute_file_name, 
                                                                        number_of_chunks=self.thread_limit)
                # perform horizontal split request
                self.perform_split_request()

                # populate message and return control back
                self.message = (
                    "Horizontal CSV Split Completed Successfully. Output in: " + self.output_location
                )
                return self.message 
                
            except Exception as e:
                mylogger.critical(str(e))
                self.message = "ERROR OCURRED PLEASE CHECK LOG FILE"
                raise ProcessingException(self.message)
       
        # vertical slicing 
        elif self.sub_option == "CSV_Vertical_Slice":
            try:
                # validate run details 
                self._validate_vsplit_run_details()

                # break the list into smaller chunks 
                self.input_file_sublist = friday_reusable.create_chunks(list_data=self.input_absolute_file_name, 
                                                                        number_of_chunks=self.thread_limit)

                # perform vertical split 
                self.perform_split_request()

                # populate message and return control back
                self.message = (
                    "Vertical CSV Split Completed Successfully. Output in: " + self.output_location
                )
                return self.message                

            except Exception as e:
                mylogger.critical(str(e))
                self.message = "ERROR OCURRED PLEASE CHECK LOG FILE"
                raise ProcessingException(self.message)

        # conditional slicing 
        elif self.sub_option == "CSV_Conditional_Slice":
            
            try:
                # validate run details 
                self._validate_csplit_run_details() 

                # break the list into smaller chunks 
                self.input_file_sublist = friday_reusable.create_chunks(list_data=self.input_absolute_file_name, 
                                                                        number_of_chunks=self.thread_limit)            

                # perform conditional split
                self.perform_split_request()

                # populate message and return control back
                self.message = (
                    "Conditional CSV Split Completed Successfully. Output in: " + self.output_location
                )
                return self.message              
            
            except Exception as e:
                mylogger.critical(str(e))
                self.message = "ERROR OCURRED PLEASE CHECK LOG FILE"
                raise ProcessingException(self.message)

        # merge csv request
        elif self.sub_option == "CSV_Merge_Files":
            self.perform_merge_csv_request()



    def _validate_input_parameters(self) -> None:
        """ validate input parameters """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text='Validating Inputs')        
        
        # validate input details 
        self._validate_input_details()
        self._validate_input_code_page()
        self._validate_input_csv_delimiter()

        # validate output details 
        self._validate_output_details()
        self._validate_output_csv_file_details()


    def _validate_hsplit_run_details(self):
        """ validate horizontal split request details """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text='Validating Inputs')

        # thread limit and batch size
        self.thread_limit = self.run_config.get("Thread_Limit", None)
        self.batch_size = self.run_config.get("Batch_Size", None)
        self.input_header_flag = self.run_config.get("Input_Has_Header", False)
        self.output_header_flag = self.run_config.get("Output_Has_Header", False)

        # validate target condition 
        if self.thread_limit is None or not isinstance(self.thread_limit, int):
            self.thread_limit = 10

        # validate the batch size 
        if self.batch_size is None or not isinstance(self.batch_size, int):
            self.batch_size = 50000

        # validate the input header flag 
        if self.output_header_flag:
            if not self.input_header_flag:
                self.message = 'You cannot have output header if input header is not present. Please correct the configurations'
                raise ValidationException(self.message)


    def _validate_vsplit_run_details(self):
        """ validate vertical split run details """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text='Validating Inputs')

        # thread limit and batch size
        self.thread_limit = self.run_config.get("Thread_Limit", None)
        self.output_mapped_flag = self.run_config.get("Output_Only_Mapped", False)
        self.header_mapping = self.run_config.get("Header_Mapping", None)

        # validate target condition 
        if self.thread_limit is None or not isinstance(self.thread_limit, int):
            self.thread_limit = 10

        # validate the batch size 
        if self.batch_size is None or not isinstance(self.batch_size, int):
            self.batch_size = 50000

        # validate boolean fields 
        if not isinstance(self.output_mapped_flag, bool):
            self.message = 'Output only mapped indicator should be boolean. Please correct the configurations'
            raise ValidationException(self.message)

        if '*' in self.header_mapping:
            self.message = 'Header mapping cannot have all columns. Please correct the configurations'
            raise ValidationException(self.message)            


    def _validate_csplit_run_details(self):
        """ validate conditional split run details """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text='Validating Inputs')

        self.thread_limit = self.run_config.get("Thread_Limit", None)
        self.output_columns = self.run_config.get("Output_Columns", None)
        self.output_condition = self.run_config.get("Output_Condition", None)

        # validate target condition 
        if self.thread_limit is None or not isinstance(self.thread_limit, int):
            self.thread_limit = 10

        # validate output columns 
        if '*' in self.output_columns:
            self.output_columns=['*', ]

        # validate target columns 
        if self.output_columns is None:
            self.message = "Corrupted Request Set up for target column list. Please correct the configurations"
            raise ValidationException(self.message)

        # validate target condition 
        if self.output_condition is None:
            self.message = "Corrupted Request Set up for target condition. Please correct the configurations"
            raise ValidationException(self.message)
        

    def perform_split_request(self):
        """ perform horizontal split request """
        
        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text='Running H.Split')
        
        tasks = [] 

        if self.sub_option == "CSV_Horizontal_Slice":
            # iterate on file sublist 
            for subfile_list in self.input_file_sublist:
                thread = Thread(target=CsvManipulation.run_hsplit_process, args=(subfile_list,
                                                                                self.input_file_delimiter,
                                                                                self.input_file_code_page,
                                                                                self.input_header_flag,
                                                                                self.output_location,
                                                                                self.output_file_delimiter,
                                                                                self.output_file_code_page,
                                                                                self.output_header_flag, 
                                                                                self.batch_size))
                thread.start()
                tasks.append(thread)

        elif self.sub_option == "CSV_Vertical_Slice":
            # iterate on file sublist 
            for subfile_list in self.input_file_sublist:
                thread = Thread(target=CsvManipulation.run_vsplit_process, args=(subfile_list,
                                                                                self.input_file_delimiter,
                                                                                self.input_file_code_page,
                                                                                self.output_location,
                                                                                self.output_file_delimiter,
                                                                                self.output_file_code_page,
                                                                                self.output_mapped_flag, 
                                                                                self.header_mapping))
                thread.start()
                tasks.append(thread)

        elif self.sub_option == "CSV_Conditional_Slice":
            # iterate on file sublist 
            for subfile_list in self.input_file_sublist:
                thread = Thread(target=CsvManipulation.run_csplit_process, args=(subfile_list,
                                                                                self.input_file_delimiter,
                                                                                self.input_file_code_page,
                                                                                self.output_location,
                                                                                self.output_file_delimiter,
                                                                                self.output_file_code_page,
                                                                                self.output_columns,
                                                                                self.output_condition))
                thread.start()
                tasks.append(thread)            

        # join the threads 
        for t in tasks:
            t.join()


    def perform_merge_csv_request(self):
        """ merge multiple csv files into one. all small csv file must have a header """

        mylogger.info(friday_reusable.get_function_name())
        self.progress_label.config(text='Merging Files')        

        is_first = True 
        header = [] 

        # create single merge output file 
        with open(os.path.join(self.output_location, 'Merged.csv'), 'w', newline='', encoding=self.output_file_code_page) as output_file:
            output_csv = csv.writer(output_file, delimiter=self.output_file_delimiter)

            for file in self.input_absolute_file_name:
                csv_file = open(file, "r", newline="", encoding=self.input_file_code_page)
                input_csv = csv.reader(csv_file, delimiter=self.input_file_delimiter)

                # when first file and first record - save as header 
                header = next(input_csv)

                # if first file and first record 
                if is_first:
                    is_first = False 
                    output_csv.writerow(header)
                
                # now write all rows from the input csv 
                output_csv.writerows(input_csv)
                csv_file.close()


    @staticmethod
    def run_hsplit_process(subfile_list, input_delimiter, input_codepage, input_header_flag, output_location, output_delimiter, output_codepage, output_header_flag, batch_size):
        """ perform horizontal split request """

        mylogger = logging.getLogger(__name__)
        mylogger.info(friday_reusable.get_function_name())

        # create record list
        record_list = [] 

        for file_name in subfile_list:
            input_file_name, input_file_ext = os.path.splitext(os.path.basename(file_name))
            input_file, input_file_stream = friday_reusable.get_file_handles(file_name=file_name, 
                                                                             file_codepage=input_codepage, 
                                                                             file_delimiter=input_delimiter, 
                                                                             file_type="CSV")
            header = []

            # if input header is present 
            if input_header_flag:
                header = next(input_file_stream)

            # read data stream
            idx = 1 
            for csv_record in input_file_stream:
                record_list.append(csv_record)

                # check the size of record list
                if len(record_list) > batch_size:
                    smaller_file_name = input_file_name + "_horizontal_split_" + str(idx) + input_file_ext
                    idx = CsvManipulation.write_smaller_csv_file(record_list, 
                                                                 output_location, 
                                                                 smaller_file_name, 
                                                                 output_codepage,
                                                                 output_delimiter,
                                                                 output_header_flag, 
                                                                 header,
                                                                 idx)
                    record_list = []

            # if list has some records 
            if record_list:
                smaller_file_name = input_file_name + "_horizontal_split_" + str(idx) + input_file_ext
                idx = CsvManipulation.write_smaller_csv_file(record_list, 
                                                             output_location, 
                                                             smaller_file_name, 
                                                             output_codepage,
                                                             output_delimiter,
                                                             output_header_flag,
                                                             header,
                                                             idx)
                record_list = []

            # close input file 
            input_file.close()


    @staticmethod
    def write_smaller_csv_file(record_list, output_location, smaller_file_name, output_codepage, output_delimiter, output_header_flag, header, idx):
        """ write smaller csv file in batches """

        mylogger = logging.getLogger(__name__)
        mylogger.info(friday_reusable.get_function_name())

        with open(os.path.join(output_location, smaller_file_name), "w", encoding=output_codepage, newline="") as fp:
            csv_writer = csv.writer(fp, delimiter=output_delimiter)
            if output_header_flag:
                csv_writer.writerow(header)
            csv_writer.writerows(record_list)

        idx += 1

        return idx
    

    @staticmethod
    def run_vsplit_process(subfile_list, input_delimiter, input_codepage, output_location, output_delimiter, output_codepage, output_mapped_flag, header_mapping) -> None:
        """ run vertical split mode """

        mylogger = logging.getLogger(__name__)
        mylogger.info(friday_reusable.get_function_name())

        for file_name in subfile_list:
            input_file_name, input_file_ext = os.path.splitext(os.path.basename(file_name))
            input_file, input_file_stream = friday_reusable.get_file_handles(file_name=file_name, 
                                                                             file_codepage=input_codepage, 
                                                                             file_delimiter=input_delimiter, 
                                                                             file_type="CSV")
            header = []
            header = next(input_file_stream)

            # if no header then continue to next file 
            if len(header) == 0:
                continue 

            # create output file handle 
            output_file = open(os.path.join(output_location, input_file_name + "_vertical_split" + input_file_ext), "w", newline="", encoding=output_codepage)
            output_csv = csv.writer(output_file, delimiter=output_delimiter)
            
            # copy header and get selected header
            header_copy = header.copy()

            # remove unnecessary quotes and spaces 
            header_copy = [i.strip("'") for i in header_copy]
            header_copy = [i.strip('"') for i in header_copy]
            header_copy = [i.strip() for i in header_copy]

            # get header_lookup details 
            selected_header, modified_header, header_lookup = CsvManipulation.get_selected_header(header_mapping, header_copy, output_mapped_flag)

            # write header record to output file 
            if output_mapped_flag:
                output_csv.writerow(selected_header)
            else:
                output_csv.writerow(modified_header)

            # Iterate thrugh each row in the csv and write to output 
            for row in input_file_stream: 

                if len(row) == 0: 
                    continue            

                # run loop across each element in header and select data which needs to be written
                modified_row = [] 
                if output_mapped_flag:
                    for i, item in enumerate(row): 
                        if header_copy[i] in header_lookup.keys(): 
                            modified_row.append(item)
                else: 
                    modified_row = row

                output_csv.writerow(modified_row)            

            input_file.close()
            output_file.close()                        


    @staticmethod
    def get_selected_header(header_mapping, header_copy, output_mapped_flag) -> tuple[list, list, dict]:
        """ get header selected """

        mylogger = logging.getLogger(__name__)
        mylogger.info(friday_reusable.get_function_name())

        # create header_lookup 
        header_lookup = {} 
        selected_header = []

        # get header_lookup details 
        for condition in header_mapping:
            find_column, target_column = condition.split("|")

            # remove quotes and spaces 
            find_column = find_column.strip('"')
            find_column = find_column.strip("'")
            find_column = find_column.strip()

            # remove quotes and spaces 
            target_column = target_column.strip('"')
            target_column = target_column.strip("'")
            target_column = target_column.strip()

            if find_column in header_copy:
                header_lookup[find_column] = target_column

        # populate selected header based on lookup value 
        modified_header = header_copy.copy()
        for index, head in enumerate(header_copy): 
            if head in header_lookup.keys():
                modified_header[index] = header_lookup[head]

                if output_mapped_flag:
                    selected_header.append(header_lookup[head])


        return selected_header, modified_header, header_lookup
        

    @staticmethod
    def run_csplit_process(subfile_list, input_delimiter, input_codepage, output_location, output_delimiter, output_codepage, output_columns, output_condition) -> None:
        """ run conditional slice mode """

        mylogger = logging.getLogger(__name__)
        mylogger.info(friday_reusable.get_function_name())

        for file_name in subfile_list:
            input_file_name, input_file_ext = os.path.splitext(os.path.basename(file_name))
            input_file, input_file_stream = friday_reusable.get_file_handles(file_name=file_name, 
                                                                             file_codepage=input_codepage, 
                                                                             file_delimiter=input_delimiter, 
                                                                             file_type="CSV")
            header = []
            header = next(input_file_stream, None)

            # if no header then continue to next file 
            if len(header) == 0:
                continue 

            # create output file handle 
            output_file = open(os.path.join(output_location, input_file_name + "_conditional_split" + input_file_ext), "w", newline="", encoding=output_codepage)
            output_csv = csv.writer(output_file, delimiter=output_delimiter)
            
            # copy header and get selected header
            header_copy = header.copy()

            # write header to outcsv 
            selected_header = []
            if '*' in output_columns:
                output_csv.writerow(header)
            else: 
                for target in output_columns:
                    selected_header.append(target)

                output_csv.writerow(selected_header)            

            # remove unnecessary quotes and spaces 
            header_copy = [i.strip("'") for i in header_copy]
            header_copy = [i.strip('"') for i in header_copy]
            header_copy = [i.strip() for i in header_copy]

            # create condition list 
            condition_list = shlex.split(output_condition)              

            # iterate over each row 
            for row in input_file_stream:

                if len(row) == 0: 
                    continue
                
                condition_flag = TokenizeBase64Csv.validate_token_condition(row, header_copy, condition_list)

                if condition_flag:
                    selected_row = []

                    if '*' in output_columns:
                        output_csv.writerow(row)
                    else: 
                        for target in output_columns:
                            selected_row.append(row[header_copy.index(target)])

                        output_csv.writerow(selected_row)
            
            # close files
            input_file.close()
            output_file.close()
