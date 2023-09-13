''' 
    Created By: Rohit Abhishek 
    Function: This module is collection of various operations to be performed on data.
              This module will accept the data from the GUI interface and performs operations based on the call made by the GUI program.
              Has interface with exception module
'''

import csv
import ctypes as ct
import datetime
import difflib
import fnmatch
import hashlib
import json
import logging
import math
import multiprocessing
import os
import queue
import re
import webbrowser
from abc import ABC
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import astuple, dataclass
from datetime import date, datetime
from decimal import Decimal
from itertools import zip_longest
from multiprocessing import Process, get_context
from threading import Thread
from time import mktime, struct_time

import friday_database
import friday_reusable
from friday_constants import (ATTACH_RELEASE_DB, BASE, BASE_DATABASE,
                              BASE_METADATA_DATABASE, BASE_METADATA_TABLE,
                              BASE_PDF_DATABASE, BASE_PDF_TABLE, BASE_TABLE,
                              COMPARE_REPORT_FILE, CSV, EXCEPTION_FILE_NAME,
                              IMAGE_PREFIX, JSON, MATCH_DATABASE,
                              MATCH_FILE_NAME, MATCH_TABLE, MESSAGE_LOOKUP,
                              MISMATCH_DATABASE, MISMATCH_TABLE,
                              OUT_OF_SEQ_FILE_NAME, RELEASE, RELEASE_DATABASE,
                              RELEASE_METADATA_DATABASE,
                              RELEASE_METADATA_TABLE, RELEASE_PDF_DATABASE,
                              RELEASE_PDF_TABLE, RELEASE_TABLE,
                              UNMATCH_FILE_NAME)
from friday_exception import ProcessingException, ValidationException
from jinja2 import Environment, FileSystemLoader
from multipledispatch import dispatch
from PyPDF2 import PdfReader

# get name of the logger 
mylogger = logging.getLogger(__name__)

# change default CSV size 
csv.field_size_limit(int(ct.c_ulong(-1).value // 2))


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


class CompareFiles(ABC):
    """ Compare file abstract class """

    def __init__(self, config:dict={}, root_option:str="", sub_option:str="", mypath:str="", template_location:str="", deloitte_image:str="", travis_image:str="", application_name: str="", environment_name: str="", run_id:int=1, travis_status_queue:queue.Queue=None, open_browser:bool=True, merge_match_unmatch:bool=False) -> None:

        # initialize variables
        self.__config = config
        self.__root_option = root_option
        self.__sub_option = sub_option
        self.__mypath = mypath
        self.__template_location = template_location
        self.__travis_image = travis_image
        self.__deloitte_image = deloitte_image
        self.__application_name = application_name
        self.__environment_name = environment_name
        self.__run_id = run_id
        self.travis_status_queue = travis_status_queue
        self.__open_browser = open_browser
        self.__merge_match_unmatch = merge_match_unmatch

        # set configurations needed for comparison
        self.base_config = self.__config.get("BaseConfig")
        self.release_config = self.__config.get("ReleaseConfig")
        self.compare_config = self.__config.get("CompareConfig")
        self.output_config = self.__config.get("OutputConfig")

        # get template directories
        self.__env = Environment(loader=FileSystemLoader(self.template_location))
        self.template = self.__env.get_template(COMPARE_REPORT_FILE)

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
        self.output_store_base_release = False
        self.output_generate_summary_flag = None    

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

        # create an instance of status message 
        self.status_message = StatusMessage(self.__run_id, self.__root_option, self.__sub_option, self.output_location, "Initiating", self.message)


    # set the getter setter property for config
    @property
    def config(self):
        """ getter and setter property """
        return self.__config

    @config.setter
    def config(self, config):
        if bool(config): 
            self.message = MESSAGE_LOOKUP.get(8)
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
            self.message = MESSAGE_LOOKUP.get(9)
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
            self.message = MESSAGE_LOOKUP.get(10)
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

    # set the getter setter property for template location
    @property
    def template_location(self):
        """ getter and setter property """
        return self.__template_location

    @template_location.setter
    def template_location(self, template_location):
        if not bool(template_location) and template_location != "":
            self.__template_location = template_location 
        else: 
            self.__present_working_dir = os.path.dirname(os.path.abspath(__file__))
            self.__template_location = os.path.join(self.__present_working_dir, "templates")

    # set the getter setter property for application_name
    @property
    def application_name(self):
        """ getter and setter property """
        return self.__application_name

    @application_name.setter
    def application_name(self, application_name):
        if application_name == "": 
            self.message = MESSAGE_LOOKUP.get(1) %("Application Name")
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
            self.message = MESSAGE_LOOKUP.get(1) %("Environment Name")
            raise ValidationException(self.message)

        if not bool(environment_name):
            self.__environment_name = environment_name

    # set the getter setter property for travis image
    @property
    def travis_image(self):
        """ getter and setter property """
        return self.__travis_image

    @travis_image.setter
    def travis_image(self, travis_image):
        if not bool(travis_image) and travis_image != "":
            self.__travis_image = travis_image 

    # set the getter setter property for deloitte image
    @property
    def deloitte_image(self):
        """ getter and setter property """
        return self.__deloitte_image

    @deloitte_image.setter
    def deloitte_image(self, deloitte_image):
        if not bool(deloitte_image) and deloitte_image != "":
            self.__deloitte_image = deloitte_image     

    # set the getter setter property for run id 
    @property
    def run_id(self):
        """ getter and setter property """
        return self.__run_id

    @run_id.setter
    def run_id(self, run_id):
        if not bool(run_id) and run_id != 0:
            self.__run_id = run_id                 

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
    

    def put_status_message_queue(self, output_location=None, status=None, message=None) -> None: 
        """ put message on status queue """
        
        if output_location is not None: 
            self.status_message.output_location = output_location 

        if status is not None: 
            self.status_message.status = status 
        
        if message is not None: 
            self.status_message.message = message
        
        # put the changed dataclass values on queue 
        self.travis_status_queue.put(astuple(self.status_message))
        # self.treeview.event_generate("<<MessageGenerated>>") if self.treeview else None
        

    def _validate_base_folder_and_files(self) -> None:
        """Validate base folder and file configurations"""

        mylogger.info(friday_reusable.get_function_name())

        # get the base folder and file details 
        self.base_location = self.base_config.get("Base_Location", None)
        self.base_files = self.base_config.get("Base_Files", None)

        # base location validation
        if self.base_location is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Base Location")
            raise ValidationException(self.message)

        # base files validation
        if self.base_files is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Base Files")
            raise ValidationException(self.message)

        # checks if Base Location mentioned
        if self.base_location == "":
            self.message = MESSAGE_LOOKUP.get(12) %("Base Location")
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
            self.message = MESSAGE_LOOKUP.get(11) %("Base")
            raise ValidationException(self.message)


    def _validate_base_details(self) -> None:
        """Validate base batch size and code page configurations"""

        mylogger.info(friday_reusable.get_function_name())

        self.base_file_code_page = self.base_config.get("Base_File_Code_Page", None)

        # base files code page
        if self.base_file_code_page is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Base Files Code Page")
            raise ValidationException(self.message)


    def _validate_base_csv_details(self) -> None:
        """Validate base csv configurations"""

        mylogger.info(friday_reusable.get_function_name())

        self.base_file_delimiter = self.base_config.get("Base_File_Delimiter", None)

        # base delimiter validation
        if self.base_file_delimiter is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Base Files Delimiter")
            raise ValidationException(self.message)


    def _validate_release_folder_and_files(self) -> None:
        """Validate release configurations for CSV Files"""

        mylogger.info(friday_reusable.get_function_name())

        # get the base folder and file details 
        self.release_location = self.release_config.get("Release_Location", None)
        self.release_files = self.release_config.get("Release_Files", None)        

        # release location validation
        if self.release_location is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Release Location")
            raise ValidationException(self.message)

        # release files validation
        if self.release_files is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Release Files")
            raise ValidationException(self.message)

        # checks if release Location mentioned
        if self.release_location == "":
            self.message = MESSAGE_LOOKUP.get(12) %("Release Location")
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
            self.message = MESSAGE_LOOKUP.get(11) %("Release")
            raise ValidationException(self.message)


    def _validate_release_details(self) -> None:
        """Validate release Batch size and code page of files"""

        mylogger.info(friday_reusable.get_function_name())

        self.release_file_code_page = self.release_config.get("Release_File_Code_Page", None)

        # release files code page
        if self.release_file_code_page is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Release File Code Page")
            raise ValidationException(self.message)


    def _validate_release_csv_details(self) -> None:
        """Validate CSV Settings for release side"""

        mylogger.info(friday_reusable.get_function_name())

        self.release_file_delimiter = self.release_config.get("Release_File_Delimiter", None)

        # release delimiter validation
        if self.release_file_delimiter is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Release File Delimiter")
            raise ValidationException(self.message)


    def _validate_compare_details(self) -> None:
        """Validate compare configurations"""

        mylogger.info(friday_reusable.get_function_name())

        self.compare_file_keys = self.compare_config.get("File_Keys", None)

        # file keys
        if self.compare_file_keys is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Compare Keys")
            raise ValidationException(self.message)

        if len(self.compare_file_keys) == 0:
            self.message = MESSAGE_LOOKUP.get(12) %("Compare Keys")
            raise ValidationException(self.message)
        

    def _validate_compare_parent_child_sep(self) -> None:
        """Validate compare configurations"""

        mylogger.info(friday_reusable.get_function_name())

        self.compare_parent_child_sep = self.compare_config.get("Parent_Child_Separator", ".")


    def _validate_compare_skip_details(self):
        """Validate compare skip configurations"""

        mylogger.info(friday_reusable.get_function_name())

        self.compare_skip_fields = self.compare_config.get("Skip_Fields", None)

        # skip fields
        if self.compare_skip_fields is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Compare Skip Fields")
            raise ValidationException(self.message)


    def _validate_compare_case_details(self) -> None:
        """Validate compare case sensitive configurations"""

        mylogger.info(friday_reusable.get_function_name())

        self.compare_case_sensitive = self.compare_config.get("Case_Sensitive_Compare", None)

        # Need matching records flag
        if self.compare_case_sensitive is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Case Sensitive Compare")
            raise ValidationException(self.message)


    def _validate_compare_match_details(self) -> None:
        """Validate compare match configurations"""

        mylogger.info(friday_reusable.get_function_name())

        self.compare_match_flag = self.compare_config.get("Include_Matching_Records", None)

        # Need matching records flag
        if self.compare_match_flag is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Compare Match Flag")
            raise ValidationException(self.message)


    def _validate_compare_process_limit(self) -> None:
        """Validate Process Size"""

        mylogger.info(friday_reusable.get_function_name())

        self.compare_processor_limit = self.compare_config.get("Processor_Limit", None)

        # check if Process limit is non-zero integer value
        if not isinstance(self.compare_processor_limit, int):
            self.message = MESSAGE_LOOKUP.get(12) %("Processor Limit")
            raise ValidationException(self.message)

        if self.compare_processor_limit <= 0:
            self.message = MESSAGE_LOOKUP.get(13) %("Processor Limit")
            raise ValidationException(self.message)


    def _validate_compare_batch_limit(self) -> None:
        """Validate Batch Size"""

        mylogger.info(friday_reusable.get_function_name())
        
        self.compare_batch_size = self.compare_config.get("Batch_Size", None)

        # check if Batch limit is non-zero integer value
        if not isinstance(self.compare_batch_size, int):
            self.message = MESSAGE_LOOKUP.get(12) %("Batch Limit")
            raise ValidationException(self.message)

        if self.compare_batch_size <= 0:
            self.message = MESSAGE_LOOKUP.get(13) %("Batch Limit")
            raise ValidationException(self.message)


    def _validate_output_details(self) -> None:
        """Validate Output Details"""

        mylogger.info(friday_reusable.get_function_name())
        
        self.output_location_dir = self.output_config.get("Output_Location", None)
        self.output_file_code_page = self.output_config.get("Output_File_Code_Page", None)

        # output config fields
        if self.output_location_dir is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Output Location")
            raise ValidationException(self.message)

        # output config fields
        if self.output_file_code_page is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Output Code Page")
            raise ValidationException(self.message)

        # check if output loation is present
        if self.output_location_dir != "":
            validInd, self.message = friday_reusable.validate_folder_location(self.output_config.get("Output_Location"))
            if not validInd:
                raise ValidationException(self.message)
            

    def _validate_output_csv_file_details(self) -> None:
        """Validate Output file delimiter"""

        mylogger.info(friday_reusable.get_function_name())

        self.output_file_delimiter = self.output_config.get("Output_File_Delimiter", None)

        # output config fields
        if self.output_file_delimiter is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Output Delimiter")
            raise ValidationException(self.message)

        # default output delimiter if not populated
        if self.output_file_delimiter == "":
            self.output_file_delimiter = ","


    def _validate_output_store_base_release_details(self) -> None:
        """Validate Output file delimiter"""

        mylogger.info(friday_reusable.get_function_name())

        self.output_store_base_release = self.output_config.get("Output_Store_Base_Release", False)


    def _validate_output_generate_report(self) -> None:
        """Validate Output generate html configurations"""

        mylogger.info(friday_reusable.get_function_name())

        self.output_generate_summary_flag = self.output_config.get("Output_Generate_Summary", None)

        # validate output summary flag
        if self.output_generate_summary_flag is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Keep intermediate files")
            raise ValidationException(self.message)
        

    def _validate_file_keys(self) -> bool:
        """ validate file key value """

        mylogger.info(friday_reusable.get_function_name())
        
        # check if key is not anything other than file_name and file_index 
        if self.compare_file_keys.lower() not in ["file_name", "file_index"]:
            self.message = MESSAGE_LOOKUP.get(12) %("Compare File Keys")
            raise ValidationException

        compare_file_str = ""
        if isinstance(self.compare_file_keys, str):
            compare_file_str = self.compare_file_keys
            self.compare_file_keys = [self.compare_file_keys, ]

        return compare_file_str        


    def validate_input_parameters(self) -> None:
        """ validate input parameters from the configuration file """
        
        mylogger.info(friday_reusable.get_function_name())


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


    def _set_mongo_extract_flag(self) -> None:
        """ set mongo extract flag """

        mylogger.info(friday_reusable.get_function_name())

        # check if the extract is from mongo 
        self.mongo_extract = False
        for key in self.compare_file_keys:
            if "_id" in key and len(self.compare_file_keys) == 1: 
                self.mongo_extract = True


    def _get_base_release_files(self) -> None:
        """ get base and release file details """

        mylogger.info(friday_reusable.get_function_name())
  

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

        # divide the files into multiple chunks 
        self.base_file_sublist = friday_reusable.create_chunks_dict(dict_data=self.base_file_dict, 
                                                                    number_of_chunks=self.compare_processor_limit)
        self.release_file_sublist = friday_reusable.create_chunks_dict(dict_data=self.release_file_dict,
                                                                       number_of_chunks=self.compare_processor_limit)
        

    def _create_json_compare_database(self, store_base_release_data):
        """ create json compare databases """

        # create base and release tables if store option is selected 
        if store_base_release_data:
            base_database = friday_database.JsonCompareDatabase(os.path.join(self.output_location, BASE_DATABASE))
            base_database.create_table(BASE_TABLE)
            base_database.disconnect()

            # create release table 
            release_database = friday_database.JsonCompareDatabase(os.path.join(self.output_location, RELEASE_DATABASE))
            release_database.create_table(RELEASE_TABLE)
            release_database.disconnect()

        # create mismatch table 
        mismatch_database = friday_database.JsonCompareDatabase(os.path.join(self.output_location, MISMATCH_DATABASE))
        mismatch_database.create_compare_table(MISMATCH_TABLE)
        mismatch_database.disconnect()

        # check the match flag and create the matching table 
        if self.compare_match_flag:
            match_database = friday_database.JsonCompareDatabase(os.path.join(self.output_location, MATCH_DATABASE))
            match_database.create_compare_table(MATCH_TABLE)
            match_database.disconnect()         


    def _get_missing_keyid_details(self) -> None:
        """ get missing keyid details """

        # establish connection with base and release databases 
        database_object = friday_database.JsonCompareDatabase(os.path.join(self.output_location, BASE_DATABASE))
        attach_db = (os.path.join(self.output_location, RELEASE_DATABASE),)           

        # get key id in base but not release 
        base_keyid_not_in_release = database_object.get_base_key_id_not_in_release(attach_db=attach_db,
                                                                                   base_table=BASE_TABLE,
                                                                                   release_table=RELEASE_TABLE,
                                                                                   attach_db_name="RELEASE_DB")
        self._write_compare_file(base_keyid_not_in_release, "Thread-a_unmatch.csv")

        # get key id in release but not base 
        release_keyid_not_in_base = database_object.get_release_key_id_not_in_base(attach_db=attach_db,
                                                                                   base_table=BASE_TABLE,
                                                                                   release_table=RELEASE_TABLE,
                                                                                   attach_db_name="RELEASE_DB")
        self._write_compare_file(release_keyid_not_in_base, "Thread-b_unmatch.csv")

        # disconnect the from the database 
        database_object.disconnect()


    def _get_base_release_data_count(self) -> list:
        """ get base count, release count and common key count """

        # get base data count 
        database_object = friday_database.JsonCompareDatabase(os.path.join(self.output_location, BASE_DATABASE))
        base_key_count = database_object.get_key_count(table_name=BASE_TABLE)
        database_object.disconnect()

        # get release count 
        database_object = friday_database.JsonCompareDatabase(os.path.join(self.output_location, RELEASE_DATABASE))
        release_key_count = database_object.get_key_count(table_name=RELEASE_TABLE)
        database_object.disconnect()

        # get common key count 
        database_object = friday_database.JsonCompareDatabase(os.path.join(self.output_location, BASE_DATABASE))
        attach_db = (os.path.join(self.output_location, RELEASE_DATABASE),)       
        common_key_count = database_object.get_common_key_count(attach_db=attach_db,
                                                                base_table=BASE_TABLE,
                                                                release_table=RELEASE_TABLE,
                                                                attach_db_name="RELEASE_DB")
        database_object.disconnect()        


        return base_key_count[0][0], release_key_count[0][0], common_key_count[0][0]
    

    def _get_compare_data_sublist(self, limit, offset_value) -> list:
        """ get compare data """

        # get common key count 
        database_object = friday_database.JsonCompareDatabase(os.path.join(self.output_location, BASE_DATABASE))
        attach_db = (os.path.join(self.output_location, RELEASE_DATABASE),)       
        common_key_count = database_object.get_common_key_count(attach_db=attach_db,
                                                                base_table=BASE_TABLE,
                                                                release_table=RELEASE_TABLE,
                                                                attach_db_name="RELEASE_DB")            
                

    def _write_compare_file(self, missing_iter, file_name) -> None:
        """Write base not found in release and vice versa"""

        mylogger.info(friday_reusable.get_function_name())

        # create missing filename
        missing_file = open(os.path.join(self.output_location, file_name), "w", encoding=self.output_config["Output_File_Code_Page"], newline="")
        missing_file_csv = csv.writer(missing_file, delimiter=self.output_file_delimiter)

        # write all rows
        missing_file_csv.writerows(missing_iter)
        missing_file.close()


    def _merge_temp_files(self) -> None:
        """Merge all temporary files generated in the output location"""

        mylogger.info(friday_reusable.get_function_name())
        self.put_status_message_queue(status="Merging Outputs")

        tasks = []

        # start merging datasets
        merge_mismatch_process = Thread(target=CompareFiles.__consolidate_unmatch_files, args=(self.output_generate_summary_flag,
                                                                                               self.output_file_code_page,
                                                                                               self.output_file_delimiter,
                                                                                               self.output_retain_temp_files,
                                                                                               self.output_location,
                                                                                               UNMATCH_FILE_NAME,
                                                                                               self.output_header,
                                                                                               self.application_name,
                                                                                               self.environment_name))
        merge_mismatch_process.start()
        tasks.append(merge_mismatch_process)

        # check if matching records needed
        if self.compare_match_flag:
            merge_match_process = Thread(target=CompareFiles.__consolidate_match_files, args=(self.output_generate_summary_flag,
                                                                                              self.output_file_code_page,
                                                                                              self.output_file_delimiter,
                                                                                              self.output_retain_temp_files,
                                                                                              self.output_location,
                                                                                              MATCH_FILE_NAME,
                                                                                              self.output_header,
                                                                                              self.application_name,
                                                                                              self.environment_name))
            merge_match_process.start()
            tasks.append(merge_match_process)

        # merge out of sequence files
        merge_oos_process = Thread(target=CompareFiles.__consolidate_oos_files, args=(self.output_file_code_page,
                                                                                      self.output_retain_temp_files,
                                                                                      self.output_location,
                                                                                      OUT_OF_SEQ_FILE_NAME))
        merge_oos_process.start()
        tasks.append(merge_oos_process)

        # merge exception files
        merge_exp_process = Thread(target=CompareFiles.__consolidate_exception_files, args=(self.output_file_code_page,
                                                                                            self.output_retain_temp_files,
                                                                                            self.output_location,
                                                                                            EXCEPTION_FILE_NAME))
        merge_exp_process.start()
        tasks.append(merge_exp_process)

        # join the threads 
        for t in tasks:
            t.join()


    def _generate_data_compare_summary_report(self):
        """Generate HTML Compare report"""

        mylogger.info(friday_reusable.get_function_name())
        self.put_status_message_queue(status="Generating Summary")

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

        # render static html file
        html = self.template.render(page_title_text="Compare Report for " + str(self.sub_option),
                                    img_logo=IMAGE_PREFIX + self.__deloitte_image,
                                    travis_logo=IMAGE_PREFIX + self.__travis_image,
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

        table_description = "Compare Configurations"
        table_description_columns = ("Item", "Value")
        if isinstance(self.compare_file_keys, str):
            compare_file_keys = self.compare_file_keys 
        else: 
            compare_file_keys = str(",".join(self.compare_file_keys))

        table_description_contents = [
            ("Option Selected", self.root_option),
            ("Sub-Option Selected", self.sub_option),
            ("Application Name", self.application_name),
            ("Environment Name", self.environment_name),
            ("Keys Used for Compare", compare_file_keys),
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

        # populate return variables
        table_description = "Compare Summary Details"
        table_description_columns = ("Top 50 Fields with High Variance", "Result")

        # connect to database extract count data for mismatch table 
        database_object = friday_database.TravisDatabase(os.path.join(self.output_location, MISMATCH_DATABASE))
        table_description_contents = database_object.get_count_summary(MISMATCH_TABLE)
        database_object.disconnect()

        return table_description, table_description_columns, table_description_contents
    

    def __get_mismatch_data_count(self) -> 'tuple[str, tuple, list]':
        """get mismatch data summary"""

        mylogger.info(friday_reusable.get_function_name())

        # populate return variables
        table_description = "Sample Field Mismatch Data (Top 50)"
        table_description_columns = (
            "Concatenated Key",
            "Field Name",
            "Base Value",
            "Release Value",
            "Remarks",
        )

        # connect to database extract count data for mismatch table 
        database_object = friday_database.TravisDatabase(os.path.join(self.output_location, MISMATCH_DATABASE))
        table_description_contents = database_object.get_mismatch_sample_data(MISMATCH_TABLE, limit=100)
        database_object.disconnect()

        return table_description, table_description_columns, table_description_contents


    @staticmethod
    def _read_and_store_json_data(base_file_list, base_file_codepage, base_file_delimiter, release_file_list, release_file_codepage, release_file_delimiter, output_location, output_delimiter, key_data, batch_size, parent_child_separator, mongo_extract, file_type) -> None:
        """ read and store the json data """

        get_context().process = base_file_list

        tasks = []

        # start base thread to load the base database 
        for base_index, base_file_dict in enumerate(base_file_list):
            thread_name = "Thread-" + str(base_index)
            t = Thread(target=CompareFiles.__load_json_data, args=(base_file_dict,
                                                                   thread_name,
                                                                   base_file_codepage,
                                                                   base_file_delimiter,
                                                                   output_location,
                                                                   output_delimiter,
                                                                   key_data,
                                                                   batch_size,
                                                                   parent_child_separator,
                                                                   BASE_DATABASE,
                                                                   BASE_TABLE,
                                                                   mongo_extract,
                                                                   file_type))
            t.start()
            tasks.append(t)


        # start release thread to load the release database 
        for release_index, release_file_dict in enumerate(release_file_list):
            thread_name = "Thread-" + str(release_index)
            t = Thread(target=CompareFiles.__load_json_data, args=(release_file_dict,
                                                                   thread_name,
                                                                   release_file_codepage, 
                                                                   release_file_delimiter,
                                                                   output_location,
                                                                   output_delimiter,
                                                                   key_data,
                                                                   batch_size,
                                                                   parent_child_separator,
                                                                   RELEASE_DATABASE,
                                                                   RELEASE_TABLE,
                                                                   mongo_extract,
                                                                   file_type))
            t.start()
            tasks.append(t)

        # join the tasks 
        for t in tasks:
            t.join()

    
    @staticmethod
    def __load_json_data(file_dict, thread_name, file_codepage, file_delimiter, output_location, output_delimiter, key_data, batch_size, parent_child_separator, db_name, table_name, mongo_extract, file_type) -> None:
        """ Load file data to compare database """

        db_location = os.path.join(output_location, db_name)

        # create empty data list
        data_list = []        

        for file, file_index in file_dict.items(): 
            stream_file, stream_data = friday_reusable.get_file_handles(file, 
                                                                        file_codepage=file_codepage,
                                                                        file_delimiter=file_delimiter,
                                                                        file_type=file_type)
            
            header = [] 

            # if file is csv then get the header record 
            if file_type == "CSV":
                header = next(stream_data)
                header = [i.strip() for i in header]

            # check if json load is performed on the json document 
            if isinstance(stream_data, dict):
                stream_data = [stream_data]
            
            # iterate over generator object and flatten the json 
            for index, json_data in enumerate(stream_data):
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
                data_list.append((file_index, file, os.path.basename(file), output_delimiter.join(json_key), json.dumps(flat_json, cls=CustomJSONEncoder), index))

                # if length of data list more than the batch size - initiate process to load the database
                if len(data_list) > batch_size:
                    CompareFiles.__insert_json_data(db_location, table_name, data_list)
                    data_list = []

            # close stream file 
            stream_file.close()
        
        # check if data list < batch size then insert the remaining data 
        if data_list:
            CompareFiles.__insert_json_data(db_location, table_name, data_list)


    @staticmethod
    def __insert_json_data(db_location:str="", table_name:str="", data_list:list=[]) -> None:
        """ insert json data to db location """
        database_object = friday_database.JsonCompareDatabase(db_location)
        database_object.insert_data(table_name, data_list)
        database_object.disconnect()


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
    def __consolidate_unmatch_files(generate_summary, output_code_page, output_delimiter, retain_temp, output_location, unmatch_file_name, output_header, application_name, environment_name) -> None:
        """Consolidate all unmatch file to one"""
        
        database_object = friday_database.TravisDatabase(os.path.join(output_location, MISMATCH_DATABASE))

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
                row_list.append((row[0], row[1], row[2], row[3], row[4], application_name, environment_name)) if generate_summary else None

            # close and remove smaller file
            output_smaller_file.close()
            os.remove(os.path.join(output_location, file)) if not retain_temp else None

            database_object.insert_data(MISMATCH_TABLE, row_list)

        # close merged file
        output_file.close()

        # disconnect from the database 
        database_object.disconnect()

    
    @staticmethod
    def __consolidate_match_files(generate_summary, output_code_page, output_delimiter, retain_temp, output_location, match_file_name, output_header, application_name, environment_name) -> None:
        """Consolidate all matching fields file"""

        database_object = friday_database.TravisDatabase(os.path.join(output_location, MATCH_DATABASE))

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
                row_list.append((row[0], row[1], row[2], row[3], row[4], application_name, environment_name)) if generate_summary else None

            # close and remove smaller files
            output_smaller_file.close()
            os.remove(os.path.join(output_location, file)) if not retain_temp else None

            database_object.insert_data(MATCH_TABLE, row_list)

        output_file.close()
        database_object.disconnect()


    @staticmethod
    def __consolidate_oos_files(output_code_page, retain_temp, output_location, oos_file_name) -> None:
        """Consolidate all oos file"""

        output_file = open(os.path.join(output_location, oos_file_name), "w", encoding=output_code_page)

        # iterate over all files to create single merged file
        for file in fnmatch.filter(os.listdir(output_location), "Thread*oos.csv"):
            output_smaller_file = open(os.path.join(output_location, file), "r", encoding=output_code_page)
            output_file.write(output_smaller_file.read())
            output_smaller_file.close()
            os.remove(os.path.join(output_location, file)) if not retain_temp else None

        output_file.close()


    @staticmethod
    def __consolidate_exception_files(output_code_page, retain_temp, output_location, excp_file_name):
        """ consolidate exception files """

        output_file = open(os.path.join(output_location, excp_file_name), "w", encoding=output_code_page)

        # iterate over all files to create single merged file
        for file in fnmatch.filter(os.listdir(output_location), "Thread*exception.log"):
            output_smaller_file = open(os.path.join(output_location, file), "r", encoding=output_code_page)
            output_file.write(output_smaller_file.read())
            output_smaller_file.close()
            os.remove(os.path.join(output_location, file)) if not retain_temp else None

        output_file.close()
 

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
    def _dynamic_compare_process(process_name, output_location, output_codepage, output_delimiter, process_limit, compare_match_flag, amount_regex, compare_skip_fields, compare_case_sensitive, common_data_extract_queue) -> None:

        get_context().process = process_name

        index = 0 

        while True:
            # check the size of the queue 
            if common_data_extract_queue.qsize() != 0:
                item = common_data_extract_queue.get()
                index += 1

                # get the object 
                if item == "DONE":
                    break 

                # create sublist of the 
                compare_list = friday_reusable.create_chunks(list_data=item, 
                                                             number_of_chunks=process_limit)
                # initialize variable for batches 
                tasks = [] 

                # create argument list 
                arg = (("Thread-", 
                        index, 
                        thread_index, 
                        sublist,
                        compare_match_flag,
                        amount_regex,
                        compare_skip_fields,
                        compare_case_sensitive,
                        output_codepage,
                        output_delimiter,
                        output_location) for thread_index, sublist in enumerate(compare_list))


                with ThreadPoolExecutor(max_workers=process_limit) as executor:
                    futures_to_compare = [executor.submit(CompareFiles._start_batch_compare, "Thread-", index, thread_index, sublist,compare_match_flag,amount_regex,
                                                          compare_skip_fields,compare_case_sensitive,
                                                          output_codepage,output_delimiter,output_location) for thread_index, sublist in enumerate(compare_list)]
                common_data_extract_queue.task_done()

        # common_data_extract_queue.task_done()



class CompareMetaData(CompareFiles):
    """ Compare File Metadata """

    def __init__(self, config: dict = {}, root_option: str = "", sub_option: str = "", mypath: str = "", template_location: str = "", deloitte_image: str = "", travis_image: str = "", application_name: str = "", environment_name: str = "", run_id: int = 1, travis_status_queue: queue.Queue = None, open_browser: bool = True, merge_match_unmatch: bool = False) -> None:
        super().__init__(config, root_option, sub_option, mypath, template_location, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue, open_browser, merge_match_unmatch)
        
        # set processor limit to 2 only for metadata compare and pdf compares 
        self.compare_processor_limit = 2
        self.compare_skip_fields = []


    def compare_files_metadata(self):
        """extract and store compare metadata"""

        mylogger.info(friday_reusable.get_function_name())

        try:
            tasks = [] 

            # put initial message on treeview 
            self.put_status_message_queue(status="Setting Up")

            # validate input parameters 
            self.validate_input_parameters()

            # Key can be either File_Index or File_Name; cannot be both. Set the compare key fields
            compare_file_str = self._validate_file_keys()

            # get base and release files
            self._get_base_release_files()

            # create all database files in output location 
            self.create_metadata_databases()

            # create status message queue 
            self.put_status_message_queue(status="Extracting Metadata")            

            # initiate a process to get the base data and store in the database 
            base_process = Process(target=CompareMetaData.process_metadata_files, args=(self.base_file_sublist, 
                                                                                        self.compare_file_keys,
                                                                                        self.output_location, 
                                                                                        self.output_file_code_page,
                                                                                        self.output_file_delimiter,
                                                                                        BASE))
            base_process.start()
            tasks.append(base_process)

            # create two processes - release process
            release_process = Process(target=CompareMetaData.process_metadata_files, args=(self.release_file_sublist, 
                                                                                           self.compare_file_keys,
                                                                                           self.output_location, 
                                                                                           self.output_file_code_page,
                                                                                           self.output_file_delimiter,
                                                                                           RELEASE))
            release_process.start()
            tasks.append(release_process)  

            # join the tasks 
            for t in tasks: 
                t.join()       

            # create status message queue 
            self.put_status_message_queue(status="Comparing Metadata")

            # peform metadata compare
            self.perform_metadata_compare(compare_file_str)

            # consolidate the temporary files and load to mismatch database
            self._merge_temp_files()

            # generate summary report
            if self.output_generate_summary_flag:
                self._generate_data_compare_summary_report()

        except Exception as e:
            mylogger.critical(str(e))
            self.message = "Error Occured: " + str(e)
            self.put_status_message_queue(status="Error", message=self.message)
            raise ProcessingException(self.message)

        self.message = MESSAGE_LOOKUP.get(14) %("File Metadata Compare", self.output_location)

        # create a status message 
        self.put_status_message_queue(status="Completed", message=self.message)
        

    def validate_input_parameters(self):
        """ Implementation of validate_input_parameter """
        
        mylogger.info(friday_reusable.get_function_name())

        # validate input folder and files provided as input by user 
        self._validate_base_folder_and_files()
        self._validate_release_folder_and_files()

        # validate compare configurations 
        self._validate_compare_details()
        self._validate_compare_match_details()

        # validate output configurations 
        self._validate_output_details()
        self._validate_output_csv_file_details()
        self._validate_output_generate_report()
   

    def create_metadata_databases(self) -> None:
        """ create base and release metadata database """

        mylogger.info(friday_reusable.get_function_name())
        
        # create base table 
        base_database = friday_database.MetadataDatabase(os.path.join(self.output_location, BASE_METADATA_DATABASE))
        base_database.create_table(BASE_METADATA_TABLE)
        base_database.disconnect()

        # create release table 
        release_database = friday_database.MetadataDatabase(os.path.join(self.output_location, RELEASE_METADATA_DATABASE))
        release_database.create_table(RELEASE_METADATA_TABLE)
        release_database.disconnect()

        # create mismatch table 
        mismatch_database = friday_database.MetadataDatabase(os.path.join(self.output_location, MISMATCH_DATABASE))
        mismatch_database.create_compare_table(MISMATCH_TABLE)
        mismatch_database.disconnect()

        # check the match flag and create the matching table 
        if self.compare_match_flag:
            match_database = friday_database.MetadataDatabase(os.path.join(self.output_location, MATCH_DATABASE))
            match_database.create_compare_table(MATCH_TABLE)
            match_database.disconnect()


    @staticmethod
    def process_metadata_files(file_sublist, compare_file_keys, output_location, output_file_code_page, output_file_delimiter, file_type) -> None:
        """ create raw data tables and store the metadata information in the database """

        get_context().process = file_type

        tasks = [] 

        # check the file type and assign database and table name
        if file_type == BASE:
            db_name = BASE_METADATA_DATABASE
            table_name = BASE_METADATA_TABLE
        else: 
            db_name = RELEASE_METADATA_DATABASE
            table_name = RELEASE_METADATA_TABLE

        # iterate over each file and create load pdf threads 
        for index, file_dict in enumerate(file_sublist):
            thread_name = "Thread-" + str(index)
            t = Thread(target=CompareMetaData.load_file_metadata, args=(file_dict,
                                                                        compare_file_keys,
                                                                        output_location,
                                                                        db_name,
                                                                        thread_name,
                                                                        table_name,
                                                                        output_file_code_page,
                                                                        output_file_delimiter))
            t.start()
            tasks.append(t)

        
        for t in tasks: 
            t.join()

    
    @staticmethod
    def load_file_metadata(file_dict, compare_file_keys, output_location, db_name, thread_name, table_name, output_file_code_page, output_file_delimiter) -> None:
        """ load files metadata to base and release database """

        get_context().process = db_name 
        
        data_list = [] 

        for file, file_index in file_dict.items():
            try:
                file_name = os.path.basename(file)
                file_ext = os.path.splitext(file_name)[1][1:]
                file_stat = os.stat(file)
                file_open = open(file, "rb")
                file_text = file_open.read() 
                hash_code = hashlib.md5(file_text).hexdigest()
                file_create_time=datetime.fromtimestamp(file_stat.st_ctime)
                file_modified_time=datetime.fromtimestamp(file_stat.st_mtime)                

                row_tuple = (file_index, file, file_name, file_ext, file_stat.st_size, str(hash_code), file_create_time, file_modified_time)
                data_list.append(row_tuple)
            except Exception as e:
                mylogger.error(
                    f"Exception in getting metadata for {file}\n" + str(e)
                )

        # insert the data to the database 
        database_object = friday_database.MetadataDatabase(os.path.join(output_location, db_name))
        database_object.insert_data(table_name, data_list)
        database_object.disconnect()


    def perform_metadata_compare(self, compare_file_str) -> None:
        """ perform metadata compare """

        database_object = friday_database.MetadataDatabase(os.path.join(self.output_location, BASE_METADATA_DATABASE))
        attach_db = (os.path.join(self.output_location, RELEASE_METADATA_DATABASE),)

        # get base files not found in release 
        base_file_not_in_release = database_object.get_base_file_not_in_release(attach_db=attach_db,
                                                                                db_key=compare_file_str,
                                                                                base_table=BASE_METADATA_TABLE,
                                                                                release_table=RELEASE_METADATA_TABLE,
                                                                                attach_db_name=ATTACH_RELEASE_DB)
        self._write_compare_file(base_file_not_in_release, "Thread-d_unmatch.csv")

        # get release files not found in base 
        release_file_not_in_base = database_object.get_release_file_not_in_base(attach_db=attach_db, 
                                                                                db_key=compare_file_str,
                                                                                base_table=BASE_METADATA_TABLE,
                                                                                release_table=RELEASE_METADATA_TABLE,
                                                                                attach_db_name=ATTACH_RELEASE_DB)
        self._write_compare_file(release_file_not_in_base, "Thread-c_unmatch.csv")

        # get common records which are in both tables for comparision
        base_release_data = database_object.get_base_release_data(attach_db=attach_db, 
                                                                  db_key=compare_file_str,
                                                                  base_table=BASE_METADATA_TABLE,
                                                                  release_table=RELEASE_METADATA_TABLE,
                                                                  attach_db_name=ATTACH_RELEASE_DB)
        
        self.write_compare_details(base_release_data, "Thread-a_unmatch.csv", "Thread-b_match.csv", compare_file_str)

        # disconnect from the database 
        database_object.disconnect()


    def write_compare_details(self, base_release_data, unmatch_file_name, match_file_name, compare_file_str):
        """ create compare report """

        # define a header record 
        header_name = ["File_Index", "File_Name", "File_Type", "File_Size",  "File_Checksum", "File_Create_Timestamp", "File_Modified_Timestamp"]

        # get unmatch file
        unmatch_file = open(os.path.join(self.output_location, unmatch_file_name), "w", encoding=self.output_file_code_page, newline="")
        unmatch_csv_file = csv.writer(unmatch_file, delimiter=self.output_file_delimiter)

        if self.compare_match_flag:
            match_file = open(os.path.join(self.output_location, match_file_name), "w", encoding=self.output_file_code_page, newline="")
            match_csv_file = csv.writer(match_file, delimiter=self.output_file_delimiter)        

        # iterate over row and compare the data 
        for row in base_release_data:

            unmatch_row_list = [] 
            match_row_list = []

            # get base and release key
            base_key = row[1]
            release_key = row[len(row) // 2 + 1]            
           
            # run the loop excluding file index and file name
            for index in range(2, len(row) // 2):
                if row[index] != row[index + len(row) // 2]:
                    unmatch_row = ["|".join([str(base_key), str(release_key)]), header_name[index], row[index], row[index + len(row) // 2], ""]
                    unmatch_row_list.append(unmatch_row)
                elif self.compare_match_flag:
                    match_row = ["|".join([str(base_key), str(release_key)]), header_name[index], row[index], row[index + len(row) // 2], ""]
                    match_row_list.append(match_row)                    

            # write unmatching rows 
            unmatch_csv_file.writerows(unmatch_row_list)

            if self.compare_match_flag:
                match_csv_file.writerows(match_row_list)

        # close the files 
        unmatch_file.close()
        if self.compare_match_flag:
            match_file.close()


class PDFCompare(CompareFiles):
    def __init__(self, config: dict = {}, root_option: str = "", sub_option: str = "", mypath: str = "", template_location: str = "", deloitte_image: str = "", travis_image: str = "", application_name: str = "", environment_name: str = "", run_id: int = 1, travis_status_queue: queue.Queue = None, open_browser: bool = True, merge_match_unmatch: bool = False) -> None:
        super().__init__(config, root_option, sub_option, mypath, template_location, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue, open_browser, merge_match_unmatch)
        
        # set processor limit to 2 only for metadata compare and pdf compares 
        self.compare_processor_limit = 2
        self.compare_skip_fields = []


    def compare_pdf_files(self):
        """ compare pdf files """

        mylogger.info(friday_reusable.get_function_name())
        
        tasks = [] 
        try:
            self.put_status_message_queue(status="Setting Up") 

            # validate the input parameters 
            self.validate_input_parameters()

            # Key can be either File_Index or File_Name; cannot be both. Set the compare key fields
            compare_file_str = self._validate_file_keys()            

            # get base and release file details 
            self._get_base_release_files()

            # create all database files in output location 
            self.create_pdf_databases()

            # create status message queue 
            self.put_status_message_queue(status="Extracting PDF Data")               

            # create two processes - base process. 
            base_process = Process(target=PDFCompare.process_pdf_file, args=(self.base_file_sublist, 
                                                                             self.compare_file_keys,
                                                                             self.output_location, 
                                                                             self.output_file_code_page,
                                                                             self.output_file_delimiter,
                                                                             BASE))
            base_process.start()
            tasks.append(base_process)

            # create two processes - release process
            release_process = Process(target=PDFCompare.process_pdf_file, args=(self.release_file_sublist, 
                                                                                self.compare_file_keys,
                                                                                self.output_location, 
                                                                                self.output_file_code_page,
                                                                                self.output_file_delimiter,
                                                                                RELEASE))
            release_process.start()
            tasks.append(release_process)  

            # join the tasks 
            for t in tasks: 
                t.join()     

            # create status message queue 
            self.put_status_message_queue(status="Comparing PDF")                

            # perform pdf comparision 
            self.perform_checksum_compare(compare_file_str)

            # consolidate all temporary files 
            self._merge_temp_files()

            # create summary report
            if self.output_generate_summary_flag:
                self._generate_data_compare_summary_report()                


        except Exception as e:
            mylogger.critical(str(e))
            self.message = "Error Occured: " + str(e)
            self.put_status_message_queue(status="Error", message=self.message)
            raise ProcessingException(self.message)

        self.message = MESSAGE_LOOKUP.get(14) %("File Metadata Compare", self.output_location)

        # create a status message 
        self.put_status_message_queue(status="Completed", message=self.message)


    def validate_input_parameters(self):
        """ Implementation of validate_input_parameter """        

        mylogger.info(friday_reusable.get_function_name())

        # validate input folder and files provided as input by user 
        self._validate_base_folder_and_files()
        self._validate_release_folder_and_files()

        # validate compare configurations 
        self._validate_compare_details()
        self._validate_compare_match_details()

        # validate output configurations 
        self._validate_output_details()
        self._validate_output_csv_file_details()
        self._validate_output_generate_report()

        # validate text compare flag 
        self._validate_text_compare()


    def _validate_text_compare(self):
        """Validate compare configurations"""

        mylogger.info(friday_reusable.get_function_name())

        self.get_compare_text = self.compare_config.get("Page_Level_Compare_Reports", None)


    def create_pdf_databases(self) -> None:
        """ create PDF Dataases for comparision """

        mylogger.info(friday_reusable.get_function_name())

        # create base table 
        base_database = friday_database.PDFDatabase(os.path.join(self.output_location, BASE_PDF_DATABASE))
        base_database.create_table(BASE_PDF_TABLE)
        base_database.disconnect()

        # create release table 
        release_database = friday_database.PDFDatabase(os.path.join(self.output_location, RELEASE_PDF_DATABASE))
        release_database.create_table(RELEASE_PDF_TABLE)
        release_database.disconnect()

        # create mismatch table 
        mismatch_database = friday_database.PDFDatabase(os.path.join(self.output_location, MISMATCH_DATABASE))
        mismatch_database.create_compare_table(MISMATCH_TABLE)
        mismatch_database.disconnect()

        # check the match flag and create the matching table 
        if self.compare_match_flag:
            match_database = friday_database.PDFDatabase(os.path.join(self.output_location, MATCH_DATABASE))
            match_database.create_compare_table(MATCH_TABLE)
            match_database.disconnect()        


    @staticmethod 
    def process_pdf_file(file_sublist, compare_file_keys, output_location, output_file_code_page, output_file_delimiter, file_type) -> None:

        get_context().process = file_type 

        tasks = [] 

        # create different db and table names 
        if file_type == BASE:
            db_name = BASE_PDF_DATABASE
            table_name = BASE_PDF_TABLE
        else: 
            db_name = RELEASE_PDF_DATABASE
            table_name = RELEASE_PDF_TABLE

        # iterate over each file and create load pdf threads 
        for index, file_dict in enumerate(file_sublist):
            thread_name = "Thread-" + str(index)
            t = Thread(target=PDFCompare.load_pdf_file_data, args=(file_dict,
                                                                   compare_file_keys,
                                                                   output_location,
                                                                   db_name,
                                                                   thread_name,
                                                                   table_name,
                                                                   output_file_code_page,
                                                                   output_file_delimiter))
            t.start()
            tasks.append(t)

        
        for t in tasks: 
            t.join()


    @staticmethod
    def load_pdf_file_data(file_dict, compare_file_keys, output_location, db_name=BASE_DATABASE, thread_name="Thread-z", table_name=BASE_TABLE, output_code_page="utf-8", output_delimiter=",") -> None:
        """extract metadata of file and insert into the database""" 

        data_list = []

        get_context().process = db_name

        exception_file = open(os.path.join(output_location, thread_name + "_exception.log"), "w", encoding=output_code_page)

        for file, file_index in file_dict.items():
            try:
                file_name = os.path.basename(file)
                file_open = PdfReader(file)

                # iterate over each page and provide the checksum value 
                for page_number, page in enumerate(file_open.pages):
                    page_text_content = str(page.extract_text())
                    page_text_checksum = hashlib.md5(bytes(page_text_content, "utf-8")).hexdigest()
                    page_number_text = f"Page_Number_{page_number + 1}"

                    row_tuple = (file_index, file, file_name, page_number_text, page_text_checksum, page_text_content)
                    data_list.append(row_tuple)

            except Exception as e:
                exception_file.write(
                    f"Exception in getting metadata for {file}\n" + str(e)
                )
                continue 

            if data_list: 
                # insert the data to the database 
                database_object = friday_database.PDFDatabase(os.path.join(output_location, db_name))
                database_object.insert_data(table_name, data_list)
                database_object.disconnect()
                data_list = [] 

        if data_list: 
            # insert the data to the database 
            database_object = friday_database.PDFDatabase(os.path.join(output_location, db_name))
            database_object.insert_data(table_name, data_list)
            database_object.disconnect()
            

    def perform_checksum_compare(self, compare_file_str) -> None:
        """ perform checksum compare page level """

        # establish connection with base and release databases 
        database_object = friday_database.PDFDatabase(os.path.join(self.output_location, BASE_PDF_DATABASE))
        attach_db = (os.path.join(self.output_location, RELEASE_PDF_DATABASE),)   

        # call different routines to perform various compares 
        self.get_missing_file_details(database_object, attach_db, compare_file_str)
        # self.get_missing_page_details(database_object, attach_db, compare_file_str)
        
        if self.compare_match_flag:
            self.get_matching_records(database_object, attach_db, compare_file_str)
        non_matching_checksum_rows = self.get_non_matching_records(database_object, attach_db, compare_file_str)

        # perform detailed text compare 
        if self.get_compare_text:
            self.perform_detailed_text_compare(database_object, attach_db, compare_file_str, non_matching_checksum_rows)

        # close the db connection 
        database_object.disconnect()


    def get_missing_file_details(self, database_object, attach_db, compare_file_str) -> None:
        """ get missing file details """

        # get files that are in base but not in release
        base_file_not_in_release = database_object.get_base_file_not_in_release(attach_db=attach_db,
                                                                                db_key=compare_file_str,
                                                                                base_table=BASE_PDF_TABLE,
                                                                                release_table=RELEASE_PDF_TABLE,
                                                                                attach_db_name=ATTACH_RELEASE_DB)
        self._write_compare_file(base_file_not_in_release, "Thread-z_unmatch.csv")

        # get release files not found in base 
        release_file_not_in_base = database_object.get_release_file_not_in_base(attach_db=attach_db, 
                                                                                db_key=compare_file_str,
                                                                                base_table=BASE_PDF_TABLE,
                                                                                release_table=RELEASE_PDF_TABLE,
                                                                                attach_db_name=ATTACH_RELEASE_DB)
        self._write_compare_file(release_file_not_in_base, "Thread-y_unmatch.csv")

    ## TODO - For now ignore the Missing Page Details... SQL needs to be revisited for missing pages
    def get_missing_page_details(self, database_object, attach_db, compare_file_str) -> None:
        """ get missing page details """

        # get base page not found in the release for common documents 
        base_page_not_in_release = database_object.get_base_page_not_in_release_pdf(attach_db=attach_db, 
                                                                                    db_key=compare_file_str,
                                                                                    base_table=BASE_PDF_TABLE,
                                                                                    release_table=RELEASE_PDF_TABLE,
                                                                                    attach_db_name=ATTACH_RELEASE_DB)
        self._write_compare_file(base_page_not_in_release, "Thread-x_unmatch.csv")

        # get base page not found in the release for common documents 
        release_page_not_in_base = database_object.get_release_page_not_in_base_pdf(attach_db=attach_db, 
                                                                                    db_key=compare_file_str,
                                                                                    base_table=BASE_PDF_TABLE,
                                                                                    release_table=RELEASE_PDF_TABLE,
                                                                                    attach_db_name=ATTACH_RELEASE_DB)
        self._write_compare_file(release_page_not_in_base, "Thread-w_unmatch.csv")             

    def get_matching_records(self, database_object, attach_db, compare_file_str) -> None:
        """ get matching records """

        # get list of rows which are common in both base and release but have same checksum values
        page_checksum_rows_match = database_object.get_matching_page_checksum(attach_db=attach_db, 
                                                                              db_key=compare_file_str,
                                                                              base_table=BASE_PDF_TABLE,
                                                                              release_table=RELEASE_PDF_TABLE,
                                                                              attach_db_name=ATTACH_RELEASE_DB)
        self._write_compare_file(page_checksum_rows_match, "Thread-v_match.csv")


    def get_non_matching_records(self, database_object, attach_db, compare_file_str) -> None:
        """ get non matching records """

        # get list of rows which are common in both base and release but have same checksum values
        page_checksum_rows_unmatch = database_object.get_unmatching_page_checksum(attach_db=attach_db, 
                                                                                  db_key=compare_file_str,
                                                                                  base_table=BASE_PDF_TABLE,
                                                                                  release_table=RELEASE_PDF_TABLE,
                                                                                  attach_db_name=ATTACH_RELEASE_DB)
        # get rid of file index 
        unmatched_list = [] 

        for row in page_checksum_rows_unmatch:
            unmatched_list.append((row[2], row[3], row[4], row[5], row[6]))

        self._write_compare_file(unmatched_list, "Thread-u_unmatch.csv")


        return page_checksum_rows_unmatch


    def perform_detailed_text_compare(self, database_object, attach_db, compare_file_str, non_matching_checksum_rows) -> None:
        """ perform detailed text comparision based """ 

        html_difference = difflib.HtmlDiff()
        for row in non_matching_checksum_rows:
            key_field = row[0] if compare_file_str.upper() == "FILE_INDEX" else row[1]
            page_number_str = row[3]
            text_data = database_object.get_page_text(attach_db=attach_db,
                                                      db_key=compare_file_str, 
                                                      key_field=key_field, 
                                                      page_number_str=page_number_str, 
                                                      base_table=BASE_PDF_TABLE, 
                                                      release_table=RELEASE_PDF_TABLE,
                                                      attach_db_name=ATTACH_RELEASE_DB)
            # get base file names and text 
            base_file_name = text_data[0][0]
            base_text = text_data[0][1]
            release_file_name = text_data[0][2]
            release_text = text_data[0][3]

            output_html_name = f"PDF_Compare_Text_File_Number_{key_field}_{page_number_str}.html"

            with open(os.path.join(self.output_location, output_html_name), "w", encoding="utf-8") as output_html:
                html_diff = html_difference.make_file(base_text.splitlines(True), 
                                                      release_text.splitlines(True), 
                                                      base_file_name, 
                                                      release_file_name)
                output_html.write(html_diff)                   



class JsonStreamCompare(CompareFiles):
    """This is Quick Json Compare routine.
    Assumption is both Json has equal number of objects and in same sequence only field values may differ.
    Uses ijson package with C Parser backend. This will enable to stream json file and objects from both files can be captured for comparison.
    """

    def __init__(self, config: dict = {}, root_option: str = "", sub_option: str = "", mypath: str = "", template_location: str = "", deloitte_image: str = "", travis_image: str = "", application_name: str = "", environment_name: str = "", run_id: int = 1, travis_status_queue: queue.Queue = None, open_browser: bool = True, merge_match_unmatch: bool = False) -> None:
        super().__init__(config, root_option, sub_option, mypath, template_location, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue, open_browser, merge_match_unmatch)

    def compare_json_streams(self):
        """ stream json files and compare fields """

        mylogger.info(friday_reusable.get_function_name())

        try:

            self.put_status_message_queue(status="Setting Up")

            # validate input parameters 
            self.validate_input_parameters()

            # get mongo extract flag 
            self._set_mongo_extract_flag()                   

            # get base and release file list
            self._get_base_release_files()

            # create compare databases
            self._create_json_compare_database(self.output_store_base_release)

            # Stream compare must not be used if files are unbalanced 
            if len(self.base_file_name) != len(self.release_file_name):
                self.message = MESSAGE_LOOKUP.get(15) %("JSON")
                raise ProcessingException(self.message)

            self.put_status_message_queue(status="Initiating Compare")

            # start compare process 
            self.initiate_compare_process()

            # consolidate all temporary files 
            self._merge_temp_files()

            # create summary report
            if self.output_generate_summary_flag:
                self._generate_data_compare_summary_report()

        except Exception as e:
            mylogger.critical(str(e))
            self.message = "Error Occured: " + str(e)
            self.put_status_message_queue(status="Error", message=self.message)
            raise ProcessingException(self.message)

        self.message = MESSAGE_LOOKUP.get(14) %("JSON Stream Compare", self.output_location)

        self.put_status_message_queue(status="Completed", message=self.message)


    def validate_input_parameters(self) -> None:
        """ validate all input parameters """

        mylogger.info(friday_reusable.get_function_name())

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



    def initiate_compare_process(self) -> None:
        """ initiate compare process """

        mylogger.info(friday_reusable.get_function_name())

        # iterate and pass the sublist for processing 
        index = 0
        tasks = []

        # check if store base and release data flag is set - if yes, create a parallel process to read and store files in these database 
        if self.output_store_base_release:
            process = Process(target=CompareFiles._read_and_store_json_data, args=(self.base_file_sublist, 
                                                                                   self.base_file_code_page,
                                                                                   self.base_file_delimiter,
                                                                                   self.release_file_sublist,
                                                                                   self.release_file_code_page,
                                                                                   self.release_file_delimiter,
                                                                                   self.output_location, 
                                                                                   self.output_file_delimiter,
                                                                                   self.compare_file_keys,
                                                                                   self.compare_batch_size,
                                                                                   self.compare_parent_child_sep,
                                                                                   self.mongo_extract,
                                                                                   JSON))
            process.start()
            tasks.append(process)

        # create mulitprocessing queue for threads to put data for reporting 
        stream_queue_mgr = multiprocessing.Manager() 
        stream_process_queue = stream_queue_mgr.Queue() 

        # iterate on sublist and initiate process to start comparision 
        for base_sublist, release_sublist in zip(self.base_file_sublist, self.release_file_sublist):
            thread_name = "Thread-" + str(index)

            base_list = base_sublist 
            release_list = release_sublist 

            # check if base sublist is dictionary 
            if isinstance(base_sublist, dict):
                base_list = base_sublist.keys() 

            if isinstance(release_sublist, dict):
                release_list = release_sublist.keys() 

            # start the process to compare the json files 
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
                                                                                     JSON))
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

    def __init__(self, config: dict = {}, root_option: str = "", sub_option: str = "", mypath: str = "", template_location: str = "", deloitte_image: str = "", travis_image: str = "", application_name: str = "", environment_name: str = "", run_id: int = 1, travis_status_queue: queue.Queue = None, open_browser: bool = True, merge_match_unmatch: bool = False) -> None:
        super().__init__(config, root_option, sub_option, mypath, template_location, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue, open_browser, merge_match_unmatch)


    def compare_json_data(self) -> str:
        """ stream data and load to database for comparison """

        mylogger.info(friday_reusable.get_function_name())

        try:

            self.put_status_message_queue(status="Setting Up")

            # validate input parameters 
            self.validate_input_parameters()

            # get mongo_extract flag
            self._set_mongo_extract_flag()

            # get base and release files
            self._get_base_release_files()

            # create compare databases
            self._create_json_compare_database(True)            

            # start compare process 
            self.put_status_message_queue(status="Initiating Compare")
            self.initiate_compare_process()

            # consolidate all temporary files 
            self._merge_temp_files()

            # create summary report
            if self.output_generate_summary_flag:
                self._generate_data_compare_summary_report()            

        except Exception as e:
            mylogger.critical(str(e))
            self.message = "Error Occured: " + str(e)
            self.put_status_message_queue(status="Error", message=self.message)
            raise ProcessingException(self.message)

        self.message = MESSAGE_LOOKUP.get(14) %("JSON Dynamic Compare", self.output_location)
        self.put_status_message_queue(status="Completed", message=self.message)



    def validate_input_parameters(self) -> None:
        """ validate all input parameters """

        mylogger.info(friday_reusable.get_function_name()) 

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


    def initiate_compare_process(self) -> None:

        mylogger.info(friday_reusable.get_function_name()) 

        # iterate and pass the sublist for processing 
        tasks = []

        # check if store base and release is database is set
        CompareFiles._read_and_store_json_data(self.base_file_sublist, 
                                               self.base_file_code_page,
                                               self.base_file_delimiter,
                                               self.release_file_sublist,
                                               self.release_file_code_page,
                                               self.release_file_delimiter,
                                               self.output_location, 
                                               self.output_file_delimiter,
                                               self.compare_file_keys,
                                               self.compare_batch_size,
                                               self.compare_parent_child_sep,
                                               self.mongo_extract,
                                               JSON)

        # once data is loaded -> get key_id missing from base but in release and vice versa 
        self._get_missing_keyid_details()

        # get base, relase and common key count 
        base_key_count, release_key_count, common_key_count = self._get_base_release_data_count()
        self.base_record_count.append(base_key_count)
        self.release_record_count.append(release_key_count)
        self.compare_records_len.append(common_key_count)
        
        # get number of batches 
        number_of_batches = math.ceil(common_key_count / self.compare_batch_size)
        if number_of_batches < 1: 
            number_of_batches = 1

        # set offset fields 
        limit_value = self.compare_batch_size
        offset_value = 0
        key_id = ""

        # the current process will be producer. Create a consumer process to compare the data and create output files
        # create a multiprocessing queue 
        common_data_extract_mgr = multiprocessing.Manager() 
        common_data_extract_queue = common_data_extract_mgr.Queue()

        # start a process to monitor this queue and dispatch data compare 
        consumer_process = Process(target=CompareFiles._dynamic_compare_process, args=('consumer_process',
                                                                                       self.output_location,
                                                                                       self.output_file_code_page,
                                                                                       self.output_file_delimiter,
                                                                                       self.compare_processor_limit,
                                                                                       self.compare_match_flag,
                                                                                       self.amount_regex,
                                                                                       self.compare_skip_fields,
                                                                                       self.compare_case_sensitive,
                                                                                       common_data_extract_queue))
        consumer_process.start()

        # create database object to be executed in the loop 
        database_object = friday_database.JsonCompareDatabase(os.path.join(self.output_location, BASE_DATABASE))
        attach_db = (os.path.join(self.output_location, RELEASE_DATABASE),)   
        connection, cursor = database_object.get_attach_connection_cursor(attach_db=attach_db,
                                                                          attach_db_name=ATTACH_RELEASE_DB)

        # iterate and create chunk for comparison 
        for i in range(number_of_batches):
            compare_list, key_id = database_object.get_compare_data(connection=connection, 
                                                                    cursor=cursor,
                                                                    base_table=BASE_TABLE,
                                                                    release_table=RELEASE_TABLE,
                                                                    limit=limit_value,
                                                                    offset=offset_value,
                                                                    key_id = key_id,
                                                                    attach_db_name=ATTACH_RELEASE_DB)
            offset_value = offset_value + limit_value
                
            # put message on queue 
            if compare_list:
                common_data_extract_queue.put(compare_list)

        # close the connection 
        database_object.detach_disconnect(connection, cursor, ATTACH_RELEASE_DB)

        # put complete message on the queue
        common_data_extract_queue.join()
        common_data_extract_queue.put("DONE")

        # join the compare process for it to complete 
        consumer_process.join()



class CsvStreamCompare(CompareFiles):

    def __init__(self, config: dict = {}, root_option: str = "", sub_option: str = "", mypath: str = "", template_location: str = "", deloitte_image: str = "", travis_image: str = "", application_name: str = "", environment_name: str = "", run_id: int = 1, travis_status_queue: queue.Queue = None, open_browser: bool = True, merge_match_unmatch: bool = False) -> None:
        super().__init__(config, root_option, sub_option, mypath, template_location, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue, open_browser, merge_match_unmatch)

    def compare_csv_streams(self):
        """ Compare Base and Release CSV Files in streaming mode """

        mylogger.info(friday_reusable.get_function_name())

        try:
            self.put_status_message_queue(status="Setting Up")

            # validate input parameters 
            self.validate_input_parameters() 

            # get base and release file list
            self._get_base_release_files()

            # create compare databases
            self._create_json_compare_database(self.output_store_base_release)

            # Stream compare must not be used if files are unbalanced 
            if len(self.base_file_name) != len(self.release_file_name):
                self.message = MESSAGE_LOOKUP.get(15) %("JSON")
                raise ProcessingException(self.message)
            
            self.put_status_message_queue(status="Initiating Compare")

            # start compare process 
            self.initiate_compare_process()

            # consolidate all temporary files 
            self._merge_temp_files()

            # create summary report
            if self.output_generate_summary_flag:
                self._generate_data_compare_summary_report()
            
        except Exception as e:
            mylogger.critical(str(e))
            self.message = "Error Occured: " + str(e)
            self.put_status_message_queue(status="Error", message=self.message)
            raise ProcessingException(self.message)

        self.message = MESSAGE_LOOKUP.get(14) %("CSV Stream Compare", self.output_location)

        self.put_status_message_queue(status="Completed", message=self.message)            


    def validate_input_parameters(self):

        mylogger.info(friday_reusable.get_function_name())

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
        """ initiate compare process """

        mylogger.info(friday_reusable.get_function_name())

        # iterate and pass the sublist for processing 
        index = 0
        tasks = []

        # check if store base and release data flag is set - if yes, create a parallel process to read and store files in these database 
        if self.output_store_base_release:
            process = Process(target=CompareFiles._read_and_store_json_data, args=(self.base_file_sublist, 
                                                                                   self.base_file_code_page,
                                                                                   self.base_file_delimiter,
                                                                                   self.release_file_sublist,
                                                                                   self.release_file_code_page,
                                                                                   self.release_file_delimiter,
                                                                                   self.output_location, 
                                                                                   self.output_file_delimiter,
                                                                                   self.compare_file_keys,
                                                                                   self.compare_batch_size,
                                                                                   self.compare_parent_child_sep,
                                                                                   self.mongo_extract,
                                                                                   CSV))
            process.start()
            tasks.append(process)

        # create mulitprocessing queue for threads to put data for reporting 
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
                                                                                     CSV))
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
    def __init__(self, config: dict = {}, root_option: str = "", sub_option: str = "", mypath: str = "", template_location: str = "", deloitte_image: str = "", travis_image: str = "", application_name: str = "", environment_name: str = "", run_id: int = 1, travis_status_queue: queue.Queue = None, open_browser: bool = True, merge_match_unmatch: bool = False) -> None:
        super().__init__(config, root_option, sub_option, mypath, template_location, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue, open_browser, merge_match_unmatch)

    
    def compare_csv_data(self):
        """ Compare Base and Release CSV Files in streaming mode """

        mylogger.info(friday_reusable.get_function_name())

        try:
            self.put_status_message_queue(status="Setting Up")

            # validate input parameters 
            self.validate_input_parameters()

            # get base and release file list
            self._get_base_release_files()

            # create compare databases
            self._create_json_compare_database(True)

            # Stream compare must not be used if files are unbalanced 
            if len(self.base_file_name) != len(self.release_file_name):
                self.message = MESSAGE_LOOKUP.get(15) %("JSON")
                raise ProcessingException(self.message)
            
            self.put_status_message_queue(status="Initiating Compare")

            # start compare process 
            self.initiate_compare_process()

            # consolidate all temporary files 
            self._merge_temp_files()

            # create summary report
            if self.output_generate_summary_flag:
                self._generate_data_compare_summary_report()
            
        except Exception as e:
            mylogger.critical(str(e))
            self.message = "Error Occured: " + str(e)
            self.put_status_message_queue(status="Error", message=self.message)
            raise ProcessingException(self.message)

        self.message = MESSAGE_LOOKUP.get(14) %("CSV Stream Compare", self.output_location)

        self.put_status_message_queue(status="Completed", message=self.message)


    def validate_input_parameters(self) -> None:
        """ validate all input parameters """

        mylogger.info(friday_reusable.get_function_name()) 

        # validate input files and folders 
        self._validate_base_folder_and_files()
        self._validate_base_details()
        self._validate_base_csv_details()

        # validate release files       
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

        # validate output config 
        self._validate_output_details()
        self._validate_output_csv_file_details()
        self._validate_output_generate_report()


    def initiate_compare_process(self):
        """ initiate compare prcess """

        mylogger.info(friday_reusable.get_function_name())

        # iterate and pass the sublist for processing 
        tasks = []        

        # check if store base and release is database is set
        CompareFiles._read_and_store_json_data(self.base_file_sublist, 
                                                self.base_file_code_page,
                                                self.base_file_delimiter,
                                                self.release_file_sublist,
                                                self.release_file_code_page,
                                                self.release_file_delimiter,
                                                self.output_location, 
                                                self.output_file_delimiter,
                                                self.compare_file_keys,
                                                self.compare_batch_size,
                                                self.compare_parent_child_sep,
                                                self.mongo_extract,
                                                CSV)
        
        # once data is loaded -> get key_id missing from base but present in release or vice-versa
        self._get_missing_keyid_details()

        # get base, release and common key count 
        base_key_count, release_key_count, common_key_count = self._get_base_release_data_count() 
        self.base_record_count.append(base_key_count)
        self.release_record_count.append(release_key_count)
        self.compare_records_len.append(common_key_count)

        # get number of batches 
        number_of_batches = math.ceil(common_key_count / self.compare_batch_size)
        if number_of_batches < 1: 
            number_of_batches = 1        

        # set offset fields 
        limit_value = self.compare_batch_size
        offset_value = 0
        key_id = ""

        # the current process will be producer. Create a consumer process to compare the data and create output files
        # create a multiprocessing queue 
        common_data_extract_mgr = multiprocessing.Manager() 
        common_data_extract_queue = common_data_extract_mgr.Queue()

        # start a process to monitor this queue and dispatch data compare 
        consumer_process = Process(target=CompareFiles._dynamic_compare_process, args=('consumer_process',
                                                                                       self.output_location,
                                                                                       self.output_file_code_page,
                                                                                       self.output_file_delimiter,
                                                                                       self.compare_processor_limit,
                                                                                       self.compare_match_flag,
                                                                                       self.amount_regex,
                                                                                       self.compare_skip_fields,
                                                                                       self.compare_case_sensitive,
                                                                                       common_data_extract_queue))
        consumer_process.start()

        # create database object to be executed in the loop 
        database_object = friday_database.JsonCompareDatabase(os.path.join(self.output_location, BASE_DATABASE))
        attach_db = (os.path.join(self.output_location, RELEASE_DATABASE),)   
        connection, cursor = database_object.get_attach_connection_cursor(attach_db=attach_db,
                                                                          attach_db_name=ATTACH_RELEASE_DB)

        # iterate and create chunk for comparison 
        for i in range(number_of_batches):
            compare_list, key_id = database_object.get_compare_data(connection=connection, 
                                                                    cursor=cursor,
                                                                    base_table=BASE_TABLE,
                                                                    release_table=RELEASE_TABLE,
                                                                    limit=limit_value,
                                                                    offset=offset_value,
                                                                    key_id = key_id,
                                                                    attach_db_name=ATTACH_RELEASE_DB)
            offset_value = offset_value + limit_value
                
            # put message on queue 
            if compare_list:
                common_data_extract_queue.put(compare_list)

        # close the connection 
        database_object.detach_disconnect(connection, cursor, ATTACH_RELEASE_DB)

        # put complete message on the queue
        common_data_extract_queue.join()
        common_data_extract_queue.put("DONE")

        # join the compare process for it to complete 
        consumer_process.join() 