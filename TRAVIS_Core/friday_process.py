''' 
    Created By: Rohit Abhishek 
    Function: This module is collection of various operations to be performed on data.
              This module will accept the data from the GUI interface and performs operations based on the call made by the GUI program.
              Has interface with exception module
'''

import asyncio
import base64
import csv
import ctypes as ct
import datetime
import fnmatch
import json
import logging
import multiprocessing
import os
import queue
import shlex
import subprocess
import threading
from abc import ABC
from collections import OrderedDict
from dataclasses import astuple, dataclass
from datetime import datetime
from multiprocessing import Process, get_context
from threading import Thread

import boto3
import flatten_json
import friday_reusable
import pandas as pd
from botocore.exceptions import ClientError
from friday_compare import CsvDynamicCompare
from friday_constants import (IMAGE_PREFIX, JSON, MESSAGE_LOOKUP,
                              TAGGING_REPORT_FILE)
from friday_exception import ProcessingException, ValidationException
from friday_reusable import CustomJSONEncoder, StatusMessage
from jinja2 import Environment, FileSystemLoader
from pymongo import MongoClient

mylogger = logging.getLogger(__name__)

# change csv default size
csv.field_size_limit(int(ct.c_ulong(-1).value // 2))


class ManipulateData(ABC):
    """ Manipulation interface """

    def __init__(self, config:dict={}, root_option:str="", sub_option:str="", mypath="", template_location:str="", deloitte_image:str="", travis_image:str="", application_name: str="", environment_name: str="", run_id:int=0, travis_status_queue:queue.Queue=None, treeview=None) -> None:

        # initialize variables
        self.__config = config
        self.__root_option = root_option
        self.__sub_option = sub_option
        self.__mypath = mypath
        self.__template_location = template_location
        self.__deloitte_image = deloitte_image
        self.__travis_image = travis_image
        self.__application_name = application_name
        self.__environment_name = environment_name
        self.__run_id = run_id 
        self.travis_status_queue = travis_status_queue
        self.treeview = treeview
        
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


    # set the getter setter property for application_name
    @property
    def application_name(self):
        """ getter and setter property """
        return self.__application_name

    @application_name.setter
    def application_name(self, application_name):
        if application_name == "": 
            self.message = (MESSAGE_LOOKUP.get(1)) %("Application Name")
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
            self.message = (MESSAGE_LOOKUP.get(1)) %("Environment Name")
            raise ValidationException(self.message)

        if not bool(environment_name):
            self.__environment_name = environment_name

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
        self.treeview.event_generate("<<MessageGenerated>>") if self.treeview else None


    def _validate_input_details(self):
        """ validate input file folder and files """

        mylogger.info(friday_reusable.get_function_name())

        self.input_location = self.input_config.get("Input_Location", None)
        self.input_files = self.input_config.get("Input_Files", None)

        # Check other keys present for horizontal cut
        if self.input_location is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Input Location")
            raise ValidationException(self.message)

        # check if base location is present
        validInd, self.message = friday_reusable.validate_folder_location(self.input_location)
        if not validInd:
            raise ValidationException(self.message)

        # Check if selective input flag key is present
        if self.input_files is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Input Files")
            raise ValidationException(self.message)
        
        # check if input is empty or has some file 
        validInd, self.message  = friday_reusable.validate_file_location(self.input_location, 
                                                                         self.input_files)
        if not validInd:
            raise ValidationException(self.message)
        

    def _validate_input_code_page(self):
        """ Validate input code page """

        mylogger.info(friday_reusable.get_function_name())

        self.input_file_code_page = self.input_config.get("Input_File_Code_Page", None)

        # check if input Location key is present
        if self.input_file_code_page is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Input Code Page")
            raise ValidationException(self.message)
        

    def _validate_input_csv_delimiter(self):
        """ Validate input csv delimiters """

        mylogger.info(friday_reusable.get_function_name())

        self.input_file_delimiter = self.input_config.get("Input_Delimiter", None)

        if self.input_file_delimiter is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Input Delimiter")
            raise ValidationException(self.message)
        

    def _validate_output_details(self):
        """ Validate output details """

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

        self.output_file_delimiter = self.output_config.get("Output_Delimiter", None)

        # output config fields
        if self.output_file_delimiter is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Output Delimiter")
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

        self._validate_input_details()
        self._validate_input_code_page()
        self._validate_input_csv_delimiter()

        self._validate_output_details()
        self._validate_output_csv_file_details()


    def _get_input_file_details(self) -> None:
        """ get all input files """

        mylogger.info(friday_reusable.get_function_name())

        self.input_file_dict = friday_reusable.get_all_files_dict(files=self.input_files, 
                                                                  file_location=self.input_location,
                                                                  file_type="input")
        
        self.input_absolute_file_name = list(self.input_file_dict)
        self.input_file_name = [os.path.basename(x) for x in self.input_absolute_file_name]


class TokenizeBase64Csv(ManipulateData):
    """ Manipulate CSV files """
    
    def __init__(self, config: dict = {}, root_option: str = "", sub_option: str = "", mypath="", template_location: str = "", deloitte_image: str = "", travis_image: str = "", application_name: str = "", environment_name: str = "", run_id: int = 0, travis_status_queue: queue.Queue = None, treeview=None) -> None:
        super().__init__(config, root_option, sub_option, mypath, template_location, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue, treeview)

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
            self.message = MESSAGE_LOOKUP.get(14) %("CSV Tokenization", self.output_location)
            self.put_status_message_queue(status="Completed", message=self.message)
        
        except Exception as e:
            mylogger.critical(str(e))
            self.message = "Error Occured: " + str(e)
            self.put_status_message_queue(status="Error", message=self.message)
            raise ProcessingException(self.message)


    def _validate_input_parameters(self):
        """ Validate input parameters """

        mylogger.info(friday_reusable.get_function_name())

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

        # get the run details variable
        self.target_column_list = self.run_config.get("Target_Columns", None)
        self.target_condition = self.run_config.get("Condition", None)
        self.thread_limit = self.run_config.get("Thread_Limit", None)

        # validate target columns 
        if self.target_column_list is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Target Column List")
            raise ValidationException(self.message)

        # validate target condition 
        if self.target_condition is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Target Condition")
            raise ValidationException(self.message)
        
        # validate target condition 
        if self.thread_limit is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Thread Limit")
            raise ValidationException(self.message)

        if len(self.target_column_list) <= 0:
            self.target_column_list = ['*', ]

        if not isinstance(self.thread_limit, int):
            self.thread_limit = 10
        

    def perform_base64_csv_tokenization(self):
        """ Perform CSV Tokenization using base64 """

        mylogger.info(friday_reusable.get_function_name())

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
        for t in tasks:            
            t.join()

    
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
    def __init__(self, config: dict = {}, root_option: str = "", sub_option: str = "", mypath="", template_location: str = "", deloitte_image: str = "", travis_image: str = "", application_name: str = "", environment_name: str = "", run_id: int = 0, travis_status_queue: queue.Queue = None, treeview=None) -> None:
        super().__init__(config, root_option, sub_option, mypath, template_location, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue, treeview)       
        self.run_config = self.config.get("RunConfig", None)
        self.thread_limit = None 
        self.batch_size = None

    def perform_json_manipulation(self):
        """ perform json file split by calling sub routines """

        mylogger.info(friday_reusable.get_function_name())

        # validate input parameters and get input file details 
        self._validate_input_parameters()
        self._get_input_file_details()

        # process Split Json File option request
        if self.sub_option == "Convert_JSON_To_CSV":
            try:
                self._validate_output_csv_file_details()
                
                # get parent child separator for flattened json 
                self.parent_child_separator = self.output_config.get("Output_Child_Separator", ".")

                # get thread limit and batch count 
                self._validate_run_details()

                # break the list into smaller chunks 
                self.input_file_sublist = friday_reusable.create_chunks(list_data=self.input_absolute_file_name, 
                                                                        number_of_chunks=self.thread_limit)
                self.perform_json_conversion()

                # populate message and return control back
                self.message = MESSAGE_LOOKUP.get(14) %("JSON To CSV", self.output_location)

                # create a status message 
                self.put_status_message_queue(status="Completed", message=self.message)
            
            except Exception as e:
                mylogger.critical(str(e))
                self.message = "Error Occured: " + str(e)
                self.put_status_message_queue(status="Error", message=self.message)
                raise ProcessingException(self.message)

        # process split json file request
        elif self.sub_option == "Split_JSON_File":
            try:
                self._validate_run_details()                      
                self.input_file_sublist = friday_reusable.create_chunks(list_data=self.input_absolute_file_name,
                                                                        number_of_chunks=self.thread_limit)
                self.process_json_split()

                # populate message and return control back
                self.message = MESSAGE_LOOKUP.get(14) %("JSON Split", self.output_location)
                
                # create a status message 
                self.put_status_message_queue(status="Completed", message=self.message)
            
            except Exception as e:
                mylogger.critical(str(e))
                self.message = "Error Occured: " + str(e)
                self.put_status_message_queue(status="Error", message=self.message)
                raise ProcessingException(self.message)

        # process json stream merge request
        elif self.sub_option == "Merge_JSON_Stream_Files":
            try:
                self.process_json_stream_merge()

                # populate message and return control back
                self.message = MESSAGE_LOOKUP.get(14) %("Merge JSON Stream Files", self.output_location) 

                # create a status message 
                self.put_status_message_queue(status="Completed", message=self.message)
            
            except Exception as e:
                mylogger.critical(str(e))
                self.message = "Error Occured: " + str(e)
                self.put_status_message_queue(status="Error", message=self.message)
                raise ProcessingException(self.message)         

        # process JSON Load and merge request
        elif self.sub_option == "Merge_JSON_Load_Files":
            try:
                self.process_json_load_merge()

                # populate message and return control back
                self.message = MESSAGE_LOOKUP.get(14) %("Merge JSON Load Files", self.output_location)

                # create a status message 
                self.put_status_message_queue(status="Completed", message=self.message)
            
            except Exception as e:
                mylogger.critical(str(e))
                self.message = "Error Occured: " + str(e)
                self.put_status_message_queue(status="Error", message=self.message)
                raise ProcessingException(self.message)          


    def _validate_input_parameters(self) -> None:
        """ validate input parameters  """

        mylogger.info(friday_reusable.get_function_name()) 
        
        # validate input parameters
        self._validate_input_details()
        self._validate_input_code_page()

        # validate output parameters
        self._validate_output_details()


    def _validate_run_details(self):
        """ validation json split run details """

        mylogger.info(friday_reusable.get_function_name())

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

        output_file = open(os.path.join(self.output_location, 'Merged.json'), 'w', encoding=self.output_file_code_page)

        # write first record 
        output_file.write("[\n")
        first_record = True 

        # iterate over each file 
        for file_name in self.input_absolute_file_name:
            input_file, input_file_stream = friday_reusable.get_file_handles(file_name=file_name, 
                                                                             file_codepage=self.input_file_code_page,
                                                                             file_type=JSON)

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

    def __init__(self, config: dict = {}, root_option: str = "", sub_option: str = "", mypath="", template_location: str = "", deloitte_image: str = "", travis_image: str = "", application_name: str = "", environment_name: str = "", run_id: int = 0, travis_status_queue: queue.Queue = None, treeview=None) -> None:
        super().__init__(config, root_option, sub_option, mypath, template_location, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue, treeview)
        
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
                self.message = MESSAGE_LOOKUP.get(14) %("CSV Horizontal Split", self.output_location)
                
                # create a status message 
                self.put_status_message_queue(status="Completed", message=self.message)
                
            except Exception as e:
                mylogger.critical(str(e))
                self.message = "Error Occured: " + str(e)
                self.put_status_message_queue(status="Error", message=self.message)
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
                self.message = MESSAGE_LOOKUP.get(14) %("CSV Vertical Split", self.output_location)
                
                # create a status message 
                self.put_status_message_queue(status="Completed", message=self.message)

            except Exception as e:
                mylogger.critical(str(e))
                self.message = "Error Occured: " + str(e)
                self.put_status_message_queue(status="Error", message=self.message)
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
                self.message = MESSAGE_LOOKUP.get(14) %("CSV Conditional Split", self.output_location)
                
                # create a status message 
                self.put_status_message_queue(status="Completed", message=self.message)
            
            except Exception as e:
                mylogger.critical(str(e))
                self.message = "Error Occured: " + str(e)
                self.put_status_message_queue(status="Error", message=self.message)
                raise ProcessingException(self.message)

        # merge csv request
        elif self.sub_option == "CSV_Merge_Files":

            try:
                self.perform_merge_csv_request()

                # populate message and return control back
                self.message = MESSAGE_LOOKUP.get(14) %("CSV Merge", self.output_location)
                
                # create a status message 
                self.put_status_message_queue(status="Completed", message=self.message)
                            
            except Exception as e:
                mylogger.critical(str(e))
                self.message = "Error Occured: " + str(e)
                self.put_status_message_queue(status="Error", message=self.message)
                raise ProcessingException(self.message)                


    def _validate_input_parameters(self) -> None:
        """ validate input parameters """

        mylogger.info(friday_reusable.get_function_name())    
        
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
    def get_selected_header(header_mapping, header_copy, output_mapped_flag) -> 'tuple[list, list, dict]':
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


class CreateAwsTask(threading.Thread):

    def __init__(self, region:str="", account_list:list=[], account_lookup:dict={}, thread_name:str="Thread-0-", resource_list:list=[], output_location:str="", output_delimiter:str=",", output_codepage:str="utf-8", snapshot_flag:bool=False, thread_limit:int=10, mandatory_tags:list=[], non_mandatory_tags=[]) -> None:
        
        self.__region = region
        self.__account_list = account_list
        self.__account_lookup = account_lookup
        self.__thread_name = thread_name
        self.__resource_list = resource_list
        self.__output_location = output_location
        self.__output_delimiter = output_delimiter 
        self.__output_codepage = output_codepage
        self.__snapshot_flag = snapshot_flag
        self.__thread_limit = thread_limit
        self.__mandatory_tags = mandatory_tags
        self.__non_mandatory_tags = non_mandatory_tags

        self.__expired_token = False 
        self.start_time = datetime.now()
        self.end_time = datetime.now()

        threading.Thread.__init__(self)


    def run(self):

        # iterate on account numbers in given account_list
        for account in self.__account_list: 
            self.session = boto3.session.Session(profile_name=account)
            row = self.__account_lookup.get(account)
            app_name = "" 
            env_name = ""

            # check if expired token set 
            if self.__expired_token:
                break

            if isinstance(row, list):
                app_name = row[2]
                env_name = row[3]

            # collect tag details 
            self.tag_client = self.session.client('resourcegroupstaggingapi', region_name=self.__region)
            ec2_snapshot_arn_list, rds_snapshot_arn_list, rds_cluster_snapshot_arn_list = self.__main_tagging_process(account, app_name, env_name)

            # check if snapshot flag is set 
            if self.__snapshot_flag:
                if ec2_snapshot_arn_list:
                    self.ec2_client = self.session.client('ec2', region_name=self.__region)
                    self.__main_ec2_snapshot_process(account, ec2_snapshot_arn_list)

                if rds_snapshot_arn_list:
                    self.rds_client = self.session.client('rds', region_name=self.__region)
                    self.__main_rds_snapshot_process(account, rds_snapshot_arn_list)

                if rds_cluster_snapshot_arn_list:
                    self.rds_cluster_client = self.session.client('rds', region_name=self.__region)
                    self.__main_rds_cluster_snapshot_process(account, rds_cluster_snapshot_arn_list)


        # TODO - put the message on queue for Error logging
        print (self.__thread_name, ' Started At: ', self.start_time, ' Ended At: ', datetime.now())


    def __main_tagging_process(self, account:str="", app_name:str="Test", env_name:str="Test") -> 'tuple[list, list, list]':
        """ Extract Tagging Details for given resource """

        parameters = {'IncludeComplianceDetails': True, 
                      'ExcludeCompliantResources': False, 
                      'ResourceTypeFilters': self.__resource_list}

        # set some parameters
        row_number = 1
        mandatory_tags_count = len(self.__mandatory_tags)
        row_list = [] 
        
        # create list of arns 
        ec2_snapshot_arn_list = [] 
        rds_snapshot_arn_list = [] 
        rds_cluster_snapshot_arn_list = []

        # try to get the details for given resources 
        try:
            paginator = self.tag_client.get_paginator('get_resources')

            for page in paginator.paginate(**parameters):

                for resource in page['ResourceTagMappingList']:
                    resource_arn = resource['ResourceARN']
                    all_tags = resource['Tags']

                    # get service name, resource type and resource name 
                    service_name, resource_type, resource_name = friday_reusable.parse_aws_resource_arn(resource_arn)
                    tags_return = self.__set_resource_tags(all_tags)

                    # evaluate the if snapshot is needed
                    if self.__snapshot_flag:
                        if service_name.strip() == "ec2" and resource_type.strip() == "snapshot":
                            ec2_snapshot_arn_list.append([str(account), self.__region, app_name, env_name, resource_arn, resource_name])
                        elif service_name.strip() == "rds" and resource_type.strip() == "snapshot":
                            rds_snapshot_arn_list.append([str(account), self.__region, app_name, env_name, resource_arn, resource_name])
                        elif service_name.strip() == "rds" and resource_type.strip() == "cluster-snapshot":
                            rds_cluster_snapshot_arn_list.append([str(account), self.__region, app_name, env_name, resource_arn, resource_name])

                    # check which mandatory tag is not set 
                    mandatory_tag_notset_count = 0 

                    for tag in tags_return[0:mandatory_tags_count]:
                        if tag == "(not set)":
                            mandatory_tag_notset_count += 1

                    # set the row value 
                    row = [str(account), self.__region, app_name, env_name, resource_arn, resource_name] + \
                          [tags_return[i] for i in range(0, len(self.__mandatory_tags))] + \
                          [mandatory_tag_notset_count, row_number, mandatory_tags_count] + \
                          [tags_return[i] for i in range(len(self.__mandatory_tags), len(self.__mandatory_tags + self.__non_mandatory_tags))]

                    row_list.append(row)

            # write data to thread file 
            with open(os.path.join(self.__output_location, "Tagging_Report_" + self.__thread_name + ".csv"), "a", newline="", encoding=self.__output_codepage) as tag_file:
                tag_csv = csv.writer(tag_file, delimiter=self.__output_delimiter)
                tag_csv.writerows(row_list)

        except ClientError as c:
            if c.response['Error']['Code'] == 'ExpiredToken':
                self.__expired_token = True
            print (self.__thread_name, "Client Error", c)
        except Exception as e:
            print (self.__thread_name, "Exception", e)

        return ec2_snapshot_arn_list, rds_snapshot_arn_list, rds_cluster_snapshot_arn_list


    def __set_resource_tags(self, all_tags) -> list:
        """ set resource tags """

        tag_dict = OrderedDict() 
        
        # set tag values to "not set" as per the input 
        for tag_key in self.__mandatory_tags + self.__non_mandatory_tags:
            tag_dict[tag_key] = "(not set)"
       
        # create the Ordered Dictionary with values 
        for active_tag in all_tags:
            if active_tag['Key'] in self.__mandatory_tags + self.__non_mandatory_tags:
                tag_dict[active_tag['Key']] = active_tag['Value']

        # create list of tags 
        return list(tag_dict.values())
    

    def __main_ec2_snapshot_process(self, account, ec2_snapshot_arn_list):
        """ get ec2 snapshot details """

        ec2_snapshot_id_list = [(resource_arn_sublist[4]).split("/")[-1] for resource_arn_sublist in ec2_snapshot_arn_list]
        row = [] 
        rows = [] 

        try:
            paginator = self.ec2_client.get_paginator('describe_snapshots')
            for page in paginator.paginate():
                for resource in page['Snapshots']:
                    if resource['SnapshotId'] in ec2_snapshot_id_list:
                        row = ec2_snapshot_arn_list[ec2_snapshot_id_list.index(resource['SnapshotId'])] + \
                              [resource['State'], resource["StartTime"], resource["Description"]]
                        rows.append(row)

            # update the data 
            with open(os.path.join(self.__output_location, "Snapshot_Report_" + self.__thread_name + "_ec2.csv"), "a", newline="", encoding=self.__output_codepage) as ec2_snapshot_file:
                ec2_snapshot_csv = csv.writer(ec2_snapshot_file, delimiter=self.__output_delimiter)
                ec2_snapshot_csv.writerows(rows)

        except Exception as e:
            print (self.__thread_name, "ec2-snapshot", account, str(e)[0:200])


    def __main_rds_snapshot_process(self, account, rds_snapshot_arn_list):
        """get rds snapshot details """ 

        rds_snapshot_id_list = [(resource_arn_sublist[4]).split(":")[-1] for resource_arn_sublist in rds_snapshot_arn_list]      

        row = [] 
        rows = [] 

        try:
            paginator = self.rds_client.get_paginator('describe_db_snapshots')
            for page in paginator.paginate():
                for resource in page['DBSnapshots']:
                    if resource['DBSnapshotIdentifier'] in rds_snapshot_id_list:
                        row = rds_snapshot_arn_list[rds_snapshot_id_list.index(resource['DBSnapshotIdentifier'])] + \
                              [resource['Status'], resource["SnapshotCreateTime"], resource["Engine"]]
                        rows.append(row)

            # update the data 
            with open(os.path.join(self.__output_location, "Snapshot_Report_" + self.__thread_name + "_rds.csv"), "a", newline="", encoding=self.__output_codepage) as rds_snapshot_file:
                rds_snapshot_csv = csv.writer(rds_snapshot_file, delimiter=self.__output_delimiter)
                rds_snapshot_csv.writerows(rows)

        except Exception as e:
            print (self.__thread_name, "rds-snapshot", account, str(e)[0:200])


    def __main_rds_cluster_snapshot_process(self, account, rds_cluster_snapshot_arn_list):
        """get rds snapshot details """ 

        rds_cluster_snapshot_id_list = [(resource_arn_sublist[4]).split(":")[-1] for resource_arn_sublist in rds_cluster_snapshot_arn_list]      

        row = [] 
        rows = [] 

        try:
            paginator = self.rds_cluster_client.get_paginator('describe_db_cluster_snapshots')
            for page in paginator.paginate():
                for resource in page['DBClusterSnapshots']:
                    if resource['DBClusterSnapshotIdentifier'] in rds_cluster_snapshot_id_list:
                        row = rds_cluster_snapshot_arn_list[rds_cluster_snapshot_id_list.index(resource['DBClusterSnapshotIdentifier'])] + \
                              [resource['Status'], resource["SnapshotCreateTime"], resource["Engine"]]
                        rows.append(row)

            # update the data 
            with open(os.path.join(self.__output_location, "Snapshot_Report_" + self.__thread_name + "_rds_cluster.csv"), "a", newline="", encoding=self.__output_codepage) as rds_snapshot_file:
                rds_snapshot_csv = csv.writer(rds_snapshot_file, delimiter=self.__output_delimiter)
                rds_snapshot_csv.writerows(rows)

        except Exception as e:
            print (self.__thread_name, "rds-cluster-snapshot", account, str(e)[0:200])


class MigrationUtilities:

    def __init__(self, config:dict={}, root_option:str="", sub_option:str="", mypath:str="", template_location:str="", deloitte_image:str="", travis_image:str="", application_name:str="", environment_name:str="", run_id:int=0, travis_status_queue:queue.Queue=None, treeview=None) -> None:

        # initialize variables 
        self.__config = config 
        self.__root_option = root_option 
        self.__sub_option = sub_option
        self.__mypath = mypath
        self.__template_location = template_location 
        self.__deloitte_image = deloitte_image
        self.__travis_image = travis_image
        self.__application_name = application_name
        self.__environment_name = environment_name
        self.__run_id = run_id
        self.travis_status_queue = travis_status_queue
        self.treeview = treeview


        # create variables for storage 
        self.input_config = None 
        self.output_config = None
        self.run_config = None         

        # get output configurations 
        self.input_config = self.__config.get("InputDetails", None)
        self.output_config = self.__config.get("OutputDetails", None)

        # create workspace directory 
        self.output_location = friday_reusable.perform_data_setup(
            self.output_config.get("Output_Location", ""), 
            self.__mypath,
            str(self.__sub_option),
            self.__application_name,
            self.__environment_name,
            rename_existing=False
        )

        # set up variables 
        self.account_file = None 
        self.resource_list = None 
        self.account_list = None 
        self.mandatory_tag_list = None 
        self.other_tag_list = None 
        self.snapshot_details_flag = None 
        self.excel_flag = None 
        self.thread_limit = None 
        self.send_email = None 
        self.email_list = None 
        self.output_file_code_page = None 
        self.output_file_delimiter = None
        self.random_samples = None 
        self.in_csv = None 

        # header records 
        self.tag_file_header = None 
        self.snapshot_file_header = None 
        self.account_file_header = None 

        # load the template for reporting 
        # self.__parent_working_dir = os.path.dirname(os.path.abspath(__file__))
        # self.__template_location = os.path.join(self.__parent_working_dir, "templates")
        self.__env = Environment(loader=FileSystemLoader(self.__template_location))
        self.template = self.__env.get_template(TAGGING_REPORT_FILE)

        self.start_time=datetime.now()

        # get the region list 
        self.region_list = ["us-east-1", "us-east-2"]

        # set message variable 
        self.message=""

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

    # set the getter setter property for application_name
    @property
    def application_name(self):
        """ getter and setter property """
        return self.__application_name

    @application_name.setter
    def application_name(self, application_name):
        if application_name == "": 
            self.message = (MESSAGE_LOOKUP.get(1)) %("Application Name")
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
            self.message = (MESSAGE_LOOKUP.get(1)) %("Environment Name")
            raise ValidationException(self.message)

        if not bool(environment_name):
            self.__environment_name = environment_name


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
        self.treeview.event_generate("<<MessageGenerated>>") if self.treeview else None


    def perform_aws_operation(self):
        """ perform various aws operations """

        mylogger.info(friday_reusable.get_function_name())    

        try:
            if self.sub_option == "AWS_Cyberark_Refresh":
                # self.perform_aws_token_refresh()
                self.message = MESSAGE_LOOKUP.get(14) %("AWS Cyberark Refresh", self.output_location)

            elif self.sub_option == "AWS_Tagging_Extract":
                self.create_tagging_report()
                self.message = MESSAGE_LOOKUP.get(14) %("AWS Tagging Report", self.output_location)

            # create a status message 
            self.put_status_message_queue(status="Completed", message=self.message)

        except Exception as e:
            mylogger.critical(str(e))
            self.message = "Error Occured: " + str(e)
            self.put_status_message_queue(status="Error", message=self.message)
            raise ProcessingException(self.message)      


    # def perform_aws_token_refresh(self):
    #     """ call samlapi program to perform token refresh """

    #     mylogger.info(friday_reusable.get_function_name())
    #     self.put_status_message_queue(status="Updating Token")        

    #     # create user id and password 
    #     user_id = self.input_config['HA_User_Id']
    #     password = self.input_config['HA_Password']
    #     password = password.strip()

    #     # create full user id        
    #     if "@us.ad.deloitte.com" not in user_id:
    #         user_id = user_id.strip() + "@us.ad.deloitte.com"

    #     # get output codepage 
    #     output_codepage = self.output_config['Output_File_Code_Page']

    #     # create log file 
    #     output_log_file = open(os.path.join(self.output_location, self.sub_option + ".log"), "w", encoding=output_codepage)
        
    #     # create full path for TokenRefresh program 
    #     current_dir = os.path.dirname(os.path.abspath(__file__))
    #     tokenize_pgm = os.path.join(current_dir, "TokenRefresh.py")

    #     # iterate on profile list
    #     for profile in self.input_config['Account_Arn_Profiles']:
    #         profile_arn, profile_name = profile.split("|")
    #         profile_arn = profile_arn.strip()
    #         profile_name = profile_name.strip()

    #         self.put_status_message_queue(status="Updating " + str(profile_name))

    #         # create command for subprocess to run 
    #         command = f"python \"{tokenize_pgm}\" \"{user_id}\" \"{password}\" \"{profile_arn}\" \"{profile_name}\"" 

    #         command_list = shlex.split(command)

    #         process = subprocess.Popen(command_list, 
    #                                 shell=True,
    #                                 stdout=subprocess.PIPE,
    #                                 stderr=subprocess.PIPE)

    #         stdout, stderr = process.communicate()

    #         output_log_file.write(stdout.decode("utf-8"))
    #         output_log_file.write(stderr.decode("utf-8"))

    #     # show notepad 
    #     output_log_file.close()    
    #     show_notepad = "notepad.exe %s" %(os.path.join(self.output_location, self.sub_option + ".log"))
    #     target=os.system(show_notepad)
    

    def create_tagging_report(self): 
        """ Create tagging report for given resources """
        
        mylogger.info(friday_reusable.get_function_name())

        # get the run configurations 
        self.__get_run_details()

        # validate input parameters 
        self.__validate_account_file() 
        self.__validate_resource_list()
        self.__validate_mandatory_tag_list()
        self.__validate_other_tag_list()

        # validate run details 
        self.__validate_snapshot_details()
        self.__validate_excel_details()
        self.__validate_thread_details()
        self.__validate_random_samples()
        self.__validate_email_details()
        self.__validate_retain_files()

        # validate output details 
        self.__validate_output_details()

        # creater header record for each file
        self.__get_header_record()
        
        # get account details 
        _, self.account_lookup = friday_reusable.get_account_list(random_samples=self.random_samples,
                                                                  account_root=self.account_file)

        # create list of accounts 
        self.account_list = list(self.account_lookup.keys())

        # create batches 
        account_chunk_sublist = friday_reusable.create_chunks(list_data=self.account_list,
                                                              number_of_chunks=self.thread_limit)
 
        # initiate threads 
        self.__initiate_aws_account_threads(account_chunk_sublist)

        # merge all temporary files created 
        self.__merge_temporary_files()

        # create summary report for emailer
        p = Process(target=MigrationUtilities.create_summary_items, args=(self.in_csv,
                                                                          self.mandatory_tag,
                                                                          self.output_location))
        p.start()
        p.join()

        # encode images created by the create_summary_items process
        with open(os.path.join(self.output_location, "Tagging Completion.png"), "rb") as image_file:
            overall_tagging_completion_image = (base64.b64encode(image_file.read())).decode()

        with open(os.path.join(self.output_location, "MPE Tagging Completion.png"), "rb") as image_file:
            mpe_tag_completion_image = (base64.b64encode(image_file.read())).decode()

        # generate emailer for stakeholders 
        self.__generate_summary_report(overall_tagging_completion_image, mpe_tag_completion_image)


    def __get_run_details(self):
        """ Get input details from the configurations """

        mylogger.info(friday_reusable.get_function_name())

        self.run_config = self.__config.get("RunDetails", None)


    def __validate_account_file(self):
        """ Validate account input file """

        mylogger.info(friday_reusable.get_function_name())

        self.account_file = self.input_config.get("Account_File", None)
        
        if self.account_file is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Account File")
            raise ValidationException(self.message)
        
        validInd, self.message = friday_reusable.validate_file_location(self.account_file)
        if not validInd:
            raise ValidationException(self.message)        


    def __validate_resource_list(self):
        """ Validate resource list """ 

        mylogger.info(friday_reusable.get_function_name())

        self.resource_list = self.input_config.get("Resource_List", None)
        
        if self.resource_list is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Resource List")
            raise ValidationException(self.message)


    def __validate_mandatory_tag_list(self):
        """ Validate Mandatory tag input list """

        mylogger.info(friday_reusable.get_function_name())

        self.mandatory_tag_list = self.input_config.get("Mandatory_Tags", None)
        
        if self.mandatory_tag_list is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Mandatory Tag List")
            raise ValidationException(self.message)


    def __validate_other_tag_list(self):
        """ Validate other non-mandatory tag list """ 

        mylogger.info(friday_reusable.get_function_name())

        self.other_tag_list = self.input_config.get("Other_Tags", None)
        
        if self.other_tag_list is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Other Tag List")
            raise ValidationException(self.message)


    def __validate_snapshot_details(self):
        """ Validate snapshot flag details """ 

        mylogger.info(friday_reusable.get_function_name())

        self.snapshot_details_flag = self.run_config.get("Get_Snapshot_Details", None)
        
        if self.snapshot_details_flag is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Get Snapshot Flag")
            raise ValidationException(self.message)
        

    def __validate_excel_details(self):
        """ Validate Excel flag for output """ 

        mylogger.info(friday_reusable.get_function_name())

        self.excel_flag = self.run_config.get("Create_Excel_Report", None)
        
        if self.excel_flag is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Excel File Flag")
            raise ValidationException(self.message)


    def __validate_thread_details(self):
        """ Validate thread limit """ 

        mylogger.info(friday_reusable.get_function_name())

        self.thread_limit = self.run_config.get("Thread_Limit", None)
        
        if self.thread_limit is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Thread Limit")
            raise ValidationException(self.message)
        
        if not isinstance(self.thread_limit, int):
            self.message = MESSAGE_LOOKUP.get(13) %("Thread Limit")
            raise ValidationException(self.message)            
        

    def __validate_random_samples(self):
        """ Validate random samples """

        mylogger.info(friday_reusable.get_function_name())

        self.random_samples = self.run_config.get("Account_Random_Samples", None)
        
        if self.random_samples is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Random Account Samples")
            raise ValidationException(self.message)
        
        if not isinstance(self.random_samples, int):
            self.message = MESSAGE_LOOKUP.get(13) %("Random Account Samples")
            raise ValidationException(self.message)    


    def __validate_retain_files(self):
        """ Validate retain temporary file flag """

        mylogger.info(friday_reusable.get_function_name())

        self.retain_temp_files = self.run_config.get("Retain_Temp_Files", None)
        
        if self.retain_temp_files is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Retain Temporary File")
            raise ValidationException(self.message)
                       
        
    def __validate_email_details(self):
        """ Validate email list """

        mylogger.info(friday_reusable.get_function_name())

        self.send_email = self.run_config.get("Send_Email", None)
        self.email_list = self.run_config.get("Email_List", None)
        
        if self.send_email and not self.email_list:
            self.message = MESSAGE_LOOKUP.get(12) %("Send Email Flag")
            raise ValidationException(self.message)     


    def __validate_output_details(self):
        """ Validate output details """

        mylogger.info(friday_reusable.get_function_name())

        self.output_location_dir = self.output_config.get("Output_Location", None)
        self.output_file_code_page = self.output_config.get("Output_File_Code_Page", None)
        self.output_file_delimiter = self.output_config.get("Output_Delimiter", None)

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


    def __get_header_record(self):
        """ get header record and return back to the calling module """

        mylogger.info(friday_reusable.get_function_name())

        # create account details header 
        account_header = ["Account", "Region", "Application_Name", "Environment_Name", "Resource_Arn", "Resource_Name"]

        # create list of mandatory and non mandatory tag list
        mandatory_tags = ["mandatory_tags : " + str(i) for i in self.mandatory_tag_list]
        other_tags = ["tag : " + str(i) for i in self.other_tag_list]

        # create list of counts column 
        count_column_list = ["Mandatory Tag NotSet", "# Row Number", "Total Mandatory Tag Count"]

        # create tag file header 
        self.tag_file_header = account_header + mandatory_tags + count_column_list + other_tags
        self.mandatory_tag = account_header + mandatory_tags + count_column_list

        # create snapshot header 
        snapshot_column_list = ["Status", "Create_Timestamp", "Description"]
        self.snapshot_file_header = account_header + snapshot_column_list


    def __initiate_aws_account_threads(self, account_dict_sublist):
        """ initiate aws account threads """

        mylogger.info(friday_reusable.get_function_name())

        tasks = [] 
        # iterate over each list and initiate thread
        for index, account_list in enumerate(account_dict_sublist):
            for region in self.region_list:
                thread_name = "AWS-Thread-" + str(index) + "_" + region
                t = CreateAwsTask(region=region,
                                  account_list=account_list,
                                  account_lookup=self.account_lookup,
                                  thread_name=thread_name,
                                  resource_list=self.resource_list,
                                  output_location=self.output_location,
                                  output_delimiter=self.output_file_delimiter,
                                  output_codepage=self.output_file_code_page,
                                  snapshot_flag=self.snapshot_details_flag,
                                  thread_limit=self.thread_limit,
                                  mandatory_tags=self.mandatory_tag_list,
                                  non_mandatory_tags=self.other_tag_list)
                t.start()
                tasks.append(t)

        # join the threads 
        for t in tasks:
            t.join()


    def __merge_temporary_files(self):
        """ merge temporary files """

        mylogger.info(friday_reusable.get_function_name())

        self.output_file_name = os.path.join(self.output_location, "Migration_Tagging_Dump.csv")
        self.tagging_extract_data = [] 
        self.snapshot_extract_data = [] 

        # iterate over each tagging report csv and write to worksheet
        for output_temp_file in fnmatch.filter(os.listdir(self.output_location), "Tagging*.csv"):
            df = pd.read_csv(os.path.join(self.output_location, output_temp_file), 
                             header=0,
                             index_col=None,
                             names=self.tag_file_header,
                             dtype=str)
            self.tagging_extract_data.append(df)
            os.remove(os.path.join(self.output_location, output_temp_file)) if not self.retain_temp_files else None

        # create dataframe for emailer
        tagging_df = pd.concat(self.tagging_extract_data, 
                               axis=0, 
                               ignore_index=True)

        if self.snapshot_details_flag and len(fnmatch.filter(os.listdir(self.output_location), "Snapshot*.csv")) > 0:
            for output_temp_file in fnmatch.filter(os.listdir(self.output_location), "Snapshot*.csv"):
                df = pd.read_csv(os.path.join(self.output_location, output_temp_file), 
                                 header=0,
                                 index_col=None,
                                 names=self.snapshot_file_header,
                                 dtype=str)
                self.snapshot_extract_data.append(df)
                os.remove(os.path.join(self.output_location, output_temp_file)) if not self.retain_temp_files else None
            
            snapshot_df = pd.concat(self.snapshot_extract_data, 
                                    axis=0, 
                                    ignore_index=True)

            self.in_csv = pd.merge(tagging_df, 
                                   snapshot_df, 
                                   how="left", 
                                   on=["Account", "Region", "Application_Name", "Environment_Name", "Resource_Arn", "Resource_Name"])

        else: 
            self.in_csv = tagging_df 

        # change the count datatype 
        self.in_csv["# Row Number"] = self.in_csv["# Row Number"].astype(int)
        self.in_csv["Mandatory Tag NotSet"] = self.in_csv["Mandatory Tag NotSet"].astype(int)
        self.in_csv["Total Mandatory Tag Count"] = self.in_csv["Total Mandatory Tag Count"].astype(int)

        # load the data to csv 
        self.output_file_name=os.path.join(self.output_location, "Migration_Tagging_Dump.csv")
        self.in_csv.to_csv(self.output_file_name, index=False)

        if self.excel_flag:
            self.output_file_name=os.path.join(self.output_location, "Migration_Tagging_Dump.xlsx")
            self.in_csv.to_excel(self.output_file_name, sheet_name="Migration_Tagging_Dump", index=False)


    def __generate_summary_report(self, tag_completion_img, mpe_completion_img):
        """ Generate summary report """ 

        mylogger.info(friday_reusable.get_function_name())

        # create summary dataframe 
        summary_df = pd.DataFrame(columns=["Item", "Description"])
        summary_df ["Item"] = ["Total Number of AWS Accounts", 
                               "Account List Input Location", 
                               "Tag Data Dump Location", 
                               "Snapshot Extracted",
                               "Start Time",
                               "End Time"]
        
        summary_df["Description"] = [len(self.account_list), 
                                     '"' + os.path.dirname(self.account_file) + '"', 
                                     '"' + self.output_location + '"',
                                     self.snapshot_details_flag,
                                     self.start_time,
                                     datetime.now()]
        
        # create resource dataframe 
        mandatory_csv = self.in_csv[self.mandatory_tag]
        resource_df = mandatory_csv.groupby("Resource_Name", as_index=False)[["# Row Number", 
                                                                              "Mandatory Tag NotSet", 
                                                                              "Total Mandatory Tag Count"]].agg({"# Row Number" : "sum",
                                                                                                                 "Mandatory Tag NotSet" : "sum",
                                                                                                                 "Total Mandatory Tag Count" : "sum"})
        resource_df.rename(columns={"# Row Number" : "Total Resource Count"}, inplace=True)        
        
        # create mpe tagged resource dataframe 
        mpe_resource_csv = mandatory_csv[mandatory_csv["mandatory_tags : aws-migration-project-id"] == "MPE19072"]
        mpe_resource_df = mpe_resource_csv.groupby("Resource_Name", as_index=False)[["# Row Number", 
                                                                                     "Mandatory Tag NotSet", 
                                                                                     "Total Mandatory Tag Count"]].agg({"# Row Number" : "sum",
                                                                                                                        "Mandatory Tag NotSet" : "sum",
                                                                                                                        "Total Mandatory Tag Count" : "sum"})
        mpe_resource_df.rename(columns={"# Row Number" : "Total Resource Count"}, inplace=True)

        # iterate and store df in list 
        count_df_list = [] 
        for tag_name in self.mandatory_tag:
            if "aws-migration-project-id" not in tag_name and "mandatory_tags : " in tag_name:
                count_df = mpe_resource_csv[f"{tag_name}"].value_counts().reset_index()
                count_df.columns = [f"{tag_name}", "Count"]
                count_df_list.append(count_df.to_html(index=False))

        # create detailed summary report 
        self.__create_detailed_summary_report(summary_df, resource_df, mpe_resource_df, count_df_list, tag_completion_img, mpe_completion_img)
        self.__create_summary_report(summary_df, resource_df, mpe_resource_df, tag_completion_img, mpe_completion_img)

        # check if send email flag is set 
        if self.send_email:
            self.__create_and_send_email(summary_df)


    def __create_detailed_summary_report(self, summary_df, resource_df, mpe_resource_df, count_df_list, tag_completion_img, mpe_completion_img):
        """Create detailed summary reort""" 

        mylogger.info(friday_reusable.get_function_name())    

        # create detailed summary report 
        html_detailed = self.template.render(page_title_text="Tagging Report for" + str(self.sub_option),
                                             img_logo=IMAGE_PREFIX + self.__deloitte_image,
                                             travis_logo=IMAGE_PREFIX + self.__travis_image,
                                             title_text_1="Tagging Summary Report",
                                             date_time=str(datetime.now()),

                                             summary_report="Program Execution Summary Details",
                                             summary_data=summary_df.to_html(index=False),

                                             overall_tagging_summary="Overall Tagging Summary for All Resources",
                                             overall_summary_img=IMAGE_PREFIX + tag_completion_img,

                                             all_resource_header="Tag Count for all Resources",
                                             all_resource_count=resource_df.to_html(index=False),

                                             mpe_tagging_summary="Overall Tagging Summary for all Resources with MPE19072 Tag",
                                             mpe_tag_summary_img=IMAGE_PREFIX + mpe_completion_img,
                                             
                                             mpe_resource_header="Tag Count for Resources with MPE19072",
                                             mpe_resource_count=mpe_resource_df.to_html(index=False),

                                             section_header="Count of Mandatory Tag Values Populated for Resources with MPE19072",
                                             table_list=count_df_list,)

        output_name=self.sub_option + "_Detailed_Summary.html"
        with open(os.path.join(self.output_location, output_name), "w") as html_report:
            html_report.write(html_detailed)
        

    def __create_summary_report(self, summary_df, resource_df, mpe_resource_df, tag_completion_img, mpe_completion_img):
        """ Create Summary report """

        mylogger.info(friday_reusable.get_function_name())

        # create detailed summary report 
        html_detailed = self.template.render(page_title_text="Tagging Report for" + str(self.sub_option),
                                             img_logo=IMAGE_PREFIX + self.__deloitte_image,
                                             travis_logo=IMAGE_PREFIX + self.__travis_image,
                                             title_text_1="Tagging Summary Report",
                                             date_time=str(datetime.now()),

                                             summary_report="Program Execution Summary Details",
                                             summary_data=summary_df.to_html(index=False),

                                             overall_tagging_summary="Overall Tagging Summary for All Resources",
                                             overall_summary_img=IMAGE_PREFIX + tag_completion_img,

                                             all_resource_header="Tag Count for all Resources",
                                             all_resource_count=resource_df.to_html(index=False),

                                             mpe_tagging_summary="Overall Tagging Summary for all Resources with MPE19072 Tag",
                                             mpe_tag_summary_img=IMAGE_PREFIX + mpe_completion_img,
                                             
                                             mpe_resource_header="Tag Count for Resources with MPE19072",
                                             mpe_resource_count=mpe_resource_df.to_html(index=False),

                                             section_header="Tables below are removed intentionally",)

        output_name=self.sub_option + "_Summary.html"
        with open(os.path.join(self.output_location, output_name), "w") as html_report:
            html_report.write(html_detailed)


    def __create_and_send_email(self, summary_df):
        """ Create and send email """ 

        mylogger.info(friday_reusable.get_function_name())
        
        current_date=datetime.now()
        
        email_subject=f"Tagging Dump for Cloud Migration Team {current_date}"
        email_html_body=f""" <p>Hello,</p>
        <p>Please find Tagging Dump pulled from AWS today {current_date} on EH Shared Drive: <br>
            <b> {'"' + str(self.output_file_name) + '"'} </b></p>

            <p><b>Important</b> - Format of spreadsheet has changed. <br>
            Refer <b>Column G to S</b> for Mandatory tags. Column name is prefixed with <i>"tag_mandatory"</i> keyword. <br>
            Refer <b>Column T to V</b> for Mandatory tag count per resource. <br>
            Refer <b>Column W to AV</b> for Other tags, including container_cluster_name. <br>
            Refer <b>Column AW to AY</b> for snapshots (please refer program execution table). <br>
            Tags neither found nor populated on AWS are marked as <i>"(not set)"</i></p>
            <br><br>
            <p><b><u>See below program execution summary details </u></b></p>
            {summary_df.to_html(index=False)}
            <br><br>
            <p>HTML Summary report attached for quick reference</p>
            <br>
            <p>Thank you<br>
            DB Migration Team</p>"""

        # add attachment 
        attachment_list=[os.path.join(self.output_location, self.sub_option + "_Summary.html"), ]
        friday_reusable.send_notification(email_to_list=self.email_list, 
                                          email_subject=email_subject,
                                          email_html_body=email_html_body,
                                          email_attachments=attachment_list,
                                          email_from="")


    @staticmethod
    def create_summary_items(in_csv, mandatory_header, output_location):
        get_context().process = "create_tag_report"

        # get mandatory tag csv 
        mandatory_csv = in_csv[mandatory_header]
        MigrationUtilities.generate_all_tags_chart(mandatory_csv, output_location)
        MigrationUtilities.generate_mpe_tags_chart(mandatory_csv, output_location)


    @staticmethod
    def generate_all_tags_chart(mandatory_csv, output_location):
        """ generate charts for tagging report """

        # group by row number
        mandatory_tag_summary = mandatory_csv.groupby("# Row Number", as_index=False)[["Mandatory Tag NotSet", 
                                                                                       "Total Mandatory Tag Count"]].agg({"Mandatory Tag NotSet": "sum", 
                                                                                                                          "Total Mandatory Tag Count": "sum"})
        mandatory_tag_summary["Mandatory Tag Set"] = mandatory_tag_summary["Total Mandatory Tag Count"] - mandatory_tag_summary["Mandatory Tag NotSet"]

        # drop row not needed 
        mandatory_tag_summary=mandatory_tag_summary.drop(["# Row Number"], axis=1)
        mandatory_tag_summary=mandatory_tag_summary.drop(["Total Mandatory Tag Count", ], axis=1)

        mandatory_tag_transposed=mandatory_tag_summary.T.reset_index().set_axis(["Tag Details", 
                                                                                 "Count"], axis=1, inplace=False)
        mandatory_tag_transposed=mandatory_tag_transposed.set_index(["Tag Details", ])

        # create the piechart 
        ax = mandatory_tag_transposed.plot(kind="pie", 
                                           y="Count",
                                           figsize=(5, 5), 
                                           autopct="%.2f%%", 
                                           shadow=True,
                                           explode=[0.25, 0])
        ax.legend(bbox_to_anchor=(1,0), loc="lower right")
        ax.axis("off")

        # save pie chart 
        figure_filename="Tagging Completion.png"
        ax.figure.savefig(os.path.join(output_location, figure_filename), bbox_inches="tight")


    @staticmethod 
    def generate_mpe_tags_chart(mandatory_csv, output_location):
        """ generate chart for mpe tagged resources """

        # filter MPE data 
        mandatory_csv_mpe = mandatory_csv[mandatory_csv["mandatory_tags : aws-migration-project-id"] == "MPE19072"]
        mandatory_mpe_summary=mandatory_csv_mpe.groupby("# Row Number", as_index=False)[["Mandatory Tag NotSet", 
                                                                                         "Total Mandatory Tag Count"]].agg({"Mandatory Tag NotSet" : "sum", 
                                                                                                                            "Total Mandatory Tag Count" : "sum"})
        mandatory_mpe_summary["Mandatory Tag Set"] = mandatory_mpe_summary["Total Mandatory Tag Count"] - mandatory_mpe_summary["Mandatory Tag NotSet"]

        # drop row not needed 
        mandatory_mpe_summary=mandatory_mpe_summary.drop(["# Row Number"], axis=1)
        mandatory_mpe_summary=mandatory_mpe_summary.drop(["Total Mandatory Tag Count"], axis=1)
        mandatory_mpe_transposed=mandatory_mpe_summary.T.reset_index().set_axis(["Tag Details", "Count"], axis=1, inplace=False)
        mandatory_mpe_transposed=mandatory_mpe_transposed.set_index(["Tag Details", ])

        # create the piechart 
        ax=mandatory_mpe_transposed.plot(kind="pie",
                                         y="Count",
                                         figsize=(5, 5),
                                         autopct="%.2f%%", 
                                         shadow=True,
                                         explode=[0.25, 0])
        ax.axis("off")

        # save pie chart 
        figure_filename="MPE Tagging Completion.png"
        ax.figure.savefig(os.path.join(output_location, figure_filename), bbox_inches="tight")


class MongoDbOperations(ABC):
    def __init__(self, config:dict={}, root_option:str="", sub_option:str="", mypath:str="", template_location:str="", deloitte_image:str="", travis_image:str="", application_name:str="", environment_name:str="", run_id:int=0, travis_status_queue:queue.Queue=None, treeview=None) -> None:
        
        # initialize variables
        self.__config = config
        self.__root_option = root_option
        self.__sub_option = sub_option
        self.__mypath = mypath
        self.__template_location = template_location 
        self.__deloitte_image = deloitte_image 
        self.__travis_image = travis_image
        self.__run_id = run_id 
        self.travis_status_queue = travis_status_queue
        self.treeview = treeview 

        self.__application_name = application_name
        self.__environment_name = environment_name

        # get mongo utilities directory 
        self.mongo_dump_utility = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mongo_utilities", "bin", "mongodump.exe")
        self.mongo_export_utility = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mongo_utilities", "bin", "mongoexport.exe")
        self.mongo_bson_dump_utility = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mongo_utilities", "bin", "bsondump.exe")
        self.mongo_import_utility = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mongo_utilities", "bin", "mongoimport.exe")
        self.mongo_restore_utility = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mongo_utilities", "bin", "mongorestore.exe")

        # in all cases output configurations will be always present 
        self.output_config = self.__config.get("OutputConfig", None)

        # get template directories
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

        # set variables required for establishing connection to source database
        self.base_config = None 
        self.base_host_name = None
        self.base_host_port = None 
        self.base_user_id = None 
        self.base_password = None 
        self.base_database = None 
        self.base_auth_src = None 
        self.base_tls_ca_file = None

        # set variables required for establishing connection to target database 
        self.release_config = None 
        self.release_host_name = None
        self.release_host_port = None 
        self.release_user_id = None 
        self.release_password = None 
        self.release_database = None 
        self.release_auth_src = None 
        self.release_tls_ca_file = None         

        # set variables required for using mongo utilities 
        self.database_config = None 
        self.host_name = None 
        self.host_port = None 
        self.user_id = None 
        self.password = None 
        self.database = None 
        self.auth_src = None 
        self.tls_ca_file = None 

        # set variables for Mongo details for import - export 
        self.mongo_config = None
        self.collection_list = None
        self.collection_name = None 
        self.thread_limit = None 
        self.get_dump_in_json = False 
        self.get_dump_in_canonical = False
        self.get_dump_in_arraylist = False
        self.input_location = None 
        self.input_files = None 
        self.input_file_code_page = None 
        self.generate_scripts = False  
        self.generate_statistics = False
        self.generate_summary = False
        
        # mongo specific folders 
        self.log_location = None 
        self.dump_location = None 
        self.stats_location = None

        # output variables
        self.output_file_code_page = "utf-8" 
        self.output_file_delimiter = ","

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
            self.message = (MESSAGE_LOOKUP.get(1)) %("Application Name")
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
            self.message = (MESSAGE_LOOKUP.get(1)) %("Environment Name")
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
        self.treeview.event_generate("<<MessageGenerated>>") if self.treeview else None
        

    def _get_base_details(self):
        """ get source mongo details """

        mylogger.info(friday_reusable.get_function_name())

        # get the source details 
        self.base_config = self.__config.get("BaseDetails", None)


    def _get_release_details(self):
        """ get source mongo details """

        mylogger.info(friday_reusable.get_function_name())

        # get the source details 
        self.release_config = self.__config.get("ReleaseDetails", None)


    def _get_mongo_database_details(self):
        """ get mongo database details """

        mylogger.info(friday_reusable.get_function_name())

        # get the source details 
        self.database_config = self.__config.get("DatabaseDetails", None)   


    def _get_mongo_config_details(self):
        """ get run configurations """

        mylogger.info(friday_reusable.get_function_name())

        # get the source details 
        self.mongo_config = self.__config.get("MongoConfig", None)
   

    def _validate_base_details(self):
        """ validate source mongo details """

        mylogger.info(friday_reusable.get_function_name())
        
        # populate base detail fields 
        self.base_host_name = self.base_config.get("Base_Host_Name", None)
        self.base_host_port = self.base_config.get("Base_Host_Port", None)
        self.base_user_id = self.base_config.get("Base_User", None)
        self.base_password = self.base_config.get("Base_Password", None)
        self.base_database = self.base_config.get("Base_Database", None)
        self.base_auth_src = self.base_config.get("Base_Auth_Source", None)
        self.base_tls_ca_file = self.base_config.get("Base_Tls_CA_File", None)
        self.base_tls_flag = False

        # validate base details 
        if self.base_host_name is None: 
            self.message = MESSAGE_LOOKUP.get(12) %("Source Hostname")
            raise ValidationException(self.message)
        
        # validate if base port number provided 
        if self.base_host_port is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Source Port")
            raise ValidationException(self.message)

        # validate if base port number is integer 
        if not isinstance(self.base_host_port, int):
            try: 
                self.base_host_port = int(self.base_host_port)
            except:
                self.message = MESSAGE_LOOKUP.get(13) %("Source Database Port")
                raise ValidationException(self.message)        

        if self.base_tls_ca_file is not None:
            self.base_tls_flag = True


    def _validate_release_details(self):
        """ validate source mongo details """

        mylogger.info(friday_reusable.get_function_name())
       
        # populate release detail fields 
        self.release_host_name = self.release_config.get("Release_Host_Name", None)
        self.release_host_port = self.release_config.get("Release_Host_Port", None)
        self.release_user_id = self.release_config.get("Release_User", None)
        self.release_password = self.release_config.get("Release_Password", None)
        self.release_database = self.release_config.get("Release_Database", None)
        self.release_auth_src = self.release_config.get("Release_Auth_Source", None)
        self.release_tls_ca_file = self.release_config.get("Release_Tls_CA_File", None)
        self.release_tls_flag = False

        # validate release details 
        if self.release_host_name is None: 
            self.message = MESSAGE_LOOKUP.get(12) %("Target Hostname")
            raise ValidationException(self.message)
        
        if self.release_host_port is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Target Port")
            raise ValidationException(self.message)         
        
        if not isinstance(self.release_host_port, int):
            try: 
                self.release_host_port = int(self.release_host_port)
            except:
                self.message = MESSAGE_LOOKUP.get(13) %("Target Database Port")
                raise ValidationException(self.message)        

        if self.release_tls_ca_file is not None:
            self.release_tls_flag = True


    def _validate_mongo_database_details(self):
        """ validate mongo database details """

        mylogger.info(friday_reusable.get_function_name())
       
        # populate release detail fields 
        self.host_name = self.database_config.get("Host_Name", None)
        self.host_port = self.database_config.get("Host_Port", None)
        self.user_id = self.database_config.get("User", None)
        self.password = self.database_config.get("Password", None)
        self.database = self.database_config.get("Database", None)
        self.auth_src = self.database_config.get("Auth_Source", None)
        self.tls_ca_file = self.database_config.get("Tls_CA_File", None)   
        self.tls_flag = False

        # validate release details 
        if self.host_name is None: 
            self.message = MESSAGE_LOOKUP.get(12) %("Source Hostname")
            raise ValidationException(self.message)
        
        if self.host_port is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Source Port")
            raise ValidationException(self.message)    
        
        if not isinstance(self.host_port, int):
            try: 
                self.host_port = int(self.host_port)
            except:
                self.message = MESSAGE_LOOKUP.get(13) %("Source Database Port")
                raise ValidationException(self.message)
        
        if self.tls_ca_file is not None:
            self.tls_flag = True


    def _validate_output_config_details(self):
        """ validate mongo output database details """

        mylogger.info(friday_reusable.get_function_name())

        self.output_location_dir = self.output_config.get("Output_Location", None)
        self.output_file_code_page = self.output_config.get("Output_File_Code_Page", None)
        self.output_file_delimiter = self.output_config.get("Output_Delimiter", ",")

        # output config fields
        if self.output_location_dir is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Output Location")
            raise ValidationException(self.message)

        # output config fields
        if self.output_file_code_page is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Output codepage")
            raise ValidationException(self.message)

        # check if output loation is present
        if self.output_location_dir != "":
            validInd, self.message = friday_reusable.validate_folder_location(self.output_config.get("Output_Location"))
            if not validInd:
                raise ValidationException(self.message)


    def _validate_generate_summary(self):
        """ validate generate summary flag """

        mylogger.info(friday_reusable.get_function_name())

        self.generate_summary = self.mongo_config.get("Generate_Summary", None)        

        if self.generate_summary is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Generate Summary")
            raise ValidationException(self.message)            


    def _validate_collection_list(self):
        """ validate and assign collection details input by user """

        mylogger.info(friday_reusable.get_function_name())

        # get the source details 
        self.collection_list = self.mongo_config.get("Collection_List", None)

        if self.collection_list is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Collection List")
            raise ValidationException(self.message)
        
    def _validate_collection_name(self):
        """ Override method for collection name for insert operation """

        self.collection_name = self.database_config.get("Collection_Name")
        
        # insert operation support one collection name only. One cannot provide multiple collection name 
        if "," in self.collection_name: 
            self.message = "You cannot enter multiple collections for Insert operation. Please correct the configuration"
            raise ValidationException(self.message)     
                    
        if isinstance(self.collection_name, list): 
            self.message = "Corrupted Collection Name for Database for Insert operation. Please correct the configuration"
            raise ValidationException(self.message)            
           

    def _validate_thread_limit(self):
        """ validate and assign collection details input by user """

        mylogger.info(friday_reusable.get_function_name())

        self.thread_limit = self.mongo_config.get("Thread_Limit", None)

        if self.thread_limit is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Thread Limit")
            raise ValidationException(self.message)
    

    def _validate_dump_in_json(self):
        """ validate and assign collection details input by user """

        mylogger.info(friday_reusable.get_function_name())

        self.get_dump_in_json = self.mongo_config.get("Get_Dump_In_JSON", None)

        if self.get_dump_in_json is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Get JSON Dump")
            raise ValidationException(self.message)
        
    
    def _validate_canonical_mode(self):
        """ validate canonical mode """

        mylogger.info(friday_reusable.get_function_name())

        self.get_dump_in_canonical = self.mongo_config.get("Canonical_Mode", None)

        if self.get_dump_in_canonical is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Canonical Mode")
            raise ValidationException(self.message)        


    def _validate_json_arraylist(self):
        """ validate array list mode """

        mylogger.info(friday_reusable.get_function_name())

        self.get_dump_in_arraylist = self.mongo_config.get("JSON_In_Array", None)

        if self.get_dump_in_arraylist is None:
            self.message = MESSAGE_LOOKUP.get(12) %("JSON Array")
            raise ValidationException(self.message)
        
    
    def _validate_input_load_files(self):
        """ validate input load files """

        mylogger.info(friday_reusable.get_function_name())

        self.input_location = self.mongo_config.get("Input_Location", None)
        self.input_files = self.mongo_config.get("Input_Files", None)
        self.input_file_code_page = self.mongo_config.get("Input_File_Code_Page", None)

        # Check other keys present for horizontal cut
        if self.input_location is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Input Location")
            raise ValidationException(self.message)

        # check if base location is present
        validInd, self.message = friday_reusable.validate_folder_location(self.input_location)
        if not validInd:
            raise ValidationException(self.message)

        # Check if selective input flag key is present
        if self.input_files is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Input Files")
            raise ValidationException(self.message)
        
        # check if input is empty or has some file 
        validInd, self.message  = friday_reusable.validate_file_location(self.input_location, 
                                                                         self.input_files)
        if not validInd:
            raise ValidationException(self.message)
        
        # check if input Location key is present
        if self.input_file_code_page is None:
            self.message = MESSAGE_LOOKUP.get(12) %("Input File Codepage")
            raise ValidationException(self.message)
        
    
    def _validate_generate_ddl_flag(self):
        """ validate generate script flag """

        mylogger.info(friday_reusable.get_function_name())

        self.generate_scripts = self.mongo_config.get("Generate_Scripts", None)

        if self.generate_scripts is None: 
            self.message = MESSAGE_LOOKUP.get(12) %("Generate DDL")
            raise ValidationException(self.message)            


    def _validate_generate_stats(self):
        """ validate generate stats flag """

        mylogger.info(friday_reusable.get_function_name())

        self.generate_statistics = self.mongo_config.get("Generate_Statistics", None)

        if self.generate_statistics is None: 
            self.message = MESSAGE_LOOKUP.get(12) %("Generate Statistics")
            raise ValidationException(self.message)    


    def _validate_bson_data_import(self):
        """ validate bson import flag """

        mylogger.info(friday_reusable.get_function_name())

        self.bson_import = self.mongo_config.get("Restore_BSON", False)     

        if self.bson_import:
            file_filter = self.input_files
            if "*" in self.input_files:
                file_filter = ["*" , ]

            for filter_name in file_filter:
                file_list = fnmatch.filter(os.listdir(self.input_location), filter_name)

                file_ext_list = [os.path.splitext(file_name)[1] for file_name in file_list]
                if ".json" in file_ext_list:
                    self.message = "You cannot restore JSON files with Restore BSON option. Plesae correct the configuration"
                    raise ValidationException(self.message)


    def _create_mongo_subfolders(self, folder_name="Log"):
        """ create log file subfolder. assign name of the subfolder you would like to create within the workspace """

        mylogger.info(friday_reusable.get_function_name())
        
        location = friday_reusable.create_subfolder(self.output_location, 
                                                    self.mypath, 
                                                    folder_name, 
                                                    rename_existing=True)
        return location 
    

    @staticmethod
    def _validate_collection_details(server:str="", port:int=27017, user_name:str="", password:str="", database="", auth_src:str="admin", tls_cert:str="", tls_flag:bool=True, connect_db:bool=True, collection_list:list=[], generate_stats:bool=False, generate_script:bool=False, output_location:str="", output_codepage:str="utf-8", output_delimiter:str=","):
        """ validate list of collections entered by the user """

        mylogger = logging.getLogger(__name__)
        mylogger.info(friday_reusable.get_function_name())

        # define two empty lists
        invalid_collection_list = [] 
        valid_collection_list = []

        # create log file and statistics file 
        log_file_name = database + "_collection_statistics.log"
        log_file = open(os.path.join(output_location, log_file_name), "w")
        stats_csv = None 
        script_file = None 

        # get statistics file handle
        if generate_stats:
            stats_file_name = database + "_collection_statistics.csv"
            stats_file = open(os.path.join(output_location, stats_file_name), "w", newline="", encoding=output_codepage)
            stats_csv = csv.writer(stats_file, delimiter=output_delimiter)

            # write header record 
            header = ["Collection_Name", 
                      "Number_of_Documents", 
                      "Average_Document_Size", 
                      "Number_of_Indexes", 
                      "Total_Index_Size", 
                      "Remarks"]
            stats_csv.writerow(header)
       
        # get script file handle 
        if generate_script:
            script_file_name = database + "_collection_script.txt"
            script_file = open(os.path.join(output_location, script_file_name), "w")
            script_file.write("use " + database + "\n\n")
    
        # connect to db and start processing each collection one by one 
        try:
            mongo_connection_string = f"mongodb://{server}:{str(port)}/{database}?authSource={auth_src}"
            conn = MongoClient(mongo_connection_string, 
                               tls=tls_flag,
                               tlsCAFile=tls_cert,
                               username=user_name,
                               password=password,
                               connect=connect_db)
            db_conn = conn[database]

            # get list of collections from the db 
            valid_collection_list = db_conn.list_collection_names()

            # check if collection list empty or has "*" then pick all collections 
            if "*" in collection_list or len(collection_list) == 0 or collection_list is None:
                collection_list = valid_collection_list
            else:
                for col in collection_list:
                    if col not in valid_collection_list:
                        invalid_collection_list.append(col)
                        if generate_stats:
                            stats_csv.writerow([col, "", "", "", "", f"Collection Not in {database} database"])

            # check if invalid collections entered by the user
            if invalid_collection_list:
                collection_list = list(set(collection_list) ^ set(invalid_collection_list))

            # check if number of elements in collection list is reduced to zero 
            if len(collection_list) == 0:
                message = f"Collections Entered are not found in {database}. Please validate"
                log_file.write(message + "\n")
                raise ProcessingException(message)
            
            # iterate sover each application and collect the stats 
            for col in collection_list:
                db_stats = {} 

                try:
                    db_stats = db_conn.command("collstats", col)
                except Exception as cle:
                    mylogger.error(cle)
                    message = "DB Version is not supported for collstats command. Only document and index count will be retrieved. Validate other parameters manually"
                    log_file.write(message + "\n")
                    db_stats["count"] = db_conn[col].count_documents({})
                    db_stats["avgObjSize"] = ""
                    db_stats["nindexes"] = len(db_conn[col].index_information().keys())
                    db_stats["totalIndexSize"] = ""
                
                # check if generate summary 
                if generate_stats:
                    stats_csv.writerow([col, 
                                        db_stats.get('count',""), 
                                        db_stats.get('avgObjSize',""), 
                                        db_stats.get('nindexes',""), 
                                        db_stats.get('totalIndexSize',""), ""])

                # check if generate scripts 
                if generate_script:
                    try:
                        create_collection = 'db.createCollection("' + str(col) + '")\n'
                        index_info = db_conn[col].index_information()

                        script_file.write(create_collection)
                        for key in list(index_info.keys()):
                            create_index = {} 
                            if key != "_id_":
                                total_index = index_info.get(key)
                                create_index = dict(total_index.pop('key'))

                                if 'ns' in total_index:
                                    total_index.pop('ns')

                                if 'weights' in total_index:
                                    doc = total_index.pop('weights')
                                    total_index['weights'] = dict(doc.items())
                                total_index["name"] = key

                                if len(create_index) != 0:
                                    create_index_statement = 'db.getCollection("' + col + '").'  + "createIndex(" + str(create_index) + "," + str(total_index) + ")\n"
                                    create_index_statement = create_index_statement.replace(": True,", ": true,")
                                    create_index_statement = create_index_statement.replace(": False,", ": false,")
                                    script_file.write(create_index_statement)                            

                    except Exception as exc:
                        mylogger.error(exc)
                        log_file.write(str(exc) + '\n')
                        message = 'Error in creating script file. Please validate and execute manually' 
                        script_file.write(message + '\n')                        

        except Exception as e:
            mylogger.error(e)
            log_file.write(str(e) + '\n')
            raise ProcessingException(e)            

        # close files and db connections
        conn.close()
        log_file.close()
        stats_file.close() if generate_stats else None 
        script_file.close() if generate_script else None

        return collection_list
    

    @staticmethod
    def _get_bson_data_dump(server:str="", port:int=27017, user_name:str="", password:str="", database:str="", auth_src:str="", tls_ca_file:str="", collection:str="", output_codepage:str="utf-8", log_location:str="", dump_location:str="", mongo_dump_utility:str="mongodump.exe") -> None:
        """ get data in bson format """

        get_context().process = collection

        # create dump log file 
        file_name = collection + ".log"
        log_file = open(os.path.join(log_location, file_name), 'w', encoding=output_codepage)
        log_file.write(f"Processid: {str(os.getpid())} \tCollection: {collection} \tStart Time: {str(datetime.now())}\n" )
        
        # create dump command
        mongo_connection_string = f"mongodb://{user_name}:{password}@{server}:{str(port)}/{database}?authSource={auth_src}"
        dump_command = f"\"{mongo_dump_utility}\" --uri=\"{mongo_connection_string}\" --ssl --sslCAFile=\"{tls_ca_file}\" " \
                        f" --out=\"{dump_location}\" "

        command = f"{dump_command} --collection={collection}"

        log_file.write(f"Processid: {str(os.getpid())} \tCollection: {collection} \tEnd Time: {str(datetime.now())}\n" )

        # STart command subprocess 
        command_list = shlex.split(command)
        process = subprocess.Popen(command_list, shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        # write logs to dump log file
        log_file.write(stdout.decode("utf-8"))
        log_file.write(stderr.decode("utf-8"))


    @staticmethod
    def _get_json_data_dump(server:str="", port:int=27017, user_name:str="", password:str="", database:str="", auth_src:str="", tls_ca_file:str="", collection:str="", output_file_code_page:str="utf-8", log_location:str="", dump_location:str="", mongo_export_utility="mongoexport.exe", index:int=0, batch_size:int=0, skip:int=0, get_dump_in_canonical:bool=False, get_dump_in_arraylist:bool=True) -> None:
        """ perform data extract in json mode """

        get_context().process = skip

        # create dump log file 
        file_name = collection + '_' + str(index) + ".log"
        log_file = open(os.path.join(log_location, file_name), 'w', encoding=output_file_code_page)
        log_file.write(f"Processid: {str(os.getpid())} \tCollection: {collection} \tStart Time: {str(datetime.now())}\n" )

        output_json_file = os.path.join(dump_location, collection + '_' + str(index) + '.json')
        sort_statement = {"_id" : 1}

        # create mongo export comnmand 
        mongo_connection_string = f"mongodb://{user_name}:{password}@{server}:{str(port)}/{database}?authSource={auth_src}"
        if tls_ca_file != "":
            dump_command = f"\"{mongo_export_utility}\" --uri=\"{mongo_connection_string}\" --ssl --sslCAFile=\"{tls_ca_file}\" " \
                            f" --out=\"{output_json_file}\"  --sort=\"{sort_statement}\"  --limit={batch_size} --skip={skip}  "
        else:
            dump_command = f"\"{mongo_export_utility}\" --uri=\"{mongo_connection_string}\" " \
                            f" --out=\"{output_json_file}\"  --sort=\"{sort_statement}\"  --limit={batch_size} --skip={skip}  "            
        
        # add mode and array list if needed
        if get_dump_in_canonical:
            dump_command = f"{dump_command} --jsonFormat=canonical"
        
        if get_dump_in_arraylist:
            dump_command = f"{dump_command} --jsonArray"

        command = f"{dump_command} --collection={collection}"

        # STart command subprocess 
        command_list = shlex.split(command)
        process = subprocess.Popen(command_list, shell=True,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        # write logs to dump log file
        log_file.write(stdout.decode("utf-8"))
        log_file.write(stderr.decode("utf-8"))

        # print process id and elapsed time 
        log_file.write(f"Processid: {str(os.getpid())} \tCollection: {collection} \tEnd Time: {str(datetime.now())}\n" )
        log_file.close()


    @staticmethod
    def _import_json_data(input_file:str="", server:str="", port:int=27017, user_name:str="", password:str="", database:str="", auth_src:str="", tls_ca_file:str="", collection:str="", output_codepage:str="utf-8", output_location:str="", mongo_import_utility:str="mongoimport.exe") -> None:
        """ Perform data import to collection """

        get_context().process = input_file

        # create mongo connection url 
        mongo_connection_string = f"mongodb://{user_name}:{password}@{server}:{str(port)}/{database}?authSource={auth_src}"

        # create log filename 
        file_basename, _ = os.path.splitext(os.path.basename(input_file))
        log_file = open(os.path.join(output_location, file_basename + ".log"), "w", encoding=output_codepage)
        log_file.write(f"Processid: {str(os.getpid())} \tCollection: {collection} \tStart Time: {str(datetime.now())}\n" )

        # create import command 
        import_command = f"\"{mongo_import_utility}\" --uri=\"{mongo_connection_string}\" --type=json --ssl --sslCAFile=\"{tls_ca_file}\" " \
                        f" --file=\"{input_file}\"  --mode=insert  --jsonArray --numInsertionWorkers=10 "
        command = f"{import_command} --collection={collection}"

        command_list = shlex.split(command)
        process = subprocess.Popen(command_list, shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        # write logs to dump log file
        log_file.write(stdout.decode("utf-8"))
        log_file.write(stderr.decode("utf-8"))

        # print process id and elapsed time 
        log_file.write(f"Processid: {str(os.getpid())} \tCollection: {str(collection)} \tEnd Time: {str(datetime.now())}\n")
        log_file.close()


    @staticmethod
    def _restore_bson_data(input_file:str="", server:str="", port:int=27017, user_name:str="", password:str="", database:str="", auth_src:str="", tls_ca_file:str="", collection:str="", output_codepage:str="utf-8", output_location:str="", mongo_import_utility:str="mongorestore.exe") -> None:
        """ Perform BSON data import """

        get_context().process = input_file

        # create mongo connection url 
        mongo_connection_string = f"mongodb://{user_name}:{password}@{server}:{str(port)}/{database}?authSource={auth_src}"

        # create log filename 
        file_basename, _ = os.path.splitext(os.path.basename(input_file))
        log_file = open(os.path.join(output_location, file_basename + ".log"), "w", encoding=output_codepage)
        log_file.write(f"Processid: {str(os.getpid())} \tCollection: {collection} \tStart Time: {str(datetime.now())}\n" )

        # create import command 
        import_command = f"\"{mongo_import_utility}\" --uri=\"{mongo_connection_string}\"  --ssl --sslCAFile=\"{tls_ca_file}\" " \
                        f" --numInsertionWorkers=10 "
        command = f"{import_command} --collection={collection} --dir=\"{input_file}\" "

        command_list = shlex.split(command)
        process = subprocess.Popen(command_list, shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        # write logs to dump log file
        log_file.write(stdout.decode("utf-8"))
        log_file.write(stderr.decode("utf-8"))

        # print process id and elapsed time 
        log_file.write(f"Processid: {str(os.getpid())} \tCollection: {str(collection)} \tEnd Time: {str(datetime.now())}\n")
        log_file.close()


class MongoUtilities(MongoDbOperations):

    def __init__(self, config: dict = {}, root_option: str = "", sub_option: str = "", mypath: str = "", template_location: str = "", deloitte_image: str = "", travis_image: str = "", application_name: str = "", environment_name: str = "", run_id: int = 0, travis_status_queue: queue.Queue = None, treeview=None, app_config:dict={}, csv_first_option:str="CSV_COMPARE", csv_second_option:str="CSV_Dynamic_Compare") -> None:
        super().__init__(config, root_option, sub_option, mypath, template_location, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue, treeview)

        # load some additional variables passed
        self.app_config = app_config 
        self.csv_first_option = csv_first_option 
        self.csv_second_option = csv_second_option


    def perform_mongo_operations(self):
        """ perform various mongo operations """

        mylogger.info(friday_reusable.get_function_name())
        try:
            if self.sub_option == "Mongo_Data_Dump":
                self.perform_mongo_data_dump()
                self.message = MESSAGE_LOOKUP.get(14) %("Mongo Data Dump", self.output_location)

            elif self.sub_option == "Mongo_Data_Import":
                self.perform_mongo_data_import()
                self.message = MESSAGE_LOOKUP.get(14) %("Mongo Data Import", self.output_location)

            elif self.sub_option == "Mongo_Database_Statistics":
                self.perform_mongo_database_statistics()
                self.message = MESSAGE_LOOKUP.get(14) %("Mongo Database Statistics", self.output_location)

            elif self.sub_option == "Mongo_Source_Target_Statistics":
                self.perform_mongo_source_target_compare()
                self.message = MESSAGE_LOOKUP.get(14) %("Mongo Source Vs Target Compare", self.output_location)
            
            # create a status message 
            self.put_status_message_queue(status="Completed", message=self.message)

        except Exception as e:
            mylogger.critical(str(e))
            self.message = "Error Occured: " + str(e)
            self.put_status_message_queue(status="Error", message=self.message)
            raise ProcessingException(self.message)            


    def perform_mongo_data_dump(self) -> None:
        """ Dump data from Mongo database for given collections """

        mylogger.info(friday_reusable.get_function_name())
      

        # initialize all the variables needed for the execution for data dump 
        self._get_mongo_database_details()
        self._get_mongo_config_details()

        # validate and assign values to variables 
        self._validate_mongo_database_details()

        # validate mongo configurations 
        self._validate_collection_list()
        self._validate_dump_in_json()
        self._validate_canonical_mode()
        self._validate_json_arraylist()
        self._validate_thread_limit()
        self._validate_generate_ddl_flag()

        # validate output configurations 
        self._validate_output_config_details()
        
        # create different folders
        self.log_location = self._create_mongo_subfolders("Log")
        self.dump_location = self._create_mongo_subfolders("Dump")
        self.stats_location = self._create_mongo_subfolders("Stats")

        # validate if collections provided by the user is correct
        valid_collection_list = MongoDbOperations._validate_collection_details(server=self.host_name,
                                                                               port=self.host_port,
                                                                               user_name=self.user_id,
                                                                               password=self.password,                                                                               
                                                                               database=self.database,
                                                                               auth_src=self.auth_src,
                                                                               tls_cert=self.tls_ca_file,
                                                                               tls_flag=self.tls_flag,
                                                                               connect_db=True,
                                                                               collection_list=self.collection_list,
                                                                               generate_stats=True,
                                                                               generate_script=self.generate_scripts,
                                                                               output_location=self.stats_location,
                                                                               output_codepage=self.output_file_code_page,
                                                                               output_delimiter=self.output_file_delimiter)
        # if json dump not needed proceed with bson dump 
        self.prepare_for_json_dump() if self.get_dump_in_json else self.prepare_for_bson_dump(valid_collection_list)
            
        # merge all log files 
        friday_reusable.merge_multiple_temp_files(output_location=self.log_location, 
                                                  input_file_pattern="*.log",
                                                  output_file_name="Dump_Error.log",
                                                  first_record="")        


    def prepare_for_bson_dump(self, collection_list) -> None:
        """ Identify which utility to use for taking data dump """
        
        mylogger.info(friday_reusable.get_function_name())

        password = friday_reusable.replace_escape_character(self.password)

        # for bson document dump - simply run multiple processes and take collection dump 
        arg_list = [(self.host_name, 
                     self.host_port, 
                     self.user_id, 
                     password, 
                     self.database, 
                     self.auth_src,
                     self.tls_ca_file, 
                     collection, 
                     self.output_file_code_page, 
                     self.log_location, 
                     self.dump_location, 
                     self.mongo_dump_utility) for collection in collection_list]
        
        with multiprocessing.Pool(processes=self.thread_limit) as pool:
            pool.starmap(MongoDbOperations._get_bson_data_dump, arg_list)
    

    def prepare_for_json_dump(self) -> None:
        """ read stats file and split batches for parallel data extracts """

        mylogger.info(friday_reusable.get_function_name())      

        # read statistics collected and call mongo utilities to perform the dump process 
        stats_file_name = self.database + "_collection_statistics.csv"
        stats_file = open(os.path.join(self.stats_location, stats_file_name), "r", encoding=self.output_file_code_page, newline="")
        stats_csv = csv.reader(stats_file, delimiter=self.output_file_delimiter)
        _ = next(stats_csv)

        tasks = [] 

        # from the file get the collection name and total document count 
        for row in stats_csv:
            collection = row[0]
            total_documents = row[1]

            # Mongo Fix - Instead of running multiple threads for all enteries get data sequentially
            if int(total_documents) > 0:
                self.initiate_extract_thread(collection, total_documents)
        #         t = Thread(target=self.initiate_extract_thread, args=(collection, total_documents))
        #         t.start()
        #         tasks.append(t)

        # for t in tasks:
        #     t.join()

        # close the stats file 
        stats_file.close()


    def initiate_extract_thread(self, collection, total_documents):

        # create event loop 
        asyncio.run(self.extract_data_async(collection, total_documents))


    async def extract_data_async(self, collection, total_documents):
        
        loop = asyncio.get_running_loop()

        tasks = [] 
        
        # check if password contains escape characters 
        password = friday_reusable.replace_escape_character(self.password)

        # check if valid collection name 
        if collection != "" and collection is not None: 
            total_documents = int(total_documents)
            batch_size = total_documents // self.thread_limit

            for skip, index in zip(range(0, total_documents, batch_size), range(self.thread_limit + 1)):
                coro = asyncio.to_thread(MongoDbOperations._get_json_data_dump, self.host_name, 
                                         self.host_port,
                                         self.user_id, 
                                         password, 
                                         self.database,
                                         self.auth_src,
                                         self.tls_ca_file, 
                                         collection, 
                                         self.output_file_code_page, 
                                         self.log_location, 
                                         self.dump_location, 
                                         self.mongo_export_utility, 
                                         index, 
                                         batch_size, 
                                         skip, 
                                         self.get_dump_in_canonical, 
                                         self.get_dump_in_arraylist)
                task = asyncio.create_task(coro)
                tasks.append(task)

            # gather output 
            completed_tasks = await asyncio.gather(*tasks)


    def perform_mongo_data_import(self):
        """ start data import process """

        mylogger.info(friday_reusable.get_function_name())

        # initialize variables dictionary needed for the execution for data insert 
        self._get_mongo_database_details()
        self._get_mongo_config_details()

        # validate and assign values to variables 
        self._validate_mongo_database_details()
        self._validate_collection_name()

        # validate mongo details 
        self._validate_input_load_files()
        self._validate_thread_limit()
        self._validate_generate_stats()
        
        # validate output configurations 
        self._validate_output_config_details()
        self._validate_bson_data_import()

        # create different folders 
        self.log_location = self._create_mongo_subfolders("Log")
        self.stats_location = self._create_mongo_subfolders("Stats")

        # get all files from the location 
        file_dict = friday_reusable.get_all_files_dict(files=self.input_files,
                                                       file_location=self.input_location,
                                                       file_type="input")
        
        # create file list
        absolute_file_list = list(file_dict)

        self.perform_bson_import if self.bson_import else self.perform_json_array_import(absolute_file_list)


    def perform_bson_import(self, absolute_file_list):
        """ import bson data """
        mylogger.info(friday_reusable.get_function_name())

        password = friday_reusable.replace_escape_character(self.password)

        arg_list = [(input_file, 
                     self.host_name, 
                     self.host_port,
                     self.user_id,
                     password,
                     self.database,
                     self.auth_src,
                     self.tls_ca_file,
                     self.collection_name,
                     self.output_file_code_page,
                     self.log_location,
                     self.mongo_restore_utility) for input_file in absolute_file_list]
        
        with multiprocessing.Pool(processes=self.thread_limit) as pool:
            pool.starmap(MongoDbOperations._restore_bson_data, arg_list)

    def perform_json_array_import(self, absolute_file_list):
        """ initiate process to import json array list """

        mylogger.info(friday_reusable.get_function_name())

        file_list = friday_reusable.create_chunks(list_data=absolute_file_list, 
                                                  number_of_chunks=self.thread_limit)
        
        # create tasks sublist 
        tasks = []
        for file_sublist  in file_list:
            t = Thread(target=self.initiate_data_import, args=(file_sublist, ))
            t.start()
            tasks.append(t)

        for t in tasks:
            t.join()

        friday_reusable.merge_multiple_temp_files(output_location=self.log_location, 
                                                  input_file_pattern="*.log",
                                                  output_file_name="Import_Logs.log",
                                                  first_record="")
        
        if self.generate_statistics:
            self.collection_list = [] 
            self.collection_list.append(self.collection_name)

            # validate if collection provided by user is correct 
            _ = MongoDbOperations._validate_collection_details(server=self.host_name,
                                                               port=self.host_port,
                                                               user_name=self.user_id,
                                                               password=self.password,                                                                               
                                                               database=self.database,
                                                               auth_src=self.auth_src,
                                                               tls_cert=self.tls_ca_file,
                                                               tls_flag=self.tls_flag,
                                                               connect_db=True,
                                                               collection_list=self.collection_list,
                                                               generate_stats=True,
                                                               generate_script=False,
                                                               output_location=self.stats_location,
                                                               output_codepage=self.output_file_code_page,
                                                               output_delimiter=self.output_file_delimiter)
            
    def initiate_data_import(self, file_sublist):

        # create event loop 
        asyncio.run(self.import_json_array(file_sublist))

    
    async def import_json_array(self, file_sublist):

        tasks=[]

        password = friday_reusable.replace_escape_character(self.password)

        for input_file in file_sublist:
            coro = asyncio.to_thread(MongoDbOperations._import_json_data,
                                     input_file, 
                                     self.host_name, 
                                     self.host_port,
                                     self.user_id,
                                     password,
                                     self.database,
                                     self.auth_src,
                                     self.tls_ca_file,
                                     self.collection_name,
                                     self.output_file_code_page,
                                     self.log_location,
                                     self.mongo_import_utility)
            
            task = asyncio.create_task(coro)
            tasks.append(task)
        
        completed_tasks = await asyncio.gather(*tasks)


    def perform_mongo_data_restore(self):
        """ perform bson data load """
        pass


    def perform_mongo_database_statistics(self):
        """ gather mongo database statistics """

        mylogger.info(friday_reusable.get_function_name())

        # initialize variables dictionary needed for the execution for data insert 
        self._get_mongo_database_details()
        self._get_mongo_config_details()

        # validate and assign values to variables 
        self._validate_mongo_database_details()

        # validate mongo details 
        self._validate_collection_list()

        # if collection name is not mentioned, default to "*"
        if len(self.collection_list) <= 0:
            self.collection_list.append("*")
        
        self._validate_thread_limit()
        self._validate_generate_ddl_flag()

        _ = MongoDbOperations._validate_collection_details(server=self.host_name,
                                                           port=self.host_port,
                                                           user_name=self.user_id,
                                                           password=self.password,                                                                               
                                                           database=self.database,
                                                           auth_src=self.auth_src,
                                                           tls_cert=self.tls_ca_file,
                                                           tls_flag=self.tls_flag,
                                                           connect_db=True,
                                                           collection_list=self.collection_list,
                                                           generate_stats=True,
                                                           generate_script=self.generate_scripts,
                                                           output_location=self.output_location,
                                                           output_codepage=self.output_file_code_page,
                                                           output_delimiter=self.output_file_delimiter)
        

    def perform_mongo_source_target_compare(self):
        """ Compare source and target database """

        mylogger.info(friday_reusable.get_function_name())
       
        
        # initialize all the variables needed for the execution for data dump 
        self._get_base_details()
        self._get_release_details()
        self._get_mongo_config_details()

        # validate and assign values to variables 
        self._validate_base_details()
        self._validate_release_details()
        self._validate_output_config_details()
        self._validate_collection_list()
        self._validate_thread_limit()
        self._validate_generate_summary()

        # create base and release folders for stats 
        base_location = self._create_mongo_subfolders("Source")
        release_location = self._create_mongo_subfolders("Target")

        # validate collection list for source 
        base_collection_list = MongoDbOperations._validate_collection_details(server=self.base_host_name,
                                                                              port=self.base_host_port,
                                                                              user_name=self.base_user_id,
                                                                              password=self.base_password,                                                                               
                                                                              database=self.base_database,
                                                                              auth_src=self.base_auth_src,
                                                                              tls_cert=self.base_tls_ca_file,
                                                                              tls_flag=self.base_tls_flag,
                                                                              connect_db=True,
                                                                              collection_list=self.collection_list,
                                                                              generate_stats=True,
                                                                              generate_script=False,
                                                                              output_location=base_location,
                                                                              output_codepage=self.output_file_code_page,
                                                                              output_delimiter=self.output_file_delimiter)
        
        # validate collection list for target
        release_collection_list = MongoDbOperations._validate_collection_details(server=self.release_host_name,
                                                                                 port=self.release_host_port,
                                                                                 user_name=self.release_user_id,
                                                                                 password=self.release_password,                                                                               
                                                                                 database=self.release_database,
                                                                                 auth_src=self.release_auth_src,
                                                                                 tls_cert=self.release_tls_ca_file,
                                                                                 tls_flag=self.release_tls_flag,
                                                                                 connect_db=True,
                                                                                 collection_list=self.collection_list,
                                                                                 generate_stats=True,
                                                                                 generate_script=False,
                                                                                 output_location=release_location,
                                                                                 output_codepage=self.output_file_code_page,
                                                                                 output_delimiter=self.output_file_delimiter)        
        

        # set up details for csv
        if self.generate_summary:
            self.csv_compare_setup(base_location, release_location)


    def csv_compare_setup(self, base_location, release_location):
        """ create csv compare object and compare statistics """

        mylogger.info(friday_reusable.get_function_name())      

        # set up config for csv compare 
        csv_config = self.app_config[self.csv_first_option][self.csv_second_option]

        # set up base details 
        csv_config['BaseConfig']['Base_Location'] = base_location
        csv_config['BaseConfig']['Base_Files'] = ['*.csv', ]
        csv_config['BaseConfig']['Base_File_Delimiter'] = self.output_file_delimiter
        csv_config['BaseConfig']['Base_File_Code_Page'] = self.output_file_code_page

        # set up release details 
        csv_config['ReleaseConfig']['Release_Location'] = release_location
        csv_config['ReleaseConfig']['Release_Files'] = ['*.csv', ]
        csv_config['ReleaseConfig']['Release_File_Delimiter'] = self.output_file_delimiter
        csv_config['ReleaseConfig']['Release_File_Code_Page'] = self.output_file_code_page

        # compare configurations 
        csv_config['CompareConfig']['File_Keys'] = ['Collection_Name', ]
        csv_config['CompareConfig']['Skip_Fields'] = ['Remarks', ]
        csv_config['CompareConfig']['Case_Sensitive_Compare'] = True
        csv_config['CompareConfig']['Include_Matching_Records'] = True
        csv_config['CompareConfig']['Processor_Limit'] = 10        
        csv_config['CompareConfig']['Batch_Size'] = 100

        # output configurations 
        csv_config['OutputConfig']['Output_Location'] = self.output_location
        csv_config['OutputConfig']['Output_File_Delimiter'] = self.output_file_delimiter
        csv_config['OutputConfig']['Output_File_Code_Page'] = self.output_file_code_page
        csv_config['OutputConfig']['Output_Generate_Summary'] = self.generate_summary        

        # create compare object 
        csv_compare = CsvDynamicCompare(csv_config, 
                                        self.csv_first_option, 
                                        self.csv_second_option, 
                                        self.output_location, 
                                        self.template_location, 
                                        self.deloitte_image,
                                        self.travis_image,
                                        self.application_name, 
                                        self.environment_name,
                                        self.run_id,
                                        self.travis_status_queue,
                                        self.treeview,
                                        True,
                                        False)
        csv_compare.compare_csv_data()