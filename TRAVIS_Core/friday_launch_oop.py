''' 
    Created By: Rohit Abhishek 
    Function: This module is responsible to create GUI based on the input from yaml file. 
              yaml file will be provided along with this module which has GUI definition and input parameters. 

              This is main module to initiate the processing. It calls: - 
              a. friday_reusable.py: To create reusable directory for individual to work with data. By default it is C:\\Users\\<Userid>\\Documents
              b. friday_compare.py: To perform CSV and JSON Compares 
              c. friday_process.py: To perform various operations like, not limited to, CSV Manipulation, JSON Manipulation, Base64 Encryption, Mongo Extract & Load
              d. friday_exception.py: For validation and processing exceptions 
'''

import base64
import datetime
import io
import logging
import multiprocessing
import os
import queue
import re
import socket
import subprocess
import sys
import tkinter as tk
from threading import Thread
from tkinter import filedialog, messagebox

import friday_reusable
import psutil
import ttkbootstrap as tkb
from friday_compare import (CompareMetaData, CsvDynamicCompare,
                            CsvStreamCompare, JsonDynamicCompare,
                            JsonStreamCompare, PDFCompare)
from friday_config import FridayConfig, startup_process
from friday_constants import (CONFIG_FILE_NAME, FILE_TYPES, LOG_FILE_NAME,
                              MESSAGE_LOOKUP, SPECIAL_CHARACTERS,
                              STATIC_FOLDER_NAME, TEMPLATE_FOLDER_NAME,
                              TRAVIS1_TITLE)
from friday_exception import ProcessingException, ValidationException
from friday_process import (CsvManipulation, JsonManipulation,
                            MigrationUtilities, MongoUtilities,
                            TokenizeBase64Csv)
from PIL import Image, ImageTk
from ttkbootstrap.constants import *
from ttkbootstrap.scrolled import ScrolledFrame


class master(tkb.Window):
    _instance = None
    OPERATION_COUNTER = 1
    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        # workspace setting variables 
        self.__screen_total_width = self.winfo_screenwidth() 
        self.__screen_total_height = self.winfo_screenheight()
        self.__screen_height = None 
        self.__screen_width = None 
        self.__logging_level = None 
        self.__application_name = None 
        self.__environment_name = None 

        # set up gui variables 
        self.__travis_resizable = None 
        self.__travis_vertical_margin = None 
        self.__travis_horizontal_margin = None 

        # get image data 
        self.__travis_submit_button_bstream = None 
        self.__travis_submit_button_image = None 
        self.__travis_deloitte_full_bstream = None 
        self.__travis_bstream = None 
        self.__travis_image = None 
        self.__travis_image_resize = None 
        self.__travis_deloitte_dlogo_bstream = None 
        self.__travis_deloitte_dlogo_image = None 

        # configuration data 
        self.__mypath = None 
        self.__config_data = None 
        self.__gui_config = None 
        self.__image_config = None 
        self.__app_config = None 
        self.__style_name = None 
        self.__theme_names = None 

        # various sections on GUI 
        self.selection_section = None
        self.execution_notebook = None

        # selections made by the user 
        self.first_option_selected = None 
        self.run_dictionary = {} 
        self.__key_data = {}
       
        # directory locations 
        self.__static_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)),  STATIC_FOLDER_NAME)  
        self.__template_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), TEMPLATE_FOLDER_NAME)

        # override configuration file from the local folder if present else default it 
        if os.path.exists(os.path.join(os.path.dirname(sys.executable), CONFIG_FILE_NAME)):
            self.__config_file = os.path.join(os.path.dirname(sys.executable), CONFIG_FILE_NAME)
        else:
            self.__config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), CONFIG_FILE_NAME) 

        # special character regex 
        self.special_regex = re.compile(SPECIAL_CHARACTERS)

        # validate token and create user workspace 
        self.__mypath, self.__config_data, self.__travis_current_date, self.__travis_start_date, self.__travis_days_used, self.__travis_valid_days = FridayConfig.show(root=self,
                                                                                                                                                                       function=startup_process, 
                                                                                                                                                                       config_file=self.__config_file, 
                                                                                                                                                                       static_folder=self.__static_directory)
        # validate the returned data
        self.__validate_travis_return()

        # get configurations from yaml file 
        self.__gui_config = self.__config_data["TravisConfig"]["gui_config"]
        self.__image_config = self.__config_data["TravisConfig"]["image_config"]

        # remove the travis config from the yaml for app configurations 
        self.__app_config = self.__config_data.copy()
        self.__app_config.pop("TravisConfig")

        # create tkbootstrap style 
        self.__style_name                     = tkb.Style()
        self.__theme_names                    = self.__style_name.theme_names()
        self.__theme_names.sort()
        self.__style_name.theme_use("superhero")        

        # set up log handle  
        self.__set_up_log_handle()     

        # get the GUI configurations 
        self.__get_gui_configurations() 

        # set up TRAVIS GUI 
        self.__setup_travis_gui()

        # populate widgets on travis gui 
        self.__populate_travis_gui()


    def __set_up_log_handle(self) -> None:
        """ set up log handles """
        self.__logging_level = self.__gui_config.get("logging_level")
        
        # get the log level integer value 
        log_level = logging.NOTSET 
        if self.__logging_level == "INFO":
            log_level = logging.INFO
        elif self.__logging_level == "DEBUG":
            log_level = logging.DEBUG
        elif self.__logging_level == "WARN":
            log_level = logging.WARN            
        elif self.__logging_level  == "ERROR":
            log_level = logging.ERROR
        elif self.__logging_level == "CRITICAL":
            log_level = logging.CRITICAL
     
        # set up logging module 
        logging.basicConfig(filename=os.path.join(self.__mypath, LOG_FILE_NAME), 
                            filemode='w',
                            level=log_level, 
                            format=' %(asctime)s - {%(name)s : %(lineno)d} - %(levelname)s - %(message)s')


    def __get_gui_configurations(self) -> None: 
        """ get the configuration data from the yaml data """

        # populate data from the GUI Configurations 
        self.__application_name = self.__gui_config.get("application_name", "")
        self.__environment_name = self.__gui_config.get("environment_name", "")
        self.__travis_resizable = self.__gui_config.get("travis_resizable", False)
        self.__travis_vertical_margin = self.__gui_config.get("travis_vertical_margin", 150)
        self.__travis_horizontal_margin = self.__gui_config.get("travis_horizontal_margin", 0)

        # get image data 
        self.__travis_submit_button_bstream = self.__image_config.get("submit_button", None)
        self.__travis_deloitte_full_bstream = self.__image_config.get("deloitte_logo", None)
        self.__travis_bstream = self.__image_config.get("travis_logo", None)
        self.__travis_deloitte_dlogo_bstream = self.__image_config.get("deloitte_d_logo", None)

        # load all image data for consumptions 
        self.__travis_image = self.__load_travis_image()
        self.__travis_submit_button_image = ImageTk.PhotoImage(self.__load_submit_button_image())
        self.__travis_deloitte_dlogo_image = ImageTk.PhotoImage(self.__load_deloitte_dlogo_image())


    def __load_travis_image(self) -> Image: 
        """ Load travis image """
        travis_png_bytes                    = base64.b64decode(self.__travis_bstream.encode())
        travis_img_stream                   = io.BytesIO(travis_png_bytes)
        travis_pil                          = Image.open(travis_img_stream)

        return travis_pil


    def __load_submit_button_image(self) -> Image:
        """ Load submit button image """
        travis_submit_bytes                    = base64.b64decode(self.__travis_submit_button_bstream.encode())
        travis_submit_stream                   = io.BytesIO(travis_submit_bytes)
        travis_submit_pil                      = Image.open(travis_submit_stream)

        return travis_submit_pil 
    

    def __load_deloitte_dlogo_image(self) -> Image:
        """ Load submit button image """
        travis_dlogo_bytes                    = base64.b64decode(self.__travis_deloitte_dlogo_bstream.encode())
        travis_dlogo_stream                   = io.BytesIO(travis_dlogo_bytes)
        travis_dlogo_pil                      = Image.open(travis_dlogo_stream)

        return travis_dlogo_pil


    # implement singleton pattern 
    def __new__(cls, *args, **kwargs):
        """ return the object if already exists """

        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls)

        return cls._instance


    def __setup_travis_gui(self) -> 'None': 
        
        logging.info(friday_reusable.get_function_name())

        # set root container parameters gui parameters 
        self.__screen_width                   = self.__screen_total_width - self.__travis_horizontal_margin
        self.__screen_height                  = self.__screen_total_height - self.__travis_vertical_margin
        self.__travis_size = f"{self.__screen_width}x{self.__screen_height}+0+0"
        self.title(TRAVIS1_TITLE)
        self.geometry(self.__travis_size)
        self.resizable(self.__travis_resizable, self.__travis_resizable)
        self.config(borderwidth=2, relief='sunken')

        # set up icon for TRAVIS 
        self.__travis_image_resize              = self.__travis_image.resize((256,256))
        self.iconphoto(False, ImageTk.PhotoImage(self.__travis_image_resize))
        self.iconphoto(True, ImageTk.PhotoImage(self.__travis_image_resize))

    
    def __populate_travis_gui(self) -> None:
        """ Populate 4 sections on travis gui """

        # create main container frame on bootstrap window where all widgets will be populated 
        self.travis_frame                       = tkb.Frame(self, 
                                                            width=self.__screen_width,
                                                            height=self.__screen_height,
                                                            borderwidth=2)
        self.travis_frame.grid(row=0, column=0, sticky="nsew")
        self.travis_frame.grid_propagate(False)  

        # populate header section 
        self.__populate_header_section()

        # populate execution block 
        self.__populate_selection_section()

        # populate tree on the screen with current status of execution 
        self.__populate_progress_view()              
 
        # populate add menu bar
        self.__add_menu_bar()


    def __populate_header_section(self) -> None:
        """ Populate header section of travis window """

        logging.info(friday_reusable.get_function_name())

        # populate deloitte logo on header section 
        self.__populate_deloitte_logo_header() 

        # populate theme combobox menu 
        self.__populate_theme_select_header()

        self.__populate_travis_validity_header()

        # populate IP Address of the machine currently used for running travis 
        self.__populate_ip_address_header()

        # populate CPU meter
        self.__populate_cpu_count_header()

        # populate memory available meter
        self.__populate_available_memory_header()

        # populate number of concurrent users using the machine 
        self.__populate_logged_usercount_header()

        # set grid configurations for column of header frame (there are 10 columns but use configure till 8)
        self.travis_frame.grid_propagate(False)
        # self.travis_frame.grid_columnconfigure(0, weight=1, uniform='a')
        self.travis_frame.grid_columnconfigure(1, weight=1, uniform='a')
        self.travis_frame.grid_columnconfigure(2, weight=1, uniform='a')
        self.travis_frame.grid_columnconfigure(3, weight=1, uniform='a')
        self.travis_frame.grid_columnconfigure(4, weight=1, uniform='a')
        self.travis_frame.grid_columnconfigure(5, weight=1, uniform='a')
        self.travis_frame.grid_columnconfigure(6, weight=1, uniform='a')
        self.travis_frame.grid_columnconfigure(7, weight=1, uniform='a')
        # self.travis_frame.grid_columnconfigure(8, weight=1, uniform='a')

        separator = tkb.Separator(self.travis_frame)
        separator.grid(row=1, column=0, columnspan=11, padx=(10,10), pady=(10, 10), sticky="ew")        

    
    def __populate_deloitte_logo_header(self) -> None:
        """ Populate Deloitte logo """
        
        logging.info(friday_reusable.get_function_name())

        # Add Deloitte Logo in row 0
        logo_img_label                      = tkb.Label(self.travis_frame, 
                                                        image=self.__travis_deloitte_dlogo_image, 
                                                        text="Data Migration Studio", 
                                                        compound="left", 
                                                        underline=5,
                                                        font='-family Verdana -size 8')
        logo_img_label.grid(row=0, column=0, sticky="w", padx=10, pady=10) 

    
    def __populate_theme_select_header(self) -> None:
        """ Populate combobox on header frame with theme select """

        logging.info(friday_reusable.get_function_name())

        # create labels 
        self.theme_label                    = tkb.Label(self.travis_frame, 
                                                        text="Select a Theme: ")
        self.theme_label.grid(row=0, column=1, padx=10, sticky="e") 

        # create theme combo box
        self.theme_combo                    = tkb.Combobox(self.travis_frame, 
                                                           values=self.__theme_names)
        self.theme_combo.grid(row=0, column=2, padx=10, sticky="w")
        self.theme_combo.current(self.__theme_names.index(self.__style_name.theme.name))
        self.theme_combo.bind("<<ComboboxSelected>>", self.__change_travis_theme)
        self.theme_combo.configure(state="readonly")


    def __populate_travis_validity_header(self) -> None:
        """ Populate travis validity on header """

        logging.info(friday_reusable.get_function_name())

        # create an label box for travis validity
        self.valid_label                    = tkb.Label(self.travis_frame, 
                                                        text="Travis Validity: ")
        self.valid_label.grid(row=0, column=3, padx=10, sticky="e")

        # create rntry box for travis validity 
        start_date_formatted = datetime.datetime.fromtimestamp(float(self.__travis_start_date))
        valdity_end_date = (start_date_formatted+datetime.timedelta(days=float(self.__travis_valid_days))).strftime("%b %d, %Y")

        self.valid_date_string              = tkb.StringVar() 
        self.valid_date_string.set(valdity_end_date)

        self.valid_entry                       = tkb.Entry(self.travis_frame,
                                                        textvariable=self.valid_date_string,
                                                        justify=CENTER,
                                                        state=DISABLED)
        self.valid_entry.grid(row=0, column=4, padx=10, sticky="w")



    def __populate_ip_address_header(self) -> None:
        """ Populate ip address of this machine on the screen """

        logging.info(friday_reusable.get_function_name())

        # create an entry box with IP Address 
        self.ip_label                       = tkb.Label(self.travis_frame, 
                                                        text="IP Address: ")
        self.ip_label.grid(row=0, column=5, padx=10, sticky="e")

        # create ip value string 
        ip_address                          = socket.gethostbyname(socket.gethostname())
        self.ip_value                       = tkb.StringVar() 
        self.ip_value.set(ip_address)

        self.ip_entry                       = tkb.Entry(self.travis_frame,
                                                        textvariable=self.ip_value,
                                                        justify=CENTER,
                                                        state=DISABLED)
        self.ip_entry.grid(row=0, column=6, padx=10, sticky="w")


    def __populate_cpu_count_header(self) -> None:
        """ Populate Number of cores available on this machine"""

        logging.info(friday_reusable.get_function_name())

        # place meter widget for cpu count 
        cpu_count                           = os.cpu_count()
        self.cpu_meter                      = tkb.Meter(self.travis_frame, 
                                                        bootstyle="default", 
                                                        amounttotal=cpu_count, 
                                                        metersize=100, 
                                                        amountused=cpu_count,
                                                        textright="Logical",
                                                        subtext="processors",
                                                        textfont="-size 10 -weight bold",
                                                        subtextfont="-size 7",
                                                        interactive=False)                
        self.cpu_meter.grid(row=0, column=7, sticky="e", padx=10)


    def __populate_available_memory_header(self) -> None:
        """ Populate memory avaialble for execution on this machine """

        logging.info(friday_reusable.get_function_name())

        # place meter widget for memory count 
        memory_count                        = psutil.virtual_memory()
        self.memory_meter                   = tkb.Meter(self.travis_frame, 
                                                        bootstyle="success", 
                                                        amounttotal=(memory_count.total // (1024 ** 3)), 
                                                        metersize=100, 
                                                        amountused=(memory_count.used // (1024 ** 3)),
                                                        textright="gb",
                                                        subtext=f"memory in use",
                                                        textfont="-size 10 -weight bold",
                                                        subtextfont="-size 7",
                                                        interactive=False)            
        self.memory_meter.grid(row=0, column=8, sticky="e", padx=10)
        self.memory_meter.after(1000, self.__update_memory_meter)


    def __populate_logged_usercount_header(self) -> None:
        """ Populate number of concurrent users working on this machine """

        logging.info(friday_reusable.get_function_name())

        # place meter widget with number of active users 
        user_count                          = len(psutil.users())
        self.users_meter                    = tkb.Meter(self.travis_frame, 
                                                        bootstyle="danger",
                                                        amounttotal=user_count,
                                                        metersize=100,
                                                        amountused=user_count,
                                                        textright="user(s)",
                                                        subtext="logged in",
                                                        textfont="-size 10 -weight bold",
                                                        subtextfont="-size 7",
                                                        interactive=False)
        self.users_meter.grid(row=0, column=9, sticky="e", padx=10)
        self.users_meter.after(1000, self.__update_user_meter)


    def __show_folder(self, folder_location) -> None:
        """ Open Explorer window to show the current workspace """

        logging.info(friday_reusable.get_function_name())

        file_path = os.path.join(os.getenv('WINDIR',""), 'explorer.exe')

        # explorer would choke on forward slashes
        path = os.path.normpath(folder_location)

        if os.path.isdir(path):
            subprocess.run([file_path, path])

        elif os.path.isfile(path):
            subprocess.run([file_path, '/select,', os.path.normpath(path)])


    def __change_travis_theme(self, selection_event):
        """ Change GUI theme based on the selection made by the user """

        logging.info(friday_reusable.get_function_name())

        # read the theme selected 
        theme_name                          = self.theme_combo.get()
        self.__style_name.theme_use(theme_name)


    def __update_memory_meter(self):
        """ update memory meter after every 1 second """
        # logging.info(friday_reusable.get_function_name())

        memory_count                        = psutil.virtual_memory()
        self.memory_meter.configure(amounttotal=(memory_count.total // (1024 ** 3)),
                                    amountused=(memory_count.used // (1024 ** 3)))
        
        if not self.debugger_is_active():
            self.memory_meter.after(1000, self.__update_memory_meter)


    def __update_user_meter(self):
        """ update user count meter after every one second """
        # logging.info(friday_reusable.get_function_name())

        user_count                        = len(psutil.users())
        self.users_meter.configure(amounttotal=user_count,
                                   amountused=user_count)
        
        if not self.debugger_is_active():
            self.users_meter.after(1000, self.__update_user_meter)            


    def debugger_is_active(self):
        """ check if running in debugger """

        # logging.info(friday_reusable.get_function_name())
        return hasattr(sys, 'gettrace') and sys.gettrace() is not None

    
    def __populate_selection_section(self) -> None:
        """ Populate selection section """

        logging.info(friday_reusable.get_function_name())

        # create container block for selection section on the main container frame at row = 2
        self.selection_section                          = tkb.Labelframe(self.travis_frame, 
                                                                         text="Selection Block")        
        self.selection_section.grid(row=2, column=0, columnspan=11, padx=(10, 10), pady=(10, 10), sticky="ew")

        self.__populate_workspace_data_selection()

        # create operation label in selection pane 
        self.__populate_operation_names_selection() 

        # create applicaiton name panel in selection pane 
        self.__populate_application_name_selection() 

        # create environment name panel in selection panel 
        self.__populate_environment_name_selection()

        # place a separator line between the selection and execution block 
        separator = tkb.Separator(self.travis_frame)
        separator.grid(row=3, column=0, columnspan=11, padx=(10,10), pady=(10,10), sticky="ew")
         
        # set grid configurations for column of header frame
        self.selection_section.grid_columnconfigure(0, weight=1, uniform='a')
        self.selection_section.grid_columnconfigure(1, weight=1, uniform='a')
        self.selection_section.grid_columnconfigure(2, weight=1, uniform='a')
        self.selection_section.grid_columnconfigure(3, weight=1, uniform='a')
        self.selection_section.grid_columnconfigure(4, weight=1, uniform='a')
        self.selection_section.grid_columnconfigure(5, weight=1, uniform='a')
        self.selection_section.grid_columnconfigure(6, weight=1, uniform='a')
        self.selection_section.grid_columnconfigure(7, weight=1, uniform='a')
        self.selection_section.grid_columnconfigure(8, weight=1, uniform='a')        


    def __populate_workspace_data_selection(self) -> None:
        """ Populate workspace details """

        logging.info(friday_reusable.get_function_name())

        # add workspace labels and value 
        self.workspace_label                = tkb.Label(self.selection_section, 
                                                        text="Default Workspace: ")
        self.workspace_label.grid(row=0, column=0, padx=10, sticky="e")

        # create workspace value string 
        self.workspace_value                = tkb.StringVar() 
        self.workspace_value.set(self.__mypath)

        self.workspace_entry                = tkb.Entry(self.selection_section, 
                                                        textvariable=self.workspace_value,
                                                        state=DISABLED)
        self.workspace_entry.grid(row=0, column=1, padx=10, columnspan=2, sticky="ew")

        # create a show button for navigating to the workspace location 
        self.show_button                    = tkb.Button(self.selection_section, 
                                                         text="Show", 
                                                         command=lambda : self.__show_folder(self.__mypath))
        self.show_button.grid(row=0, column=2, padx=10, sticky="e")


    def __populate_operation_names_selection(self) -> None:
        """ Populate operation name combobox in selection section """
        
        logging.info(friday_reusable.get_function_name())

        # place selection combobox on Labelframe 
        self.operation_label                            = tkb.Label(self.selection_section, 
                                                                text="Operation Name: ")
        self.operation_label.grid(row=0, column=3, padx=(10,10), pady=(10, 10), sticky="e")

        # create a drop down with values from yaml file except for application, description etc 
        self.operation_combo                            = tkb.Combobox(self.selection_section, 
                                                                       bootstyle="primary",
                                                                       values=["Please Select..."] + list(self.__app_config))        
        self.operation_combo.grid(row=0, column=4, padx=(10,10), pady=(10, 10), sticky="w")
        self.operation_combo.current(0)
        self.operation_combo.bind("<<ComboboxSelected>>", self.__evaluate_first_option)
        self.operation_combo.configure(state="readonly")


    def __populate_application_name_selection(self) -> None:
        """ Populate application name in the selection section """

        logging.info(friday_reusable.get_function_name())

        # populate application label 
        application_label                   = tkb.Label(self.selection_section, 
                                                        text='Application: ')
        application_label.grid(row=0, column=5, sticky="e", padx=(10, 10), pady=(10, 10))

        # populate application entry box 
        self.application_name_text          = tkb.StringVar() 
        self.application_name_text.set(self.__application_name)        
        self.application_entry              = tkb.Entry(self.selection_section, 
                                                        textvariable=self.application_name_text)
        self.application_entry.grid(row=0, column=6, sticky="w", padx=(10, 10), pady=(10, 10))    
        self.application_entry.configure(state=ACTIVE)

        # set trace if there is any change in the text 
        self.application_name_text.trace("w", lambda name, index, mode, 
                                         application_name_text=self.application_name_text:self.__set_app_env_name(application_name_text, 
                                                                                                                 'application_name'))
        
    def __populate_environment_name_selection(self) -> None:
        """ Populate environment name on selection section """

        logging.info(friday_reusable.get_function_name())

        # populate environment label
        environment_label                   = tkb.Label(self.selection_section, 
                                                        text='Environment: ')
        environment_label.grid(row=0, column=7, sticky="e", padx=(10, 10), pady=(10, 10))

        # populate environment entry
        self.environment_name_text          = tkb.StringVar() 
        self.environment_name_text.set(self.__environment_name)
        self.environment_entry              = tkb.Entry(self.selection_section, 
                                                        textvariable=self.environment_name_text)
        self.environment_entry.grid(row=0, column=8, sticky="w", padx=(10, 10), pady=(10, 10))
        self.environment_entry.configure(state=ACTIVE)

        # set trace if there any change in environment text 
        self.environment_name_text.trace("w", lambda name, index, mode, 
                                         environment_name_text=self.environment_name_text:self.__set_app_env_name(environment_name_text, 
                                                                                                                 'environment_name'))


    def __set_app_env_name(self, text_value, text_name):
        """ Update application and environment names in the yaml file """

        # logging.info(friday_reusable.get_function_name())
        update_data                     = text_value.get()
        
        if update_data != "":
            self.__gui_config[text_name] = text_value.get()


    def __evaluate_first_option(self, selection_event) -> None:
        """ get the option selected by the user and create execution panel on travis """

        logging.info(friday_reusable.get_function_name())

        # set the first option selected 
        self.first_option_selected      = self.operation_combo.get() 

        # reset the GUI TODO - Execution notebook
        self.__reset_execution_panel()

        # create execution notebook with all options as tab name and data entered in each tab 
        self.execution_notebook             = tkb.Notebook(self.travis_frame, bootstyle="dark")
        self.execution_notebook.grid(row=6, column=0, columnspan=11, rowspan=2,padx=(10, 10), pady=(10,10), sticky="ew")

        # evaluate the option entered in the first option 
        if "please select" not in self.first_option_selected.lower():
            temp_config                     = (self.__app_config.get(self.first_option_selected)).copy()

            # now add new notebook frames with tab name as second option 
            for tab_name in list(temp_config):
                execution_frame = tkb.Frame(self.execution_notebook)
                self.execution_notebook.add(execution_frame, text=tab_name, sticky="nsew")
                self.__evaluate_second_option(tab_name, temp_config, execution_frame)


    def __reset_execution_panel(self) -> None:
        """ reset the GUI's execution panel  """
        logging.info(friday_reusable.get_function_name())

        self.second_option_selected     = None 
        
        # remove execution notebook from the screen if any 
        if self.execution_notebook is not None:
            self.execution_notebook.destroy()


    def __evaluate_second_option(self, second_option_selected, temp_config, execution_frame) -> None:
        """ get the second option and populate them as notebook tabs """

        logging.info(friday_reusable.get_function_name())

        # execution_frame_text       = temp_config[second_option_selected].get("description", None)

        # create canvas for scrollable execution frame 
        # execution_canvas           = tkb.Canvas(execution_frame)
        # execution_canvas.pack(side=LEFT, fill=BOTH, expand=YES)
        # execution_canvas.pack_propagate(False)

        # # create scrollbar on exection frame 
        # self.execution_scrollbar        = tkb.Scrollbar(execution_frame, 
        #                                            orient=VERTICAL, 
        #                                            bootstyle="round-primary")
        # self.execution_scrollbar.pack(side=RIGHT, fill=Y)            
        
        # # configure the scroll bar
        # self.execution_scrollbar.config(command=self.execution_canvas.yview)              

        # # configure the canvas
        # self.execution_canvas.configure(yscrollcommand=self.execution_scrollbar.set)

        # # bind the canvas to scrolling event 
        # self.execution_canvas.bind("<Configure>", lambda e : self.execution_canvas.configure(scrollregion=self.execution_canvas.bbox("all")))
        # self.execution_canvas.bind("<MouseWheel>", lambda e : self.execution_canvas.yview_scroll(int(-1 * (e.delta // 120)), "units"))

        # create another frame inside the canvas 
        # execution_section = ScrolledFrame(execution_canvas, autohide=True)
        # execution_canvas.create_window((0,0), window=execution_section.container, anchor="nw", width=self.__screen_width-30, height=self.__screen_height // 5)

        # execution section with scrollable frame, note this will not have 
        execution_section = ScrolledFrame(execution_frame, autohide=False, height=self.__screen_height // 4, bootstyle="round")
        execution_section.pack(side=LEFT, fill=BOTH, expand=YES)

        # self.self.execution_section.grid(row=4, column=0, columnspan=11, padx=(10, 10), pady=10, sticky="ew")
        # execution_section.grid_columnconfigure(0, weight=1, uniform="a")
        # execution_section.grid_columnconfigure(1, weight=1, uniform="a")

        # populate the widgets for execution section 
        self.__populate_execution_section(label_frame=None, 
                                          frame_row=1, 
                                          frame_column=1, 
                                          first_option_selected=self.first_option_selected,
                                          second_option_selected=second_option_selected,
                                          config_data=(self.__app_config[self.first_option_selected][second_option_selected]).copy(), 
                                          layout_counter=0, 
                                          parent_container=execution_section)
    
        # self.execution_canvas.configure(scrollregion=self.execution_canvas.bbox(ALL))
                

    def __populate_execution_section(self, label_frame, frame_row, frame_column, first_option_selected, second_option_selected, config_data, layout_counter=0, parent_container=None) -> None:
        logging.info(friday_reusable.get_function_name())

        r_value = 0 
        c_value = 0 

        base_weight = 10

        for key, value in config_data.items():

            # ignore anything which are already populated 
            if key in ('option_description', 'application', 'environment', 'description'):
                continue       

            # if value is dictionary then recursively call the same method for populating the label frame created
            if isinstance(value, dict):
                frame_column, frame_row = self.__get_layout_quadrant(layout_counter)
                label_frame = self.__populate_quadrant_execution(value, frame_column, frame_row, parent_container)

                # increment the layout counter
                layout_counter += 1

                # recursive call for populating key-value pair
                self.__populate_execution_section(label_frame, 
                                                  frame_row, 
                                                  frame_column, 
                                                  first_option_selected, 
                                                  second_option_selected, 
                                                  config_data[key], 
                                                  layout_counter, 
                                                  parent_container)

            elif isinstance(value, bool):
                self.__populate_boolean_data_execution(key, value, label_frame, r_value, c_value, first_option_selected, second_option_selected)

            # Display numeric data
            elif isinstance(value, int):
                self.__populate_int_data_execution(key, value, label_frame, r_value, c_value, first_option_selected, second_option_selected)

            # Display string data 
            elif isinstance(value, str):
                self.__populate_string_data_execution(key, value, label_frame, r_value, c_value, first_option_selected, second_option_selected)

            elif isinstance(value, list):
                self.__populate_list_data_execution(key, value, label_frame, r_value, c_value, first_option_selected, second_option_selected)

            label_frame.update() if label_frame else None
            r_value += 1

        self.submit_button              = tkb.Button(parent_container, 
                                                     image=self.__travis_submit_button_image, 
                                                     padding=0,
                                                     command=self.__process_request)
        
        self.submit_button.grid(row=5, column=0, sticky='sw', padx=(10, 10),pady=(10, 10))

        # self.execution_canvas.bind_all("<MouseWheel>", lambda e: self.execution_canvas.yview_scroll(int(-1 * (e.delta // 120)), "units"))
        

    def __get_layout_quadrant(self, layout_counter) -> None:
        """ get layout row and columns """

        # for quadrant = 0 
        if layout_counter       == 0: 
            return 0, 1
        elif layout_counter     == 1:
            return 1, 1
        elif layout_counter     == 2:
            return 0, 2
        elif layout_counter     == 3: 
            return 1, 2
        

    def __populate_quadrant_execution(self, value, frame_column, frame_row, parent_container) -> None:
        """ Populate label frame for each quadrant """

        # create labelframe container for dicitonary items 
        label_frame             = tkb.Labelframe(parent_container, 
                                                 text=value.get('description',""), 
                                                 relief='ridge')
        label_frame.grid(column=frame_column, row=frame_row, padx=(10, 10),pady=(10, 10), sticky='nsew')

        # set grid weight for label frame 
        label_frame.grid_columnconfigure(0, weight=3, uniform='a')
        label_frame.grid_columnconfigure(1, weight=10, uniform='a')
        label_frame.grid_columnconfigure(2, weight=1, uniform='a')

        return label_frame


    def __create_label_key_execution(self, key, label_frame, r_value, c_value) -> str:
        """ create label key for execution """

        # create label key 
        label_key               = key.replace("_", " ")
        label                   = tkb.Label(label_frame, 
                                            text=label_key.title())
        label.grid(row=r_value, column=c_value, padx=(5, 5), pady=(5, 5), sticky='ew')  
        
        # make grid propogate to false 
        label.grid_propagate(False)
        
        return label_key 


    def __populate_boolean_data_execution(self, key, value, label_frame, r_value, c_value, first_option_selected, second_option_selected) -> None:
        """ Populate boolean data """

        # create key label 
        _ = self.__create_label_key_execution(key, label_frame, r_value, c_value)


        # create string event change 
        text_string = tk.BooleanVar()
        text_string.trace('w', lambda name, index, mode, 
                          text_string=text_string : self.__update_yaml_data(text_string, first_option_selected, second_option_selected, name, key))
        text_string.set(bool(value))

        # create a toggle button 
        toggle_button = tkb.Checkbutton(label_frame,
                                        variable=text_string, 
                                        bootstyle="round-toggle")
        toggle_button.grid(row=r_value, column=c_value+1, padx=(5, 5), pady=(5, 5), sticky="w")
        toggle_button.grid_propagate(False)        

        # label_frame.update()

    def __populate_int_data_execution(self, key, value, label_frame, r_value, c_value, first_option_selected, second_option_selected) -> None:
        """ Populate numeric data """

        label_key = self.__create_label_key_execution(key, label_frame, r_value, c_value)

        # set numeric data to configuration
        text_string = tkb.IntVar()
        text_string.trace('w', lambda name, index, mode, 
                            text_string=text_string : self.__update_yaml_data(text_string, first_option_selected,second_option_selected, name, key))

        # add entry panel on GUI
        max_value               = value
        if "Processor" in label_key:
            max_value           = os.cpu_count()
        elif "Thread" in label_key:
            max_value           = 100 
        elif "Batch" in label_key:
            max_value           = 100000

        # set to max value 
        text_string.set(int(max_value))

        text_entry              = tkb.Spinbox(label_frame, 
                                              to=max_value,
                                              textvariable=text_string)
        text_entry.grid(row=r_value, 
                        column=c_value+1, 
                        sticky='w',
                        padx=(5, 5),
                        pady=(5, 5))
        text_entry.grid_propagate(False)

        # label_frame.update()


    def __populate_string_data_execution(self, key, value, label_frame, r_value, c_value, first_option_selected, second_option_selected) -> None:
        """ populate string data on the labelframe """

        label_key = self.__create_label_key_execution(key, label_frame, r_value, c_value)
        label_key_list = label_key.split()

        # set string data on the entry panel
        text_string = tk.StringVar()
        text_string.trace('w', lambda name, index, mode, 
                            text_string=text_string : self.__update_yaml_data(text_string, first_option_selected, second_option_selected, name, key))
        text_string.set(value)

        # check if password field and hide the informtion on screen
        if 'password' in label_key.lower():
            text_entry          = tkb.Entry(label_frame, 
                                            textvariable=text_string, 
                                            show='*')
            text_entry.grid(row=r_value, column=c_value + 1, sticky='ew', padx=(5, 5), pady=(5, 5))    
            text_entry.grid_propagate(False)

        elif 'location' in label_key.lower():
            text_entry          = tkb.Entry(label_frame, 
                                            textvariable=text_string, 
                                            state=DISABLED)
            text_entry.grid(row=r_value, column=c_value + 1, sticky='ew', padx=(5, 5), pady=(5, 5))
            
            button              = tkb.Button(label_frame, 
                                             text="Open",
                                             padding=0,
                                             command=lambda text_string=text_string, key=key : self.__open_directory_location(text_string, key, value, first_option_selected, second_option_selected))
            button.grid(row=r_value, column=c_value+2, sticky='nsew', padx=(5, 5), pady=(5, 5))                    
            text_entry.grid_propagate(False)

        elif label_key_list[-1].lower() == 'file':
            text_entry          = tkb.Entry(label_frame, 
                                            textvariable=text_string, 
                                            state=DISABLED)
            
            text_entry.grid(row=r_value, column=c_value+1, sticky='ew', padx=(5, 5), pady=(5, 5)) 
            
            button              = tkb.Button(label_frame, 
                                             text="Open",
                                             padding=0,
                                             command=lambda text_string=text_string, key=key : self.__open_file_location(text_string, key, first_option_selected, second_option_selected))
            button.grid(row=r_value, column=c_value+2, sticky='nsew', padx=(5, 5), pady=(5, 5))
            text_entry.grid_propagate(False)

        elif 'separator' in label_key.lower() or 'delimiter' in label_key.lower():
            text_entry          = tkb.Combobox(label_frame, 
                                               values=[",", "|", ".", "#", "$", "^", "~", "&", "*", "-", "+", "tab", "whitespace"], 
                                               textvariable=text_string)
            
            text_entry.grid(row=r_value, column=c_value+1, sticky='w', padx=(5, 5), pady=(5, 5)) 
            text_entry.grid_propagate(False)

        elif 'code page' in label_key.lower():
            text_entry          = tkb.Combobox(label_frame, 
                                               values=["ascii", "utf-8"], 
                                               textvariable=text_string)
            
            text_entry.grid(row=r_value, column=c_value+1, sticky='w', padx=(5, 5), pady=(5, 5)) 
            text_entry.grid_propagate(False)
            
        elif first_option_selected == "COMPARE_FILES" and 'file keys' in label_key.lower():
            text_entry          = tkb.Combobox(label_frame, 
                                               values=["File_Name", "File_Index"], 
                                               textvariable=text_string)
            
            text_entry.grid(row=r_value, column=c_value+1, sticky='w', padx=(5, 5), pady=(5, 5)) 
            text_entry.grid_propagate(False)

        elif first_option_selected == "BLAST_FURNACE" and 'Protector Type' in label_key.lower():
            text_entry          = tkb.Combobox(label_frame, 
                                               values=["App Protector", "REST Protector"], 
                                               textvariable=text_string)
            
            text_entry.grid(row=r_value, column=c_value+1, sticky='w', padx=(5, 5), pady=(5, 5)) 
            text_entry.grid_propagate(False)            


        else:
            text_entry          = tkb.Entry(label_frame, 
                                            textvariable=text_string)
            
            text_entry.grid(row=r_value, column=c_value+1, sticky='ew', padx=(5, 5), pady=(5, 5))                       
            text_entry.grid_propagate(False)
        
        # label_frame.update()
    
    def __populate_list_data_execution(self,  key, value, label_frame, r_value, c_value, first_option_selected, second_option_selected) -> None:
        """ populate string data on the labelframe """

        _ = self.__create_label_key_execution(key, label_frame, r_value, c_value)

        # set string data in entry panel
        text_string             = tk.StringVar()
        text_string.trace('w', lambda name, index, mode, 
                            text_string=text_string : self.__update_yaml_data(text_string, first_option_selected, second_option_selected, name, key))

        text_string.set(','.join(str(list_data).strip() for list_data in value))

        text_entry              = tkb.Entry(label_frame, 
                                            textvariable=text_string)
        text_entry.grid(row=r_value, column=c_value+1, sticky='ew', padx=(5 ,5), pady=(5, 5))
        text_entry.grid_propagate(False)        

        # label_frame.update()


    def __open_directory_location(self, text_string, key, value, first_option_selected, second_option_selected) -> 'None':
        """ Opens up file dialog for user to select the folder for input and output """

        logging.info(friday_reusable.get_function_name())

        # print (text_string, key, self.first_option_selected, self.second_option_selected)
        title = f"Select {key}"
        location = filedialog.askdirectory(initialdir=value, title=title)

        # open file dialog and set the value for location selected 
        for child_key, _ in self.__app_config[first_option_selected][second_option_selected].items():
            if key in self.__app_config[first_option_selected][second_option_selected][child_key]:
                self.__app_config[first_option_selected][second_option_selected][child_key][key] = location 

        text_string.set(location)


    def __open_file_location(self, text_string, key, first_option_selected, second_option_selected) -> 'None':
        """ Opens up file dialog for user to select the folder for input and output """

        logging.info(friday_reusable.get_function_name())

        # print (text_string, key, self.first_option_selected, self.second_option_selected)

        title = f"Select {key}"
        location = filedialog.askopenfilename(initialdir='./', title=title, filetypes=FILE_TYPES)

        # open file dialog and set the value for location selected 
        for child_key, _ in self.__app_config[first_option_selected][second_option_selected].items():
            if key in self.__app_config[first_option_selected][second_option_selected][child_key]:
                self.__app_config[first_option_selected][second_option_selected][child_key][key] = location 

        text_string.set(location)


    def __add_menu_bar(self) -> 'None':
        """ Add menu bar items to the root window """

        logging.info(friday_reusable.get_function_name())

        # create menaubar
        self.menubar                = tkb.Menu(self)

        self.file_menu              = tkb.Menu(self.menubar, tearoff=False)

        self.file_menu.add_command(label='Show Log', command=lambda: self.__show_log_file())
        self.file_menu.add_separator()
        self.file_menu.add_command(label='Reset GUI', command=self.__reset_gui)
        self.file_menu.add_command(label='Clean Workspace', command=lambda : friday_reusable.purge_workspace_folders(current_location=self.__mypath))
        self.file_menu.add_separator()
        self.file_menu.add_command(label='Exit', command=self.destroy)

        # add the File menu to the menubar
        self.menubar.add_cascade(label="File", menu=self.file_menu)

        # create the Help menu
        self.help_menu = tkb.Menu(self.menubar, tearoff=0)

        self.help_menu.add_command(label='About', command=lambda:self.__show_travis())

        # add the Help menu to the menubar
        self.menubar.add_cascade(label="Help", menu=self.help_menu)
        
        self.config(menu=self.menubar)


    def __reset_gui(self) -> None:
        """ reset the GUI's execution panel  """
        logging.info(friday_reusable.get_function_name())

        self.operation_combo.current(0)

        # remove execution notebook from the screen if any 
        if self.execution_notebook is not None:
            self.execution_notebook.destroy()


    def __show_travis(self) -> 'None':
        """ Show travis popup window with details """

        logging.info(friday_reusable.get_function_name())

        self.about              = tkb.Toplevel(self)
        # self.about.iconphoto(False, ImageTk.PhotoImage(self.travis_pil_resized))

        screen_width            = self.winfo_screenwidth()
        screen_height           = self.winfo_screenheight()

        self.about.geometry(f"350x200+{screen_width//2 - 350//2}+{screen_height//2 - 200//2}")        
        self.about.title("TRAVIS About")
        self.travis_logo        = ImageTk.PhotoImage(self.__travis_image)

        # place on labels 
        tkb.Label(self.about, image=self.travis_logo).pack()
        tkb.Label(self.about, text="By Deloitte Version 1.0").pack(pady=15)


    def __show_log_file(self) -> 'None':
        """ Open log files from Menu option """

        logging.info(friday_reusable.get_function_name())

        log = os.path.join(self.__mypath, 'Travis.log')
        show_notepad = 'notepad.exe %s' %(log)
        target = os.system(show_notepad)  


    def __populate_progress_view(self):
        
        columns = ("id", "option_selected", "process_type", "output_location", "status", "message")
        self.progress_treeview          = tkb.Treeview(self.travis_frame, columns=columns, show=HEADINGS, bootstyle="dark")

        # configure treeview columns 
        self.progress_treeview.column("id", width=int((self.__screen_width-40)*0.05))
        self.progress_treeview.column("option_selected", width=int((self.__screen_width-40)*0.15))
        self.progress_treeview.column("process_type", width=int((self.__screen_width-40)*0.15))
        self.progress_treeview.column("output_location", width=int((self.__screen_width-40)*0.20))
        self.progress_treeview.column("status", width=int((self.__screen_width-40)*0.10))
        self.progress_treeview.column("message", width=int((self.__screen_width-40)*0.35))

        # populate heading 
        self.progress_treeview.heading("id", text="Run Id", anchor=W)
        self.progress_treeview.heading("option_selected", text="Option Selected", anchor=W)
        self.progress_treeview.heading("process_type", text="Process Type", anchor=W)
        self.progress_treeview.heading("output_location", text="Output Location", anchor=W)
        self.progress_treeview.heading("status", text="Status", anchor=W)
        self.progress_treeview.heading("message", text="Message", anchor=W)

        # bind double click event to open the directory location 
        self.progress_treeview.bind("<Double-1>", self.__open_output_location)

        # bind the progress treeview with an event 
        self.travis_status_queue = queue.Queue()
        # self.progress_treeview.bind("<<MessageGenerated>>", lambda e : self.__process_message_queue(e))
        self.progress_treeview.after(100, self.__process_message_queue)
        self.progress_treeview.grid(row=4, column=0, columnspan=11, padx=(10, 10), pady=(10, 10), sticky="ew")

        # add a separator at the end of the treeview 
        separator = tkb.Separator(self.travis_frame)
        separator.grid(row=5, column=0, columnspan=11, padx=(10,10), pady=(10, 10), sticky="ew")      


    def __open_output_location(self, event):
        """ open the output location from the treeview """
        
        # grab the selected item 
        selected = self.progress_treeview.focus()

        # get the values from the tuple 
        tree_values = self.progress_treeview.item(selected, "values")

        if len(tree_values) > 0 and tree_values is not None: 
            # get the 4th element in the tuple 
            output_location = tree_values[3]

            # call show folder routine 
            self.__show_folder(output_location)
        

    # def __process_message_queue(self, event):
    def __process_message_queue(self):
        """ get the message from the child thread """
        
        # check the queue size and update the treeview 
        if self.travis_status_queue.qsize() != 0:
            message = self.travis_status_queue.get()

            # check if message length is more than 0 
            if len(message) > 0:
                run_id = message[0]

                # update tree view with details 
                self.progress_treeview.item(run_id, text="", values=message)
        
        self.progress_treeview.after(100, self.__process_message_queue)


    # Log error message and display
    def log_error_message(self, valid_indicator, message, run_id=None):
        '''LOG ERROR MESSAGE AND EXIT FROM THE APPLICATION'''
        
        logging.info(friday_reusable.get_function_name())
        logging.info('ERROR MESSAGE FOR: '+ str(valid_indicator) + ' MESSAGE PASSED '+ str(message))

        # get the item from the treeview 
        if run_id is not None: 
            current_values = self.progress_treeview.item(run_id, "values")
            new_values = (current_values[0], current_values[1], current_values[2], current_values[3], "Error", str(message))
            self.progress_treeview.item(run_id, text="", values=new_values)

        if not valid_indicator:
            logging.error(message)
            messagebox.showerror(TRAVIS1_TITLE, message)
            return
        

    def __validate_travis_return(self):
        ''' Validate the travis return '''

        travis_current_date = self.__travis_current_date
        travis_start_date = self.__travis_start_date
        travis_start_date_formatted = datetime.datetime.fromtimestamp(float(travis_start_date))
        travis_end_date = (travis_start_date_formatted+datetime.timedelta(days=float(self.__travis_valid_days))).timestamp()

        if self.__mypath is None or self.__mypath == "":
            messagebox.showerror(TRAVIS1_TITLE, MESSAGE_LOOKUP.get(16) %("workspace", "TRAVIS Support Team"))
            sys.exit()

        if self.__config_data is None or len(self.__config_data) == 0:
            messagebox.showerror(TRAVIS1_TITLE, MESSAGE_LOOKUP.get(16) %("Configuration Data", "TRAVIS Support Team"))
            sys.exit()

        if self.__travis_current_date is None:
            messagebox.showerror(TRAVIS1_TITLE, MESSAGE_LOOKUP.get(16) %("Current TRAVIS Date", "TRAVIS Support Team"))
            sys.exit()

        if self.__travis_start_date is None:
            messagebox.showerror(TRAVIS1_TITLE, MESSAGE_LOOKUP.get(16) %("TRAVIS Start Date", "TRAVIS Support Team"))
            sys.exit()

        if self.__travis_days_used is None:
            messagebox.showerror(TRAVIS1_TITLE, MESSAGE_LOOKUP.get(16) %("TRAVIS Days Used", "TRAVIS Support Team"))
            sys.exit()

        if self.__travis_valid_days is None:
            messagebox.showerror(TRAVIS1_TITLE, MESSAGE_LOOKUP.get(16) %("TRAVIS Validity", "TRAVIS Support Team"))
            sys.exit()

        if float(travis_current_date) > float(travis_end_date):
            messagebox.showerror(TRAVIS1_TITLE, MESSAGE_LOOKUP.get(5) %("TRAVIS Token", "TRAVIS Support Team"))
            sys.exit()

        if self.__travis_days_used > float(travis_start_date):
            messagebox.showerror(TRAVIS1_TITLE, MESSAGE_LOOKUP.get(6) %("TRAVIS Token", "TRAVIS Support Team"))
            sys.exit()                       
        

    # Log error message and display
    def log_exception_message(self, message, run_id=None):
        '''LOG ERROR MESSAGE AND EXIT FROM THE APPLICATION'''
        
        logging.info(friday_reusable.get_function_name())
        logging.critical('CRITICAL EXCEPTION ' + str(message))

        # get the item from the treeview 
        if run_id is not None: 
            current_values = self.progress_treeview.item(run_id, "values")
            new_values = (current_values[0], current_values[1], current_values[2], current_values[3], "Error", str(message))
            self.progress_treeview.item(run_id, text="", values=new_values)

        messagebox.showerror(TRAVIS1_TITLE, 'CRITICAL ERROR OCCURED. PLEASE CHECK LOG FILE')
        return


    def __process_request(self):
        """ Process request entered by the user on the current screen """
        
        logging.info(friday_reusable.get_function_name())

        # get current active notebook tab and first option
        first_option_selected = self.first_option_selected
        second_option_selected = self.execution_notebook.tab(self.execution_notebook.select(), "text")

        # get configurations for from the GUI 
        configuration, application_name, environment_name = self.__get_app_env_name(first_option_selected, 
                                                                                    second_option_selected)

        message = ""
        message = self.__validate_app_env_name(application_name, 
                                               environment_name, 
                                               second_option_selected)
        if message != "":
            self.log_error_message(False, message)
            return

        # create the run dictionary 
        run_id = master.OPERATION_COUNTER
        self.run_dictionary[run_id] = (run_id, first_option_selected, second_option_selected, "", "Starting", "")

        # update the value in the treeview 
        self.progress_treeview.insert(parent="", index=END, iid=master.OPERATION_COUNTER, values=self.run_dictionary[master.OPERATION_COUNTER])
        master.OPERATION_COUNTER += 1

        # Evaluate each request type and invoke separate thread
        try:
            if first_option_selected in ['COMPARE_FILES', ] and second_option_selected in ["Metadata_Compare"]:
                t = Thread(target=master.compare_file_metadata, args=(self,
                                                                      configuration,
                                                                      first_option_selected, 
                                                                      second_option_selected,
                                                                      self.__mypath,
                                                                      self.__template_directory,                                                                      
                                                                      self.__travis_deloitte_full_bstream,
                                                                      self.__travis_bstream,
                                                                      application_name,
                                                                      environment_name, 
                                                                      run_id, 
                                                                      self.travis_status_queue))
                t.start()

            elif first_option_selected in ['COMPARE_FILES', ] and second_option_selected in ["PDF_Compare"]:
                t = Thread(target=master.compare_pdf_file_data, args=(self,
                                                                      configuration,
                                                                      first_option_selected, 
                                                                      second_option_selected,
                                                                      self.__mypath,
                                                                      self.__template_directory,                                                                      
                                                                      self.__travis_deloitte_full_bstream,
                                                                      self.__travis_bstream,
                                                                      application_name,
                                                                      environment_name, 
                                                                      run_id, 
                                                                      self.travis_status_queue))
                t.start()                

            elif self.first_option_selected in ['JSON_COMPARE', ]:
                logging.info('stream_and_compare_json_files')
                t = Thread(target=master.stream_and_compare_json_files, args=(self,
                                                                              configuration,
                                                                              first_option_selected, 
                                                                              second_option_selected,
                                                                              self.__mypath,
                                                                              self.__template_directory,
                                                                              self.__travis_deloitte_full_bstream,
                                                                              self.__travis_bstream,
                                                                              application_name,
                                                                              environment_name, 
                                                                              run_id,
                                                                              self.travis_status_queue))
                t.start()

            elif self.first_option_selected in ['CSV_COMPARE', ]:
                logging.info('stream_and_compare_csv_files')
                t = Thread(target=master.stream_and_compare_csv_files, args=(self,
                                                                             configuration,
                                                                             first_option_selected, 
                                                                             second_option_selected,
                                                                             self.__mypath,
                                                                             self.__template_directory,
                                                                             self.__travis_deloitte_full_bstream,
                                                                             self.__travis_bstream,
                                                                             application_name,
                                                                             environment_name, 
                                                                             run_id,
                                                                             self.travis_status_queue))
                t.start()

            elif self.first_option_selected in ['CSV_TOKENIZATION', ]:
                logging.info('process_encryption_request')
                t = Thread(target=master.process_encryption_request, args=(self,
                                                                           configuration,
                                                                           first_option_selected, 
                                                                           second_option_selected,
                                                                           self.__mypath,
                                                                           self.__template_directory,
                                                                           self.__travis_deloitte_full_bstream,
                                                                           self.__travis_bstream,
                                                                           application_name,
                                                                           environment_name, 
                                                                           run_id,
                                                                           self.travis_status_queue))
                t.start()  

            elif self.first_option_selected in ['CSV_MANIPULATION', ]:
                logging.info('process_csv_manipulation')
                t = Thread(target=master.process_csv_manipulation, args=(self,
                                                                         configuration,
                                                                         first_option_selected, 
                                                                         second_option_selected,
                                                                         self.__mypath,
                                                                         self.__template_directory,
                                                                         self.__travis_deloitte_full_bstream,
                                                                         self.__travis_bstream,
                                                                         application_name,
                                                                         environment_name, 
                                                                         run_id,
                                                                         self.travis_status_queue))
                t.start()      

            elif self.first_option_selected in ['JSON_MANIPULATION', ]:
                logging.info('process_json_manipulation')
                t = Thread(target=master.process_json_manipulation, args=(self,
                                                                          configuration,
                                                                          first_option_selected, 
                                                                          second_option_selected,
                                                                          self.__mypath,
                                                                          self.__template_directory,
                                                                          self.__travis_deloitte_full_bstream,
                                                                          self.__travis_bstream,
                                                                          application_name,
                                                                          environment_name, 
                                                                          run_id,
                                                                          self.travis_status_queue))
                t.start()  

            elif self.first_option_selected in ['MONGO_UTILITIES', ]: 
                logging.info('process_mongo_request')
                t = Thread(target=master.process_mongo_request, args=(self,
                                                                      configuration,
                                                                      first_option_selected, 
                                                                      second_option_selected,
                                                                      self.__mypath,
                                                                      self.__template_directory,
                                                                      self.__travis_deloitte_full_bstream,
                                                                      self.__travis_bstream,
                                                                      application_name,
                                                                      environment_name, 
                                                                      run_id,
                                                                      self.travis_status_queue,
                                                                      self.__app_config))
                t.start()

            elif self.first_option_selected in ['MIGRATION_UTILITIES', ]: 
                logging.info('process_migration_request')
                t = Thread(target=master.process_migration_request, args=(self,
                                                                          configuration,
                                                                          first_option_selected, 
                                                                          second_option_selected,
                                                                          self.__mypath,
                                                                          self.__template_directory,
                                                                          self.__travis_deloitte_full_bstream,
                                                                          self.__travis_bstream,
                                                                          application_name,
                                                                          environment_name, 
                                                                          run_id,
                                                                          self.travis_status_queue))
                t.start()                

            else: 
                message = "Looks like TRAVIS option selected is not avaiable at this moment. Please contact TRAVIS Help for more information on: " + str(','.join([self.second_option_selected, self.second_option_selected]))
                logging.error(message)
                self.log_error_message(False, message)
                
        except Exception as e:
            logging.critical(e)
            message = 'CRITICAL ERROR. PLEASE CHECK THE LOG FILE'
            self.log_error_message(False, message)



    def __validate_app_env_name(self, application_name, environment_name, second_option_selected) -> str:

        # validate if application name and environment names are populated 
        if application_name is None or application_name == "":
            return "Application Name cannot be spaces"
        
        if self.special_regex.search(application_name) is not None : 
            return "Application Name cannot have special characters"        
        
        if environment_name is None or environment_name == "":
            return "Environment name cannot be empty"
        
        if self.special_regex.search(environment_name) is not None:
            return "Environment name cannot have special characters"
        
        return ""
        

    def __get_app_env_name(self, first_option_selected, second_option_selected) -> 'tuple[dict, str, str]':
        """ gets app and env name from the app configuration """
        
        logging.info(friday_reusable.get_function_name())

        configuration               = self.__app_config[first_option_selected][second_option_selected].copy()
        application_name            = self.__gui_config.get("application_name", "")
        environment_name            = self.__gui_config.get("environment_name", "")

        return configuration, application_name, environment_name


    @staticmethod
    def compare_file_metadata(root, configuration, first_option_selected, second_option_selected, mypath, template_directory, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue) -> 'None':
        """ Process JSON compare request """

        logging.info(friday_reusable.get_function_name())

        try:
            compare_file              = CompareMetaData(configuration, 
                                                        first_option_selected, 
                                                        second_option_selected, 
                                                        mypath,
                                                        template_directory,
                                                        deloitte_image, 
                                                        travis_image,
                                                        application_name, 
                                                        environment_name, 
                                                        run_id, 
                                                        travis_status_queue)
            compare_file.compare_files_metadata()

        except ValidationException as e: 
            master.log_error_message(root, False, e, run_id)

        except ProcessingException as e: 
            master.log_error_message(root, False, e, run_id)           

        except Exception as e: 
            master.log_exception_message(root, e, run_id)   


    @staticmethod
    def compare_pdf_file_data(root, configuration, first_option_selected, second_option_selected, mypath, template_directory, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue) -> 'None':
        """ Process JSON compare request """

        logging.info(friday_reusable.get_function_name())

        try:
            compare_file               = PDFCompare(configuration, 
                                                        first_option_selected, 
                                                        second_option_selected, 
                                                        mypath,
                                                        template_directory,
                                                        deloitte_image, 
                                                        travis_image,
                                                        application_name, 
                                                        environment_name, 
                                                        run_id, 
                                                        travis_status_queue)
            compare_file.compare_pdf_files()

        except ValidationException as e: 
            master.log_error_message(root, False, e, run_id)

        except ProcessingException as e: 
            master.log_error_message(root, False, e, run_id)           

        except Exception as e: 
            master.log_exception_message(root, e, run_id)               
        

    @staticmethod
    def stream_and_compare_json_files(root, configuration, first_option_selected, second_option_selected, mypath, template_directory, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue) -> 'None':
        """ Process JSON compare request """

        logging.info(friday_reusable.get_function_name())
        
        message = ""
        try: 
            # evaluate stem option and call json stream compare or load compare
            if second_option_selected == 'JSON_Stream_Compare':
                compare_json        = JsonStreamCompare(configuration, 
                                                        first_option_selected, 
                                                        second_option_selected, 
                                                        mypath,
                                                        template_directory,
                                                        deloitte_image, 
                                                        travis_image,
                                                        application_name, 
                                                        environment_name, 
                                                        run_id, 
                                                        travis_status_queue)
                compare_json.compare_json_streams()

            elif second_option_selected == 'JSON_Dynamic_Compare':
                compare_json        = JsonDynamicCompare(configuration, 
                                                         first_option_selected, 
                                                         second_option_selected, 
                                                         mypath,
                                                         template_directory,
                                                         deloitte_image, 
                                                         travis_image,
                                                         application_name, 
                                                         environment_name, 
                                                         run_id, 
                                                         travis_status_queue)
                
                compare_json.compare_json_data()

        except ValidationException as e: 
            master.log_error_message(root, False, e, run_id)

        except ProcessingException as e: 
            master.log_error_message(root, False, e, run_id)           

        except Exception as e: 
            master.log_exception_message(root, e, run_id)   
        
    
    @staticmethod
    def stream_and_compare_csv_files(root, configuration, first_option_selected, second_option_selected, mypath, template_directory, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue) -> 'None':
        """ Process CSV Compare request in multiprocessing enviornment """

        logging.info(friday_reusable.get_function_name())

        try: 
            message = ""
            # evaluate stem option and call json stream compare or load compare
            if second_option_selected == 'CSV_Stream_Compare':
                compare_json        = CsvStreamCompare(configuration, 
                                                       first_option_selected, 
                                                       second_option_selected, 
                                                       mypath,
                                                       template_directory,
                                                       deloitte_image, 
                                                       travis_image,
                                                       application_name, 
                                                       environment_name, 
                                                       run_id, 
                                                       travis_status_queue)
                compare_json.compare_csv_streams()
            elif second_option_selected == 'CSV_Dynamic_Compare':
                compare_json        = CsvDynamicCompare(configuration, 
                                                        first_option_selected, 
                                                        second_option_selected, 
                                                        mypath,
                                                        template_directory,
                                                        deloitte_image, 
                                                        travis_image,
                                                        application_name, 
                                                        environment_name, 
                                                        run_id, 
                                                        travis_status_queue)
                compare_json.compare_csv_data()

        except ValidationException as e: 
            master.log_error_message(root, False, e, run_id)

        except ProcessingException as e: 
            master.log_error_message(root, False, e, run_id)           

        except Exception as e: 
            master.log_exception_message(root, e, run_id)   
        

    @staticmethod
    def process_encryption_request(root, configuration, first_option_selected, second_option_selected, mypath, template_directory, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue) -> 'None':
        """ Process base 64 encryption request """
        
        logging.info(friday_reusable.get_function_name())

        try: 
            tokenize_csv_file       = TokenizeBase64Csv(configuration, 
                                                        first_option_selected, 
                                                        second_option_selected, 
                                                        mypath,
                                                        template_directory,
                                                        deloitte_image, 
                                                        travis_image,
                                                        application_name, 
                                                        environment_name, 
                                                        run_id, 
                                                        travis_status_queue,
                                                        root.progress_treeview)
            tokenize_csv_file.perform_csv_tokenization()   

        except ValidationException as e: 
            master.log_error_message(root, False, e, run_id)

        except ProcessingException as e: 
            master.log_error_message(root, False, e, run_id)           

        except Exception as e: 
            master.log_exception_message(root, e, run_id)        
        

    @staticmethod
    def process_csv_manipulation(root, configuration, first_option_selected, second_option_selected, mypath, template_directory, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue) -> 'None':
        """ Perform CSV Manipulations like split horizintally, vertically, conditionally and merge the files """

        logging.info(friday_reusable.get_function_name())

        try: 
            message = ""

            split_csv_file      = CsvManipulation(configuration, 
                                                  first_option_selected, 
                                                  second_option_selected, 
                                                  mypath,
                                                  template_directory,
                                                  deloitte_image, 
                                                  travis_image,
                                                  application_name, 
                                                  environment_name, 
                                                  run_id, 
                                                  travis_status_queue,
                                                  root.progress_treeview)
            
            split_csv_file.perform_csv_manipulation()         

        except ValidationException as e: 
            master.log_error_message(root, False, e, run_id)

        except ProcessingException as e: 
            master.log_error_message(root, False, e, run_id)           

        except Exception as e: 
            master.log_exception_message(root, e, run_id)   
        
   
    @staticmethod
    def process_json_manipulation(root, configuration, first_option_selected, second_option_selected, mypath, template_directory, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue) -> 'None':
        """ JSON data manipulations like convert to csv and merge various json files """
        
        logging.info(friday_reusable.get_function_name())

        try: 

            split_csv_file      = JsonManipulation(configuration, 
                                                   first_option_selected, 
                                                   second_option_selected, 
                                                   mypath,
                                                   template_directory,
                                                   deloitte_image, 
                                                   travis_image,
                                                   application_name, 
                                                   environment_name, 
                                                   run_id, 
                                                   travis_status_queue,
                                                   root.progress_treeview)
            split_csv_file.perform_json_manipulation()

        except ValidationException as e: 
            master.log_error_message(root, False, e, run_id)

        except ProcessingException as e: 
            master.log_error_message(root, False, e, run_id)           

        except Exception as e: 
            master.log_exception_message(root, e, run_id)   


    @staticmethod
    def process_mongo_request(root, configuration, first_option_selected, second_option_selected, mypath, template_directory, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue, app_config) -> 'None':
        """ Process Mongo migration request """
       
        try: 
            message = ""
            mongo_stats         = MongoUtilities(configuration, 
                                                 first_option_selected, 
                                                 second_option_selected, 
                                                 mypath,
                                                 template_directory,
                                                 deloitte_image, 
                                                 travis_image,
                                                 application_name, 
                                                 environment_name, 
                                                 run_id, 
                                                 travis_status_queue,
                                                 root.progress_treeview,
                                                 app_config,
                                                 'CSV_COMPARE', 
                                                 'CSV_Dynamic_Compare')
            
            mongo_stats.perform_mongo_operations()

        except ValidationException as e: 
            master.log_error_message(root, False, e, run_id)

        except ProcessingException as e: 
            master.log_error_message(root, False, e, run_id)           

        except Exception as e: 
            master.log_exception_message(root, e, run_id)   
        

    @staticmethod
    def process_migration_request(root, configuration, first_option_selected, second_option_selected, mypath, template_directory, deloitte_image, travis_image, application_name, environment_name, run_id, travis_status_queue) -> 'None':
        """ Process Migration request """ 

        try: 
            message = ""
            aws_object          = MigrationUtilities(configuration, 
                                                     first_option_selected, 
                                                     second_option_selected, 
                                                     mypath,
                                                     template_directory,
                                                     deloitte_image, 
                                                     travis_image,
                                                     application_name, 
                                                     environment_name, 
                                                     run_id, 
                                                     travis_status_queue,
                                                     root.progress_treeview)
            aws_object.perform_aws_operation()

        except ValidationException as e: 
            master.log_error_message(root, False, e, run_id)

        except ProcessingException as e: 
            master.log_error_message(root, False, e, run_id)           

        except Exception as e: 
            master.log_exception_message(root, e, run_id)   
        

    def __update_yaml_data(self, textObject, first_option, second_option, name, key) -> 'None':
        """ Update application configurations for final run """

        # evaluate if name present in keyData dictionary 
        if name not in self.__key_data.keys():
            self.__key_data[name]     = key

        key = self.__key_data.get(name)

        try:
            for child_node, _ in self.__app_config[first_option][second_option].items():

                if child_node == "description":
                    continue 

                if key in self.__app_config[first_option][second_option][child_node].keys():

                    # Check the instance of the input and populate accordingly.
                    if isinstance(self.__app_config[first_option][second_option][child_node][key], list):
                        list_data = textObject.get().split(',')
                        clean = []
                        for items in list_data:
                            items = items.strip()
                            clean.append(items)
                        self.__app_config[first_option][second_option][child_node][key] = clean

                    elif isinstance(self.__app_config[first_option][second_option][child_node][key], bool) and bool(textObject.get()):
                        self.__app_config[first_option][second_option][child_node][key] = bool(textObject.get())

                    elif isinstance(self.__app_config[first_option][second_option][child_node][key], int) and int(textObject.get()):
                        self.__app_config[first_option][second_option][child_node][key] = int(textObject.get())

                    else:
                        self.__app_config[first_option][second_option][child_node][key] = textObject.get()

        except Exception as e:
            logging.error(e)

        # print (self.__app_config[first_option][second_option])
            
if __name__ == '__main__' :
    multiprocessing.freeze_support()

    try: 
        self = master()
        self.mainloop()
    except Exception as e:
        print (e)