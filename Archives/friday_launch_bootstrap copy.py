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
import copy
import io
import logging
import math
import multiprocessing
import os
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
from friday_config import FridayConfig, startup_process
from friday_exception import ProcessingException, ValidationException
from friday_process import (CompareMetaData, CsvDynamicCompare,
                            CsvManipulation, CsvStreamCompare,
                            JsonDynamicCompare, JsonManipulation,
                            JsonStreamCompare, MigrationUtilities,
                            MongoUtilities, TokenizeBase64Csv)
from PIL import Image, ImageTk
from ttkbootstrap.constants import *


class master(tkb.Window):
    _instance = None
    CONFIG_FILE_NAME="FridayConfig_bootstrap.yaml"
    STATIC_DATA="static"
    TEMPLATE_DATA="templates"
    LOG_FILE_NAME="Travis.log"

    def __init__(self, *args, **kwargs):

        super().__init__(*args, **kwargs)

        # child widgets = 
        self.child_widget = []
        self.option_selected = []
        self.running_option = []
        self.second_drop_widget = [] 
        self.x_list = [] 
        self.y_list = []
        self.key_data = {}

        # some variables 
        self.special_regex                  = re.compile("[@!#$%^*()<>?/\|}{~:]")

        # get path and configurations 
        config_file_location                = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                                           master.CONFIG_FILE_NAME)
        
        # get the current workspace and load configurations 
        self.mypath, self.config_data       = friday_reusable.create_user_workspace(config_file_location)

        # put some animations 
        # _, _ = FridayConfig.show(root=self,
        #                          function=startup_process)

        # static directory  
        self.static_directory               = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                                           master.STATIC_DATA)

        # get log level 
        log_level                           = self.get_log_level()
        logging.basicConfig(filename=os.path.join(self.mypath, master.LOG_FILE_NAME), 
                            filemode='w',
                            level=log_level, 
                            format=' %(asctime)s - {%(name)s : %(lineno)d} - %(levelname)s - %(message)s')
        
        # get GUI Configuration for GUI set up 
        self.gui_config                     = self.config_data.get('FridayConfig')

        # get application configurations 
        self.app_config                     = self.config_data.copy()
        self.app_config.pop('FridayConfig')

        # set parameters from GUI setting in yaml file
        self.set_parameters()

        # setup gui for TRAVIS application
        self.setup_gui()

        # add menu items 
        self.add_menu_bar()        
    

    def get_log_level(self) -> int:
        """ Get log level from the configuration dataset """

        if self.config_data['FridayConfig']['workspace_setting']['logging_level'] == "INFO":
            return logging.INFO
        elif self.config_data['FridayConfig']['workspace_setting']['logging_level'] == "DEBUG":
            return logging.DEBUG
        elif self.config_data['FridayConfig']['workspace_setting']['logging_level'] == "WARN":
            return logging.WARN            
        elif self.config_data['FridayConfig']['workspace_setting']['logging_level'] == "ERROR":
            return logging.ERROR
        elif self.config_data['FridayConfig']['workspace_setting']['logging_level'] == "CRITICAL":
            return logging.CRITICAL
        else:
            return logging.NOTSET            


    def set_parameters(self) -> 'None':

        logging.info(friday_reusable.get_function_name())

        # get current resolution of user's screen 
        self.screen_total_width = self.winfo_screenwidth() 
        self.screen_total_height = self.winfo_screenheight()

        # GUI dimensions 
        self.vertical_margin                = self.gui_config['gui_settings'].get('vertical-margin')
        self.horizontal_margin              = self.gui_config['gui_settings'].get('horizontal-margin')
        self.screen_width                   = self.screen_total_width - self.horizontal_margin
        self.screen_height                  = self.screen_total_height - self.vertical_margin

        # get gui style and colors 
        self.travis_title                   = self.gui_config['gui_settings'].get('title')
        self.travis_size                    = f"{self.screen_width}x{self.screen_height}+0+0"
        self.travis_resizable               = self.gui_config['gui_settings'].get('resizable')

        # get base64 image data 
        self.travis_button                  = self.gui_config['image_settings'].get('submit_button')
        self.travis_org_logo                = self.gui_config['image_settings'].get('deloitte_logo')
        self.travis_org_d_logo              = self.gui_config['image_settings'].get('deloitte_d_logo')
        self.travis_icon                    = self.gui_config['image_settings'].get('travis_logo')
        self.open_button                    = self.gui_config['image_settings'].get('open_button')

   
    # implement singleton pattern 
    def __new__(cls, *args, **kwargs):
        """ return the object if already exists """

        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls)

        return cls._instance


    def setup_gui(self) -> 'None': 
        
        logging.info(friday_reusable.get_function_name())

        # set root container parameters gui parameters 
        self.title(self.travis_title)
        self.geometry(self.travis_size)
        self.resizable(self.travis_resizable, self.travis_resizable)
        self.config(borderwidth=2, relief='sunken')

        # create TRAVIS icon for GUI
        travis_png_bytes                    = base64.b64decode(self.travis_icon.encode())
        travis_img_stream                   = io.BytesIO(travis_png_bytes)
        travis_pil                          = Image.open(travis_img_stream)
        self.travis_pil_resized             = travis_pil.resize((256,256))
        self.iconphoto(False, ImageTk.PhotoImage(self.travis_pil_resized))
        self.iconphoto(True, ImageTk.PhotoImage(self.travis_pil_resized))

        # create single frame on bootstrap window 
        self.travis_frame                   = tkb.Frame(self, 
                                                        width=self.screen_width,
                                                        height=self.screen_height-100,
                                                        borderwidth=2)
        self.travis_frame.grid(row=0, column=0, sticky="nsew")
        self.travis_frame.grid_propagate(False)

        # create progress frame 
        self.progress_frame                 = tkb.Frame(self, 
                                                        width=self.screen_width,
                                                        height=100,
                                                        borderwidth=2, relief=SUNKEN)
        self.progress_frame.grid(row=1, column=0, sticky="s")
        self.progress_frame.grid_propagate(False)

        self.grid_propagate(False)
        self.rowconfigure(1, weight=1)
        self.rowconfigure(0, weight=10)

        # create open button 
        self.open_png_bytes                 = base64.b64decode(self.open_button.encode())
        self.open_img_stream                = io.BytesIO(self.open_png_bytes)
        self.open_img                       = ImageTk.PhotoImage(Image.open(self.open_img_stream))     

        # add submit button on submit frame 
        btn_png_bytes                       = base64.b64decode(self.travis_button.encode())
        btn_img                             = io.BytesIO(btn_png_bytes)
        self.btn_img                        = ImageTk.PhotoImage(Image.open(btn_img))           

        # populate header section 
        self.populate_header_section()

        # populate execution block 
        self.populate_selection_section()


    def populate_header_section(self) -> None:

        logging.info(friday_reusable.get_function_name())

        # Add Deloitte Logo in row 0
        org_png_bytes                       = base64.b64decode(self.travis_org_d_logo.encode())
        org_img                             = io.BytesIO(org_png_bytes)
        org_pil_img                         = Image.open(org_img)
        self.org_logo                       = ImageTk.PhotoImage(org_pil_img)   
        logo_img_label                      = tkb.Label(self.travis_frame, 
                                                        image=self.org_logo)
        logo_img_label.grid(row=0, column=0, sticky="w", padx=10, pady=10) 

        # add workspace labels and value 
        self.workspace_label                = tkb.Label(self.travis_frame, 
                                                        text="Default Workspace: ")
        self.workspace_label.grid(row=0, column=1, padx=10, sticky="e")

        # create workspace value string 
        self.workspace_value                = tkb.StringVar() 
        self.workspace_value.set(self.mypath)

        self.workspace_entry                = tkb.Entry(self.travis_frame, 
                                                        textvariable=self.workspace_value,
                                                        state=DISABLED)
        self.workspace_entry.grid(row=0, column=2, padx=10, columnspan=2, sticky="ew")

        # create a show button for navigating to the workspace location 
        self.show_button                    = tkb.Button(self.travis_frame, 
                                                         text="Show", 
                                                         command=self.show_workspace)
        self.show_button.grid(row=0, column=3, padx=10, sticky="e")

        # get style names for dynamic GUI 
        self.style_name                     = tkb.Style()
        self.theme_names                    = self.style_name.theme_names()
        self.style_name.theme_use("superhero")

        # create labels 
        self.theme_label                    = tkb.Label(self.travis_frame, 
                                                        text="Select a Theme: ")
        self.theme_label.grid(row=0, column=4, padx=10, sticky="e") 

        # create theme combo box
        self.theme_combo                    = tkb.Combobox(self.travis_frame, 
                                                           values=self.theme_names)
        self.theme_combo.grid(row=0, column=5, padx=10, sticky="w")
        self.theme_combo.current(self.theme_names.index(self.style_name.theme.name))
        self.theme_combo.bind("<<ComboboxSelected>>", self.change_travis_theme)
        self.theme_combo.configure(state="readonly")

        # create an entry box with IP Address 
        self.ip_label                       = tkb.Label(self.travis_frame, 
                                                        text="IP Address: ")
        self.ip_label.grid(row=0, column=6, padx=10, sticky="e")

        # create ip value string 
        ip_address                          = socket.gethostbyname(socket.gethostname())
        self.ip_value                       = tkb.StringVar() 
        self.ip_value.set(ip_address)

        self.ip_entry                       = tkb.Entry(self.travis_frame,
                                                        textvariable=self.ip_value,
                                                        justify=CENTER,
                                                        state=DISABLED)
        self.ip_entry.grid(row=0, column=7, padx=10, sticky="w")

        # place meter widget for cpu count 
        cpu_count                           = os.cpu_count()
        self.cpu_meter                      = tkb.Meter(self.travis_frame, 
                                                        bootstyle="default", 
                                                        amounttotal=cpu_count, 
                                                        metersize=100, 
                                                        amountused=cpu_count,
                                                        textright="Logical",
                                                        subtext="Processors",
                                                        textfont="-size 10 -weight bold",
                                                        subtextfont="-size 7",
                                                        interactive=False)                
        self.cpu_meter.grid(row=0, column=8, sticky="e", padx=10)

        # place meter widget for memory count 
        memory_count                        = psutil.virtual_memory()
        self.memory_meter                   = tkb.Meter(self.travis_frame, 
                                                        bootstyle="success", 
                                                        amounttotal=(memory_count.total // (1024 ** 3)), 
                                                        metersize=100, 
                                                        amountused=(memory_count.used // (1024 ** 3)),
                                                        textright="gb",
                                                        subtext=f"Memory in Use",
                                                        textfont="-size 10 -weight bold",
                                                        subtextfont="-size 7",
                                                        interactive=False)            
        self.memory_meter.grid(row=0, column=9, sticky="e", padx=10)
        self.memory_meter.after(1000, self.update_memory_meter)

        # place meter widget with number of active users 
        user_count                          = len(psutil.users())
        self.user_meter                     = tkb.Meter(self.travis_frame, 
                                                        bootstyle="danger",
                                                        amounttotal=user_count,
                                                        metersize=100,
                                                        amountused=user_count,
                                                        textright="user(s)",
                                                        subtext="Logged in",
                                                        textfont="-size 10 -weight bold",
                                                        subtextfont="-size 7",
                                                        interactive=False)
        self.user_meter.grid(row=0, column=10, sticky="e", padx=10)
        self.user_meter.after(1000, self.update_user_meter)

        # set grid configurations for column of header frame
        self.travis_frame.grid_propagate(False)
        self.travis_frame.grid_columnconfigure(0, weight=1, uniform='a')
        self.travis_frame.grid_columnconfigure(1, weight=1, uniform='a')
        self.travis_frame.grid_columnconfigure(2, weight=1, uniform='a')
        self.travis_frame.grid_columnconfigure(3, weight=1, uniform='a')
        self.travis_frame.grid_columnconfigure(4, weight=1, uniform='a')
        self.travis_frame.grid_columnconfigure(5, weight=1, uniform='a')
        self.travis_frame.grid_columnconfigure(6, weight=1, uniform='a')
        self.travis_frame.grid_columnconfigure(7, weight=1, uniform='a')
        self.travis_frame.grid_columnconfigure(8, weight=1, uniform='a')

        separator = tkb.Separator(self.travis_frame)
        separator.grid(row=1, column=0, columnspan=11, padx=(10,10), sticky="ew")
 
    def show_workspace(self) -> None:
        logging.info(friday_reusable.get_function_name())

        file_path = os.path.join(os.getenv('WINDIR',""), 'explorer.exe')

        # explorer would choke on forward slashes
        path = os.path.normpath(self.mypath)

        if os.path.isdir(path):
            subprocess.run([file_path, path])

        elif os.path.isfile(path):
            subprocess.run([file_path, '/select,', os.path.normpath(path)])


    def update_memory_meter(self):
        # logging.info(friday_reusable.get_function_name())

        memory_count                        = psutil.virtual_memory()
        self.memory_meter.configure(amounttotal=(memory_count.total // (1024 ** 3)),
                                    amountused=(memory_count.used // (1024 ** 3)))
        
        if not self.debugger_is_active():
            self.memory_meter.after(1000, self.update_memory_meter)

    def update_user_meter(self):
        # logging.info(friday_reusable.get_function_name())

        user_count                        = len(psutil.users())
        self.user_meter.configure(amounttotal=user_count,
                                    amountused=user_count)
        
        if not self.debugger_is_active():
            self.user_meter.after(1000, self.update_user_meter)            


    def debugger_is_active(self):
        # logging.info(friday_reusable.get_function_name())
        return hasattr(sys, 'gettrace') and sys.gettrace() is not None


    def change_travis_theme(self, selection_event):
        logging.info(friday_reusable.get_function_name())
        theme_name                          = self.theme_combo.get()
        self.style_name.theme_use(theme_name)

    
    def populate_selection_section(self) -> None:
        logging.info(friday_reusable.get_function_name())

        # create exection block in LabelFrame 
        self.selection_section = tkb.Labelframe(self.travis_frame, 
                                                text="Selection Block")
        self.selection_section.grid(row=3, column=0, columnspan=11, padx=(10, 10), pady=(10, 10), sticky="ew")

        # place a separator line between the selection and execution block 
        separator = tkb.Separator(self.travis_frame)
        separator.grid(row=4, column=0, columnspan=11, padx=(10,10), pady=(10,10), sticky="ew")        

        # create progress section at the right handside of the GUI 
        self.progress_section  = tkb.Frame(self.travis_frame)
        self.progress_section.grid(row=5, column=0, columnspan=11, padx=10, pady=10, sticky="s")

        

        # set the first and second level selection variables 
        self.first_option_selected          = "" 
        self.second_option_selected         = ""
        self.execution_section              = None

        # place selection combobox on Labelframe 
        self.operation_label                = tkb.Label(self.selection_section, 
                                                      text="Operation Name: ")
        self.operation_label.grid(row=0, column=0, padx=(10,10), pady=(10, 10), sticky="e")

        # create a drop down with values from yaml file except for application, description etc 
        self.operation_combo                = tkb.Combobox(self.selection_section, 
                                                           bootstyle="primary",
                                                           values=["Please Select..."] + list(self.app_config))        
        self.operation_combo.grid(row=0, column=1, padx=(10,10), pady=(10, 10), sticky="w")
        self.operation_combo.current(0)
        self.operation_combo.bind("<<ComboboxSelected>>", self.evaluate_first_option)
        self.operation_combo.configure(state="readonly")

        # populate second drop down label 
        self.sub_operation_label            = tkb.Label(self.selection_section, 
                                                        text="Sub-Operation Name: ")
        self.sub_operation_label.grid(row=0, column=2, padx=(10,10), pady=(10, 10), sticky="e")


        # populate contents of second drop down
        self.sub_operation_combo            = tkb.Combobox(self.selection_section, 
                                                           bootstyle="primary",
                                                           values=["",])        
        self.sub_operation_combo.grid(row=0, column=3, padx=(10,10), pady=(10, 10), sticky="w")
        self.sub_operation_combo.current(0)
        self.sub_operation_combo.bind("<<ComboboxSelected>>", self.evaluate_second_option)
        self.sub_operation_combo.configure(state=DISABLED)

        # populate application label 
        application_label                   = tkb.Label(self.selection_section, 
                                                        text='Application: ')
        application_label.grid(row=0, column=4, sticky="e", padx=(10, 10), pady=(10, 10))

        # populate application entry box 
        self.application_name_text          = tkb.StringVar() 
        self.application_name_text.set("")        
        self.application_entry              = tkb.Entry(self.selection_section, 
                                                        textvariable=self.application_name_text)
        self.application_entry.grid(row=0, column=5, sticky="w", padx=(10, 10), pady=(10, 10))    
        self.application_entry.configure(state=DISABLED)

        # set trace if there is any change in the text 
        self.application_name_text.trace("w", lambda name, index, mode, 
                                         application_name_text=self.application_name_text:self.set_app_env_name(application_name_text, 
                                                                                                                self.first_option_selected, 
                                                                                                                'Application'))
        # populate environment label
        environment_label                   = tkb.Label(self.selection_section, 
                                                        text='Environment: ')
        environment_label.grid(row=0, column=6, sticky="e", padx=(10, 10), pady=(10, 10))

        # populate environment entry
        self.environment_name_text          = tkb.StringVar() 
        self.environment_name_text.set("")
        self.environment_entry              = tkb.Entry(self.selection_section, 
                                                        textvariable=self.environment_name_text)
        self.environment_entry.grid(row=0, column=7, sticky="w", padx=(10, 10), pady=(10, 10))
        self.environment_entry.configure(state=DISABLED)

        # set trace if there any change in environment text 
        self.environment_name_text.trace("w", lambda name, index, mode, 
                                         environment_name_text=self.environment_name_text:self.set_app_env_name(environment_name_text, 
                                                                                                                self.first_option_selected, 
                                                                                                                'Environment'))
        
        # set grid configurations for column of header frame
        self.selection_section.grid_columnconfigure(0, weight=1, uniform='a')
        self.selection_section.grid_columnconfigure(1, weight=1, uniform='a')
        self.selection_section.grid_columnconfigure(2, weight=1, uniform='a')
        self.selection_section.grid_columnconfigure(3, weight=1, uniform='a')
        self.selection_section.grid_columnconfigure(4, weight=1, uniform='a')
        self.selection_section.grid_columnconfigure(5, weight=1, uniform='a')
        self.selection_section.grid_columnconfigure(6, weight=1, uniform='a')
        self.selection_section.grid_columnconfigure(7, weight=1, uniform='a')


    def set_app_env_name(self, text_value, option_selected, text_name):
        logging.info(friday_reusable.get_function_name())

        update_data                     = text_value.get()
        
        if update_data != "":
            self.app_config[option_selected][text_name] = text_value.get()


    def evaluate_first_option(self, selection_event) -> None:
        logging.info(friday_reusable.get_function_name())

        self.first_option_selected      = self.operation_combo.get() 

        self.reset_gui()

        if "please select" not in self.first_option_selected.lower():
            # copy app config in temp variable 
            self.temp_config                = (self.app_config.get(self.first_option_selected)).copy()
            self.application_name           = self.temp_config.pop("Application")
            self.environment_name           = self.temp_config.pop("Environment")

            # enable second drop down 
            self.sub_operation_combo.configure(state=READONLY)
            self.sub_operation_combo.configure(values=["Please Select..."] + list(self.temp_config))
            self.sub_operation_combo.current(0)

            # set the application name entry box 
            self.application_entry.configure(state=NORMAL)
            self.application_name_text.set(self.application_name)

            # set the environment name entry box 
            self.environment_entry.configure(state=NORMAL)
            self.environment_name_text.set(self.environment_name)
        

    def reset_gui(self) -> None:
        logging.info(friday_reusable.get_function_name())

        self.second_option_selected     = "" 
        self.temp_config                = None 
        self.application_name           = None 
        self.environment_name           = None 

        self.sub_operation_combo.configure(values=["",])
        self.sub_operation_combo.current(0)
        self.application_name_text.set("")
        self.environment_name_text.set("")

        self.sub_operation_combo.configure(state=DISABLED)
        self.application_entry.configure(state=DISABLED)
        self.environment_entry.configure(state=DISABLED)

        self.destory_execution_block()

    def destory_execution_block(self):
        if self.execution_section:
            self.execution_section.destroy()


    def evaluate_second_option(self, selection_event) -> None:
        logging.info(friday_reusable.get_function_name())

        self.second_option_selected = self.sub_operation_combo.get()

        # remove the execution frame if exists 
        self.destory_execution_block()

        # create list of execution block widgets 
        if "please select" not in self.second_option_selected.lower():
            # get the description 
            self.execution_frame_text       = self.temp_config[self.second_option_selected].get("description", None)

            # create frame for execution window 
            self.execution_frame            = tkb.Labelframe(self.travis_frame,
                                                             text=self.execution_frame_text)
            self.execution_frame.grid(row=5, column=0, columnspan=11, padx=(10,10), pady=(10,10), sticky="ew")

            # create execution label frame 
            self.execution_canvas           = tkb.Canvas(self.execution_frame)
            self.execution_canvas.pack(side=LEFT, fill=BOTH, expand=1)
            self.execution_canvas.pack_propagate(False)

            # create scrollbar on exection frame 
            self.execution_scrollbar        = tkb.Scrollbar(self.execution_frame, 
                                                            orient=VERTICAL, 
                                                            bootstyle="round")
            self.execution_scrollbar.pack(side=RIGHT, fill=Y)            
            
            # configure the scroll bar
            self.execution_scrollbar.config(command=self.execution_canvas.yview)              

            # configure the canvas
            self.execution_canvas.configure(yscrollcommand=self.execution_scrollbar.set)

            # bind the canvas to scrolling event 
            self.execution_canvas.bind("<Configure>", lambda e:self.execution_canvas.configure(scrollregion=self.execution_canvas.bbox("all")))
            self.execution_canvas.bind("<MouseWheel>", lambda e: self.execution_canvas.yview_scroll(int(-1*(e.delta/120)), "units"))

            # create another frame inside the canvas 
            self.execution_section = tkb.Frame(self.execution_canvas)
            self.execution_canvas.create_window((0,0), window=self.execution_section, anchor="nw", width=self.screen_total_width-self.execution_scrollbar.winfo_width()-50)

                                                            # text=self.execution_frame_text)
            # self.execution_section.grid(row=4, column=0, columnspan=11, padx=(10, 10), pady=10, sticky="ew")
            self.execution_section.grid_columnconfigure(0, weight=1, uniform="a")
            self.execution_section.grid_columnconfigure(1, weight=1, uniform="a")

            # populate the widgets for execution section 
            self.populate_execution_section(label_frame=None, 
                                            frame_row=1, 
                                            frame_column=1, 
                                            first_option_selected=self.first_option_selected,
                                            second_option_selected=self.second_option_selected,
                                            config_data=(self.app_config[self.first_option_selected][self.second_option_selected]).copy(), 
                                            layout_counter=0)
        
 
            self.execution_canvas.configure(scrollregion=self.execution_canvas.bbox(ALL))
                 

    def populate_execution_section(self, label_frame, frame_row, frame_column, first_option_selected, second_option_selected, config_data, layout_counter=0) -> None:
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
                # for quadrant = 0 
                if layout_counter       == 0: 
                    frame_column        = 0
                    frame_row           = 1
                elif layout_counter     == 1:
                    frame_column        = 1 
                    frame_row           = 1
                elif layout_counter     == 2:
                    frame_column        = 0 
                    frame_row           = 2                    
                elif layout_counter     == 3: 
                    frame_column        = 1
                    frame_row           = 2                    


                # create labelframe container for dicitonary items 
                label_frame             = tkb.Labelframe(self.execution_section, 
                                                         text=value.get('description',""), 
                                                         relief='ridge')
                label_frame.grid(column=frame_column, 
                                    row=frame_row, 
                                    padx=(10, 10),
                                    pady=(10, 10), 
                                    sticky='nsew')
                             
                # set grid weight for label frame 
                label_frame.grid_columnconfigure(0, weight=3, uniform='a')
                label_frame.grid_columnconfigure(1, weight=10, uniform='a')
                label_frame.grid_columnconfigure(2, weight=1, uniform='a')

                # increment the layout counter
                layout_counter += 1

                # recursive call for populating key-value pair
                self.populate_execution_section(label_frame, 
                                                frame_row, 
                                                frame_column, 
                                                first_option_selected, 
                                                second_option_selected, 
                                                config_data[key], 
                                                layout_counter)

            elif isinstance(value, bool):
                # set the lable name of the input data string
                label_key               = key.replace("_", " ")
                label                   = tkb.Label(label_frame, 
                                                    text=label_key.title())
                label.grid(row=r_value, 
                           column=c_value, 
                           padx=(5, 5), 
                           pady=(5, 5), 
                           sticky='ew')  
                
                label.grid_propagate(False)

                # create boolean radio button
                text_string = tk.BooleanVar()
                text_string.trace('w', lambda name, index, mode, 
                                  text_string=text_string : self.update_yaml_data(text_string, 
                                                                                  self.first_option_selected, 
                                                                                  self.second_option_selected, 
                                                                                  name, 
                                                                                  key))
                text_string.set(bool(value))

                # create a toggle button 
                toggle_button = tkb.Checkbutton(label_frame,
                                                variable=text_string, 
                                                bootstyle="round-toggle")
                toggle_button.grid(row=r_value, 
                                   column=c_value+1,
                                   padx=(5, 5),
                                   pady=(5, 5),
                                #    columnspan=2,
                                   sticky="w")
                toggle_button.grid_propagate(False)

            # Display numeric data
            elif isinstance(value, int):
                # set the lable name of the input data string
                label_key               = key.replace("_", " ")
                label                   = tkb.Label(label_frame, 
                                                   text=label_key.title())
                label.grid(row=r_value, 
                           column=c_value, 
                           padx=(5, 5), 
                           pady=(5, 5), 
                           sticky='ew')

                # set numeric data to configuration
                text_string = tkb.IntVar()
                text_string.trace('w', lambda name, index, mode, 
                                  text_string=text_string : self.update_yaml_data(text_string, 
                                                                                  self.first_option_selected,
                                                                                  self.second_option_selected, 
                                                                                  name, 
                                                                                  key))
                # text_string.set(int(value))

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

            # Display string data 
            elif isinstance(value, str):

                # set the lable name of the input data string
                label_key = key.replace("_", " ")
                label_key_list = label_key.split()

                label                   = tkb.Label(label_frame, 
                                                    text=label_key.title())
                label.grid(row=r_value, 
                           column=c_value, 
                           padx=(5, 5), 
                           pady=(5, 5), 
                           sticky='ew')

                # set string data on the entry panel
                text_string = tk.StringVar()
                text_string.trace('w', lambda name, index, mode, 
                                  text_string=text_string : self.update_yaml_data(text_string, self.first_option_selected, self.second_option_selected, name, key))
                text_string.set(value)

                # check if password field and hide the informtion on screen
                if 'password' in label_key.lower():
                    text_entry          = tkb.Entry(label_frame, 
                                                   textvariable=text_string, 
                                                   show='*')
                    text_entry.grid(row=r_value, 
                                    column=c_value + 1, 
                                    sticky='ew', 
                                    # columnspan=2
                                    padx=(5, 5), 
                                    pady=(5, 5))    
                    text_entry.grid_propagate(False)

                elif 'location' in label_key.lower():
                    text_entry          = tkb.Entry(label_frame, 
                                                   textvariable=text_string, 
                                                   state=DISABLED)
                    text_entry.grid(row=r_value, 
                                    column=c_value + 1, 
                                    sticky='ew', 
                                    # columnspan=2,
                                    padx=(5, 5), 
                                    pady=(5, 5))
                    
                    button              = tkb.Button(label_frame, 
                                                    #  image=self.open_img,
                                                    text="Open",
                                                     padding=0,
                                                     command=lambda text_string=text_string, key=key : self.open_directory_location(text_string, key, value))
                    button.grid(row=r_value, 
                                column=c_value+2, 
                                sticky='nsew', 
                                padx=(5, 5), 
                                pady=(5, 5))                    
                    text_entry.grid_propagate(False)

                elif label_key_list[-1].lower() == 'file':
                    text_entry          = tkb.Entry(label_frame, 
                                                   textvariable=text_string, 
                                                   state=DISABLED)
                    
                    text_entry.grid(row=r_value, 
                                    column=c_value+1, 
                                    sticky='ew', 
                                    # columnspan=2,
                                    padx=(5, 5), 
                                    pady=(5, 5)) 
                    
                    button              = tkb.Button(label_frame, 
                                                    # image=self.open_img, 
                                                    text="Open",
                                                    padding=0,
                                                    command=lambda text_string=text_string, key=key : self.open_file_location(text_string, key))
                    button.grid(row=r_value, 
                                column=c_value+2, 
                                sticky='nsew', 
                                padx=(5, 5), 
                                pady=(5, 5))
                    text_entry.grid_propagate(False)

                elif 'separator' in label_key.lower() or 'delimiter' in label_key.lower():
                    text_entry          = tkb.Combobox(label_frame, 
                                                       values=[",", "|", ".", "#", "$", "^", "~", "&", "*", "-", "+", "tab", "whitespace"], 
                                                       textvariable=text_string)
                    
                    text_entry.grid(row=r_value, 
                                    column=c_value+1, 
                                    sticky='w', 
                                    padx=(5, 5), 
                                    pady=(5, 5)) 
                    text_entry.grid_propagate(False)

                elif 'code page' in label_key.lower():
                    text_entry          = tkb.Combobox(label_frame, 
                                                       values=["ascii", "utf-8"], 
                                                       textvariable=text_string)
                    
                    text_entry.grid(row=r_value, 
                                    column=c_value+1, 
                                    sticky='w', 
                                    padx=(5, 5), 
                                    pady=(5, 5)) 
                    text_entry.grid_propagate(False)                    

                else:
                    text_entry          = tkb.Entry(label_frame, 
                                                   textvariable=text_string)
                    
                    text_entry.grid(row=r_value, 
                                    column=c_value+1, 
                                    sticky='ew', 
                                    padx=(5, 5), 
                                    pady=(5, 5))                       
                    text_entry.grid_propagate(False)

            elif isinstance(value, list):
                # set the lable name of the input data string
                label_key               = key.replace("_", " ")
                label                   = tkb.Label(label_frame, 
                                                   text=label_key.title())
                label.grid(row=r_value, 
                           column=c_value, 
                           padx=(5, 5), 
                           pady=(5, 5), 
                           sticky='ew')

                # set string data in entry panel
                text_string             = tk.StringVar()
                text_string.trace('w', lambda name, index, mode, 
                                  text_string=text_string : self.update_yaml_data(text_string, self.first_option_selected, self.second_option_selected, name, key))

                text_string.set(','.join(str(list_data).strip() for list_data in value))

                text_entry              = tkb.Entry(label_frame, 
                                                   textvariable=text_string)
                text_entry.grid(row=r_value, 
                                column=c_value+1, 
                                sticky='ew', 
                                padx=(5 ,5), 
                                pady=(5, 5))
                text_entry.grid_propagate(False)

            label_frame.update()
            r_value += 1

        self.submit_button              = tkb.Button(self.execution_section, 
                                                     image=self.btn_img, 
                                                     padding=0,
                                                     command=self.process_request)
        self.submit_button.grid(row=5, 
                                column=0, 
                                sticky='sw', 
                                padx=(10, 10),
                                pady=(10, 10))
        

    def open_directory_location(self, text_string, key, value) -> 'None':
        """ Opens up file dialog for user to select the folder for input and output """

        logging.info(friday_reusable.get_function_name())

        # print (text_string, key, self.first_option_selected, self.second_option_selected)
        title = f"Select {key}"
        location = filedialog.askdirectory(initialdir=value, title=title)

        # open file dialog and set the value for location selected 
        for child_key, _ in self.app_config[self.first_option_selected][self.second_option_selected].items():
            if key in self.app_config[self.first_option_selected][self.second_option_selected][child_key]:
                self.app_config[self.first_option_selected][self.second_option_selected][child_key][key] = location 

        text_string.set(location)


    def open_file_location(self, text_string, key) -> 'None':
        """ Opens up file dialog for user to select the folder for input and output """

        logging.info(friday_reusable.get_function_name())

        # print (text_string, key, self.first_option_selected, self.second_option_selected)

        filetypes = [("All Files", "*.*"),
                     ("Text Files", "*.txt"),
                     ("SQL Files", "*.sql"),
                     ("CSV Files", "*.csv"),
                     ("json Files", "*.json"),
                     ("Key Files", "*.pem")]        

        title = f"Select {key}"
        location = filedialog.askopenfilename(initialdir='./', title=title, filetypes=filetypes)

        # open file dialog and set the value for location selected 
        for child_key, _ in self.app_config[self.first_option_selected][self.second_option_selected].items():
            if key in self.app_config[self.first_option_selected][self.second_option_selected][child_key]:
                self.app_config[self.first_option_selected][self.second_option_selected][child_key][key] = location 

        text_string.set(location)


    def add_menu_bar(self) -> 'None':
        """ Add menu bar items to the root window """

        logging.info(friday_reusable.get_function_name())

        # create menaubar
        self.menubar = tkb.Menu(self)

        self.file_menu = tkb.Menu(self.menubar, tearoff=False)

        self.file_menu.add_command(label='Show Log', command=lambda: self.show_log_file())
        self.file_menu.add_command(label='Show History', command=lambda : self.show_history())
        self.file_menu.add_separator()
        self.file_menu.add_command(label='Reset GUI', command=self.reset_gui)
        self.file_menu.add_command(label='Clean Workspace', command=lambda : friday_reusable.purge_workspace_folders(current_location=self.mypath))
        self.file_menu.add_separator()
        self.file_menu.add_command(label='Exit', command=self.destroy)

        # add the File menu to the menubar
        self.menubar.add_cascade(label="File", menu=self.file_menu)

        # create the Help menu
        self.help_menu = tkb.Menu(self.menubar, tearoff=0)

        self.help_menu.add_command(label='About', command=lambda:self.show_travis())

        # add the Help menu to the menubar
        self.menubar.add_cascade(label="Help", menu=self.help_menu)
        
        self.config(menu=self.menubar)

    def show_history(self) -> None:

        self.show_history_frame  = tkb.Toplevel(self)

        screen_width            = self.winfo_screenwidth()
        screen_height           = self.winfo_screenheight()

        self.about.geometry(f"350x200+{screen_width//2 - 350//2}+{screen_height//2 - 200//2}")        
        self.about.title("Run History")        
        
        columns = ("operation_name", "sub_operation_name", "output_folder", "start_time", "end_time", "status")

        history_tree = tkb.Treeview(self.show_history_frame, columns=columns, show="headings")
        history_tree.heading('operation_name', "Operation Name")
        history_tree.heading('sub_operation_name', "Sub Operation Name")
        history_tree.heading('output_folder', "Output_Folder")
        history_tree.heading('start_time', "Start Time")
        history_tree.heading('end_time', "End Time")
        history_tree.heading('status', "Status")


    def show_travis(self) -> 'None':
        """ Show travis popup window with details """

        logging.info(friday_reusable.get_function_name())

        self.about              = tkb.Toplevel(self)
        # self.about.iconphoto(False, ImageTk.PhotoImage(self.travis_pil_resized))

        screen_width            = self.winfo_screenwidth()
        screen_height           = self.winfo_screenheight()

        self.about.geometry(f"350x200+{screen_width//2 - 350//2}+{screen_height//2 - 200//2}")        
        self.about.title("TRAVIS About")
        travis_png_bytes        = base64.b64decode(self.travis_icon.encode())
        travis_img              = io.BytesIO(travis_png_bytes)
        self.travis_logo        = ImageTk.PhotoImage(Image.open(travis_img))

        # place on labels 
        tkb.Label(self.about, image=self.travis_logo).pack()
        tkb.Label(self.about, text="By Deloitte Version 1.0").pack(pady=15)


    def show_log_file(self) -> 'None':
        """ Open log files from Menu option """

        logging.info(friday_reusable.get_function_name())

        log = os.path.join(self.mypath, 'Travis.log')
        show_notepad = 'notepad.exe %s' %(log)
        target = os.system(show_notepad)  


    # Log error message and display
    def log_error_message(self, valid_indicator, message):
        '''LOG ERROR MESSAGE AND EXIT FROM THE APPLICATION'''
        
        logging.info(friday_reusable.get_function_name())
        logging.info('ERROR MESSAGE FOR: '+ str(valid_indicator) + ' MESSAGE PASSED '+ str(message))
        
        if not valid_indicator:
            logging.error(message)
            messagebox.showerror('TRAVIS.', message)
            return

    # Log error message and display
    def log_exception_message(self, message):
        '''LOG ERROR MESSAGE AND EXIT FROM THE APPLICATION'''
        
        logging.info(friday_reusable.get_function_name())
        logging.critical('CRITICAL EXCEPTION ' + str(message))
        messagebox.showerror('TRAVIS.', 'CRITICAL ERROR OCCURED. PLEASE CHECK LOG FILE')
        return


    def process_request(self):
        """ Process request entered by the user on the current screen """
        
        logging.info(friday_reusable.get_function_name())

        # get configurations for from the GUI 
        configuration, application_name, environment_name = self.get_app_env_name()

        # validate if application name and environment names are populated 
        if application_name is None or application_name == "":
            message = "Application Name cannot be spaces"
            self.log_error_message(False, message)
            return 
        
        if self.special_regex.search(application_name) is not None : 
            message = "Application Name cannot have special characters"
            self.log_error_message(False, message)
            return             
        
        if environment_name is None or environment_name == "":
            message = "Environment name cannot be empty"
            self.log_error_message(False, message)
            return
        
        if self.special_regex.search(environment_name) is not None:
            message = "Environment name cannot have special characters"
            self.log_error_message(False, message)
            return            

        if self.second_option_selected in self.running_option:
            message = self.second_option_selected + " is still running. Please wait"
            self.log_error_message(False,message)
            return
        
        self.running_option.append(self.second_option_selected)                

        # first call the create method to put process and progress label on GUI
        current_process_label, progress_label, progress_bar, col_nbr = self.create_progress_bar()

        # Evaluate each request type and invoke separate thread
        try:
            if self.first_option_selected in ['COMPARE_FILE_METADATA', ]:
                logging.info('compare_file_metadata')
                t = Thread(target=master.compare_file_metadata, args=(self,
                                                                      configuration,
                                                                      self.first_option_selected, 
                                                                      self.second_option_selected,
                                                                      self.mypath,
                                                                      progress_label,
                                                                      self.gui_config,
                                                                      application_name,
                                                                      environment_name, 
                                                                      current_process_label, 
                                                                      progress_bar,
                                                                      col_nbr))
                t.start()

            elif self.first_option_selected in ['JSON_COMPARE', ]:
                logging.info('stream_and_compare_json_files')
                t = Thread(target=master.stream_and_compare_json_files, args=(self,
                                                                              configuration,
                                                                              self.first_option_selected, 
                                                                              self.second_option_selected,
                                                                              self.mypath,
                                                                              progress_label,
                                                                              self.gui_config,
                                                                              application_name,
                                                                              environment_name, 
                                                                              current_process_label, 
                                                                              progress_bar,
                                                                              col_nbr))
                t.start()

            elif self.first_option_selected in ['CSV_COMPARE', ]:
                logging.info('stream_and_compare_csv_files')
                t = Thread(target=master.stream_and_compare_csv_files, args=(self,
                                                                             configuration,
                                                                             self.first_option_selected, 
                                                                             self.second_option_selected,
                                                                             self.mypath,
                                                                             progress_label,
                                                                             self.gui_config,
                                                                             application_name,
                                                                             environment_name, 
                                                                             current_process_label, 
                                                                             progress_bar,col_nbr))
                t.start()

            elif self.first_option_selected in ['CSV_TOKENIZATION', ]:
                logging.info('process_encryption_request')
                t = Thread(target=master.process_encryption_request, args=(self,
                                                                           configuration,
                                                                           self.first_option_selected, 
                                                                           self.second_option_selected,
                                                                           self.mypath,
                                                                           progress_label,
                                                                           self.gui_config,
                                                                           application_name,
                                                                           environment_name, 
                                                                           current_process_label, 
                                                                           progress_bar,col_nbr))
                t.start()  

            elif self.first_option_selected in ['CSV_MANIPULATION', ]:
                logging.info('process_csv_manipulation')
                t = Thread(target=master.process_csv_manipulation, args=(self, 
                                                                         configuration,
                                                                         self.first_option_selected, 
                                                                         self.second_option_selected,
                                                                         self.mypath,progress_label,
                                                                         self.gui_config,
                                                                         application_name,
                                                                         environment_name, 
                                                                         current_process_label, 
                                                                         progress_bar,
                                                                         col_nbr))
                t.start()      

            elif self.first_option_selected in ['JSON_MANIPULATION', ]:
                logging.info('process_json_manipulation')
                t = Thread(target=master.process_json_manipulation, args=(self, 
                                                                          configuration,
                                                                          self.first_option_selected, 
                                                                          self.second_option_selected,
                                                                          self.mypath,progress_label,
                                                                          self.gui_config,
                                                                          application_name,
                                                                          environment_name, 
                                                                          current_process_label, 
                                                                          progress_bar,
                                                                          col_nbr))
                t.start()  

            elif self.first_option_selected in ['MONGO_UTILITIES', ]: 
                logging.info('process_mongo_request')
                t = Thread(target=master.process_mongo_request, args=(self, 
                                                                      configuration,
                                                                      self.first_option_selected, 
                                                                      self.second_option_selected,
                                                                      self.mypath,progress_label,
                                                                      self.gui_config,
                                                                      application_name,
                                                                      environment_name, 
                                                                      current_process_label, 
                                                                      progress_bar,
                                                                      col_nbr, 
                                                                      self.app_config))
                t.start()

            elif self.first_option_selected in ['MIGRATION_UTILITIES', ]: 
                logging.info('process_migration_request')
                t = Thread(target=master.process_migration_request, args=(self, 
                                                                          configuration,
                                                                          self.first_option_selected, 
                                                                          self.second_option_selected,
                                                                          self.mypath,progress_label,
                                                                          self.gui_config,
                                                                          application_name,
                                                                          environment_name, 
                                                                          current_process_label, 
                                                                          progress_bar,
                                                                          col_nbr, 
                                                                          self.app_config))
                t.start()                

            else: 
                message = "Looks like TRAVIS option selected is not avaiable at this moment. Please contact TRAVIS Help for more information on: " + str(','.join([self.second_option_selected, self.second_option_selected]))
                logging.error(message)
                self.log_error_message(False, message)
                
        except Exception as e:
            logging.critical(e)
            message = 'CRITICAL ERROR. PLEASE CHECK THE LOG FILE'
            self.log_error_message(False, message)


    def create_progress_bar(self) -> 'tuple[tkb.Label, tkb.Label, tkb.Progressbar, int]':
        """ create progress bar on submit frame """        
        
        logging.info(friday_reusable.get_function_name())

        # set column and row number
        col_nbr = 1 
        row_nbr = 0

        # if x_list is non empty 
        if self.x_list:
            col_nbr                 = self.x_list[-1]
            col_nbr                 += 1 

        self.x_list.append(col_nbr)

        # create a process label to show which process is running 
        current_process_label       = tkb.Label(self.progress_frame, 
                                                text=str(self.second_option_selected))
        current_process_label.grid(row=row_nbr, column=col_nbr, padx=5, pady=2, sticky='w')

        # get real time progress on this label 
        progress_label              = tkb.Label(self.progress_frame, 
                                                text=str(self.second_option_selected))
        progress_label.grid(row=row_nbr+2, column=col_nbr, padx=5, pady=2, sticky='w')

        # get real time chaging progress bar on this 
        progress_bar                = tkb.Progressbar(self.progress_frame, 
                                                      orient=HORIZONTAL, 
                                                      length=100,
                                                      mode='indeterminate')
        progress_bar.grid(row=row_nbr+1, column=col_nbr, padx=5, pady=2, sticky='w')
        progress_bar.start()            

        return current_process_label, progress_label, progress_bar, col_nbr


    def destroy_progress_bar(self, progress_label, progress_bar, current_process_label, col_nbr, stem_option) -> 'None':
        """ remove the progress bar from the GUI once processing is complete """
        
        logging.info(friday_reusable.get_function_name())

        progress_bar.stop()
        progress_bar.destroy()
        progress_label.destroy()
        current_process_label.destroy()

        self.x_list.remove(col_nbr) if col_nbr in self.x_list else None
        self.running_option.remove(stem_option) if stem_option in self.running_option else None


    def get_app_env_name(self) -> 'tuple[dict, str, str]':
        """ gets app and env name from the app configuration """
        
        logging.info(friday_reusable.get_function_name())

        configuration               = self.app_config[self.first_option_selected][self.second_option_selected].copy()
        application_name            = self.app_config[self.first_option_selected].get("Application", "")
        environment_name            = self.app_config[self.first_option_selected].get("Environment", "")

        return configuration, application_name, environment_name


    @staticmethod
    def compare_file_metadata(root, configuration, first_option_selected, second_option_selected, mypath, progress_label, gui_config, application_name, environment_name, current_process_label, progress_bar, col_nbr) -> 'None':
        """ Process JSON compare request """

        logging.info(friday_reusable.get_function_name())

        try:
            compare_file              = CompareMetaData(configuration, 
                                                        first_option_selected, 
                                                        second_option_selected, 
                                                        mypath, 
                                                        progress_label, 
                                                        gui_config, 
                                                        application_name, 
                                                        environment_name)
            # compare_file.show()
            message = compare_file.compare_files_metadata()
            messagebox.showinfo('TRAVIS.', message)       

        except ValidationException as e: 
            master.log_error_message(root, False, e)

        except ProcessingException as e: 
            master.log_error_message(root, False, e)           

        except Exception as e: 
            master.log_exception_message(root, e)   
        
        # check if successful then show success message
        master.destroy_progress_bar(root, progress_label, progress_bar, current_process_label, col_nbr, second_option_selected)


    @staticmethod
    def stream_and_compare_json_files(root, configuration, first_option_selected, second_option_selected, mypath, progress_label, gui_config, application_name, environment_name, current_process_label, progress_bar, col_nbr) -> 'None':
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
                                                        progress_label, 
                                                        gui_config, 
                                                        application_name, 
                                                        environment_name)
                message = compare_json.compare_json_streams()

            elif second_option_selected == 'JSON_Dynamic_Compare':
                compare_json        = JsonDynamicCompare(configuration, 
                                                         first_option_selected, 
                                                         second_option_selected, 
                                                         mypath, 
                                                         progress_label, 
                                                         gui_config, 
                                                         application_name, 
                                                         environment_name)
                
                message = compare_json.compare_json_data()

            messagebox.showinfo('TRAVIS.', message)       

        except ValidationException as e: 
            master.log_error_message(root, False, e)

        except ProcessingException as e: 
            master.log_error_message(root, False, e)           

        except Exception as e: 
            master.log_exception_message(root, e)   
        
        # check if successful then show success message
        master.destroy_progress_bar(root, progress_label, progress_bar, current_process_label, col_nbr, second_option_selected)

    
    @staticmethod
    def stream_and_compare_csv_files(root, configuration, first_option_selected, second_option_selected, mypath, progress_label, gui_config, application_name, environment_name, current_process_label, progress_bar, col_nbr) -> 'None':
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
                                                       progress_label, 
                                                       gui_config, 
                                                       application_name, 
                                                       environment_name)
                message = compare_json.compare_csv_streams()
            elif second_option_selected == 'CSV_Dynamic_Compare':
                compare_json        = CsvDynamicCompare(configuration, 
                                                        first_option_selected, 
                                                        second_option_selected, 
                                                        mypath, 
                                                        progress_label, 
                                                        gui_config, 
                                                        application_name, 
                                                        environment_name)
                message = compare_json.compare_csv_data()

            messagebox.showinfo('TRAVIS.', message)       

        except ValidationException as e: 
            master.log_error_message(root, False, e)

        except ProcessingException as e: 
            master.log_error_message(root, False, e)           

        except Exception as e: 
            master.log_exception_message(root, e)   
        
        # check if successful then show success message
        master.destroy_progress_bar(root, progress_label, progress_bar, current_process_label, col_nbr, second_option_selected)


    @staticmethod
    def process_encryption_request(root, configuration, first_option_selected, second_option_selected, mypath, progress_label, gui_config, application_name, environment_name, current_process_label, progress_bar, col_nbr) -> 'None':
        """ Process base 64 encryption request """
        
        logging.info(friday_reusable.get_function_name())

        try: 
            tokenize_csv_file       = TokenizeBase64Csv(configuration,
                                                        first_option_selected, 
                                                        second_option_selected, 
                                                        mypath, 
                                                        progress_label, 
                                                        gui_config,
                                                        application_name, 
                                                        environment_name)
            

            message                 = tokenize_csv_file.perform_csv_tokenization()

            messagebox.showinfo('TRAVIS.', message)       

        except ValidationException as e: 
            master.log_error_message(root, False, e)

        except ProcessingException as e: 
            master.log_error_message(root, False, e)        

        except Exception as e: 
            master.log_exception_message(root, e)            
        
        # Check if successful then show success message
        master.destroy_progress_bar(root, progress_label, progress_bar, current_process_label, col_nbr, second_option_selected)


    @staticmethod
    def process_csv_manipulation(root, configuration, first_option_selected, second_option_selected, mypath, progress_label, gui_config, application_name, environment_name, current_process_label, progress_bar, col_nbr) -> 'None':
        """ Perform CSV Manipulations like split horizintally, vertically, conditionally and merge the files """

        logging.info(friday_reusable.get_function_name())

        try: 
            message = ""

            split_csv_file      = CsvManipulation(configuration, 
                                                  first_option_selected,
                                                  second_option_selected, 
                                                  mypath, 
                                                  progress_label, 
                                                  gui_config,
                                                  application_name, 
                                                  environment_name)
            
            message             = split_csv_file.perform_csv_manipulation()         

            messagebox.showinfo('TRAVIS.', message)       

        except ValidationException as e: 
            master.log_error_message(root, False, e)

        except ProcessingException as e: 
            master.log_error_message(root, False, e)           

        except Exception as e: 
            master.log_exception_message(root, e)   
        
        # check if successful then show success message
        master.destroy_progress_bar(root, progress_label, progress_bar, current_process_label, col_nbr, second_option_selected)

    
    @staticmethod
    def process_json_manipulation(root, configuration, first_option_selected, second_option_selected, mypath, progress_label, gui_config, application_name, environment_name, current_process_label, progress_bar, col_nbr) -> 'None':
        """ JSON data manipulations like convert to csv and merge various json files """
        
        logging.info(friday_reusable.get_function_name())

        try: 

            split_csv_file      = JsonManipulation(configuration, 
                                                   first_option_selected,
                                                   second_option_selected, 
                                                   mypath, 
                                                   progress_label, 
                                                   gui_config,
                                                   application_name, 
                                                   environment_name)
            message             = split_csv_file.perform_json_manipulation()

            messagebox.showinfo('TRAVIS.', message)       

        except ValidationException as e: 
            master.log_error_message(root, False, e)

        except ProcessingException as e: 
            master.log_error_message(root, False, e)           

        except Exception as e: 
            master.log_exception_message(root, e)   
        
        # check if successful then show success message
        master.destroy_progress_bar(root, progress_label, progress_bar, current_process_label, col_nbr, second_option_selected)


    @staticmethod
    def process_mongo_request(root, configuration, first_option_selected, second_option_selected, mypath, progress_label, gui_config, application_name, environment_name, current_process_label, progress_bar, col_nbr, app_config) -> 'None':
        """ Process Mongo migration request """
       
        try: 
            message = ""
            mongo_stats         = MongoUtilities(configuration,
                                                 first_option_selected,
                                                 second_option_selected, 
                                                 mypath, 
                                                 progress_label, 
                                                 gui_config,
                                                 application_name, 
                                                 environment_name, 
                                                 app_config,
                                                 'CSV_COMPARE', 
                                                 'CSV_Dynamic_Compare')
            
            message             = mongo_stats.perform_mongo_operations()

            messagebox.showinfo('TRAVIS.', message)       

        except ValidationException as e: 
            master.log_error_message(root, False, e)

        except ProcessingException as e: 
            master.log_error_message(root, False, e)           

        except Exception as e: 
            master.log_exception_message(root, e)   
        
        # check if successful then show success message
        master.destroy_progress_bar(root, progress_label, progress_bar, current_process_label, col_nbr, second_option_selected)        


    @staticmethod
    def process_migration_request(root, configuration, first_option_selected, second_option_selected, mypath, progress_label, gui_config, application_name, environment_name, current_process_label, progress_bar, col_nbr, app_config) -> 'None':
        """ Process Migration request """ 

        try: 
            message = ""
            aws_object          = MigrationUtilities(configuration,
                                                         first_option_selected,
                                                         second_option_selected, 
                                                         mypath, 
                                                         progress_label, 
                                                         gui_config,
                                                         application_name, 
                                                         environment_name)
            message             = aws_object.perform_aws_operation()
            
            messagebox.showinfo('TRAVIS.', message)       

        except ValidationException as e: 
            master.log_error_message(root, False, e)

        except ProcessingException as e: 
            master.log_error_message(root, False, e)           

        except Exception as e: 
            master.log_exception_message(root, e)   
        
        # check if successful then show success message
        master.destroy_progress_bar(root, progress_label, progress_bar, current_process_label, col_nbr, second_option_selected)        


    def update_yaml_data(self, textObject, first_option, second_option, name, key) -> 'None':
        """ Update application configurations for final run """

        # evaluate if name present in keyData dictionary 
        if name not in self.key_data.keys():
            self.key_data[name]     = key

        key = self.key_data.get(name)

        try:
            for child_node, _ in self.app_config[first_option][second_option].items():

                if child_node == "description":
                    continue 

                if key in self.app_config[first_option][second_option][child_node].keys():

                    # Check the instance of the input and populate accordingly.
                    if isinstance(self.app_config[first_option][second_option][child_node][key], list):
                        list_data = textObject.get().split(',')
                        clean = []
                        for items in list_data:
                            items = items.strip()
                            clean.append(items)
                        self.app_config[first_option][second_option][child_node][key] = clean

                    elif isinstance(self.app_config[first_option][second_option][child_node][key], bool) and bool(textObject.get()):
                        self.app_config[first_option][second_option][child_node][key] = bool(textObject.get())

                    elif isinstance(self.app_config[first_option][second_option][child_node][key], int) and int(textObject.get()):
                        self.app_config[first_option][second_option][child_node][key] = int(textObject.get())

                    else:
                        self.app_config[first_option][second_option][child_node][key] = textObject.get()

        except Exception as e:
            logging.error(e)

        # print (self.app_config[first_option][second_option])
            
if __name__ == '__main__' :
    multiprocessing.freeze_support()

    try: 
        self = master()
        self.mainloop()
    except Exception as e:
        print (e)