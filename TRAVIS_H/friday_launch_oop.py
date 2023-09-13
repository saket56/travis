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
import io
import logging
import math
import multiprocessing
import os
import re
import subprocess
import tkinter as tk
from threading import Thread
from tkinter import (DISABLED, HORIZONTAL, LEFT, TOP, filedialog, messagebox,
                     ttk)

import friday_reusable
from friday_config import FridayConfig, startup_process
from friday_exception import ProcessingException, ValidationException
from friday_process import (CsvDynamicCompare, CsvManipulation,
                            CsvStreamCompare, JsonCrossCompare,
                            JsonDynamicCompare, JsonManipulation,
                            JsonStreamCompare, TokenizeBase64Csv)
from PIL import Image, ImageTk


class master(tk.Tk):
    _instance = None 

    def __init__(self, *args, **kwargs):

        tk.Tk.__init__(self, *args, **kwargs)

        # child widgets = 
        self.child_widget = []
        self.option_selected = []
        self.running_option = []
        self.second_drop_widget = [] 
        self.x_list = [] 
        self.y_list = []
        self.key_data = {}

        # some variables 
        self.special_regex = re.compile("[@!#$%^*()<>?/\|}{~:]")

        # get path and configurations 
        self.mypath, self.config_data = friday_reusable.create_user_workspace()

        # some launch animations 
        # TODO - Add logic for user to provide the yaml file input location 
        _, _ = FridayConfig.show(root=self,
                                 function=startup_process)

        # static directory  
        self.static_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
        
        # create logging handle 
        log_level = None 
        if self.config_data['FridayConfig']['workspace_setting']['logging_level'] == "INFO":
            log_level=logging.INFO
        elif self.config_data['FridayConfig']['workspace_setting']['logging_level'] == "DEBUG":
            log_level=logging.DEBUG
        elif self.config_data['FridayConfig']['workspace_setting']['logging_level'] == "WARN":
            log_level=logging.WARN            
        elif self.config_data['FridayConfig']['workspace_setting']['logging_level'] == "ERROR":
            log_level=logging.ERROR
        elif self.config_data['FridayConfig']['workspace_setting']['logging_level'] == "CRITICAL":
            log_level=logging.CRITICAL
        else:
            log_level=logging.NOTSET            

        logging.basicConfig(filename=os.path.join(self.mypath, 'Travis.log'), 
                            filemode='w',
                            level=log_level, 
                            format=' %(asctime)s - {%(name)s : %(lineno)d} - %(levelname)s - %(message)s')
        
        # separate gui and app configurations 
        self.gui_config                 = self.config_data.get('FridayConfig')
        self.app_config                 = self.config_data.copy()
        self.app_config.pop('FridayConfig')

        # set parameters from GUI setting in yaml file
        self.set_parameters()

        # setup gui for TRAVIS application
        self.setup_gui()

        # add menu items 
        self.add_menu_bar()        


    def set_parameters(self) -> 'None':

        logging.info(friday_reusable.get_function_name())

        # get current resolution of user's screen 
        self.screen_total_width = self.winfo_screenwidth() 
        self.screen_total_height = self.winfo_screenheight()

        # GUI dimensions 
        self.vertical_margin            = self.gui_config['gui_settings'].get('vertical-margin')
        self.horizontal_margin          = self.gui_config['gui_settings'].get('horizontal-margin')
        self.screen_width               = self.screen_total_width - self.horizontal_margin
        self.screen_height              = self.screen_total_height - self.vertical_margin

        # get gui style and colors 
        self.travis_window_color        = self.gui_config['gui_settings'].get('window_color')
        self.travis_header_color        = self.gui_config['gui_settings'].get('header_color')
        self.travis_title               = self.gui_config['gui_settings'].get('title')
        self.travis_size                = f"{self.screen_width}x{self.screen_height}+0+0"
        self.travis_resizable           = self.gui_config['gui_settings'].get('resizable')

        # get base64 image data 
        self.travis_button              = self.gui_config['image_settings'].get('submit_button')
        self.travis_org_logo            = self.gui_config['image_settings'].get('deloitte_logo')
        self.travis_icon                = self.gui_config['image_settings'].get('travis_logo')
        self.open_button                = self.gui_config['image_settings'].get('open_button')

        # evaluate the screen size 
        if math.ceil(self.screen_width / 1000) <= 2:
            self.font_settings          = self.gui_config.get('display_settings_2k')
        else:
            self.font_settings          = self.gui_config.get('display_settings_4k')


        self.horizontal_spacing         = self.font_settings['horizontal-spacing-widget']
        self.vertical_spacing           = self.font_settings['vertical-spacing-widget']
        self.entry_widget_width         = self.font_settings['entry-width']
        self.entry_widget_border        = self.font_settings['entry-borderwidth']

        # settings for header font 
        self.header_font_color          = self.font_settings['header_font'].get('text_color')
        self.header_font_text           = self.font_settings['header_font'].get('text_size')
        self.header_font_text           = tuple(self.header_font_text)

        # settings for header2 font 
        self.header2_font_color         = self.font_settings['header2_font'].get('text_color')
        self.header2_font_text          = self.font_settings['header2_font'].get('text_size')
        self.header2_font_text          = tuple(self.header2_font_text)

        # settings for section font
        self.section_font_color         = self.font_settings['section_font'].get('text_color')
        self.section_font_text          = self.font_settings['section_font'].get('text_size')
        self.section_font_text          = tuple(self.section_font_text)

        # settings for label font 
        self.label_font_color           = self.font_settings['label_font'].get('text_color')
        self.label_font_text            = self.font_settings['label_font'].get('text_size')
        self.label_font_text            = tuple(self.label_font_text)

        # settings for entry font 
        self.entry_font_color           = self.font_settings['entry_font'].get('text_color')
        self.entry_font_text            = self.font_settings['entry_font'].get('text_size')
        self.entry_font_text            = tuple(self.entry_font_text)


        # entry configurations 
        self.entry_settings             = self.gui_config.get('entry_settings')

        # app entry setting 
        self.app_entry_box_color        = self.entry_settings['app_entry_box'].get('box_color')
        self.app_entry_text_color       = self.entry_settings['app_entry_box'].get('text_color')

        # env entry setting 
        self.env_entry_box_color        = self.entry_settings['env_entry_box'].get('box_color')
        self.env_entry_text_color       = self.entry_settings['env_entry_box'].get('text_color')

        # string entry setting 
        self.str_entry_box_color        = self.entry_settings['str_entry_box'].get('box_color')
        self.str_entry_text_color       = self.entry_settings['str_entry_box'].get('text_color')

        # number entry setting 
        self.num_entry_box_color        = self.entry_settings['num_entry_box'].get('box_color')
        self.num_entry_text_color       = self.entry_settings['num_entry_box'].get('text_color')

        # list entry setting 
        self.list_entry_box_color       = self.entry_settings['list_entry_box'].get('box_color')
        self.list_entry_text_color      = self.entry_settings['list_entry_box'].get('text_color')   

    
    # implement singleton pattern 
    def __new__(cls, *args, **kwargs):
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
        self.travis_png_bytes           = base64.b64decode(self.travis_icon.encode())
        self.travis_img                 = io.BytesIO(self.travis_png_bytes)
        self.travis_pil                 = Image.open(self.travis_img)
        self.travis_pil_resized         = self.travis_pil.resize((256,256))
        self.iconphoto(True, ImageTk.PhotoImage(self.travis_pil_resized))

        # create open button 
        self.open_png_bytes             = base64.b64decode(self.open_button.encode())
        self.open_img_stream            = io.BytesIO(self.open_png_bytes)
        self.open_img = ImageTk.PhotoImage(Image.open(self.open_img_stream))
        
        # create header frame 
        self.header_frame               = tk.Frame(self, 
                                                   width=self.screen_width, 
                                                   height=self.screen_height/10, 
                                                   relief='raised', 
                                                   borderwidth=2, 
                                                   border=2, 
                                                   bg=self.travis_header_color)
        self.header_frame.grid(row=0, column=0) 

        # create entry frame where user can enter the data 
        self.entry_frame                = tk.Frame(self, 
                                                   width=self.screen_width, 
                                                   height=self.screen_height - self.screen_height/10 - self.screen_height/10, 
                                                   relief='sunken', 
                                                   borderwidth=2, 
                                                   bg=self.travis_window_color)
        self.entry_frame.grid(row=1, column=0)
        self.entry_frame.grid_propagate(False)

        # create submission frame where user can enter the data 
        self.submit_frame               = tk.Frame(self, 
                                                   width=self.screen_width, 
                                                   height=self.screen_height/10, 
                                                   relief='raised', 
                                                   borderwidth=2, 
                                                   bg=self.travis_header_color)
        self.submit_frame.grid(row=3, column=0)
        self.submit_frame.grid_propagate(False)

        # add submit button on submit frame 
        btn_png_bytes                   = base64.b64decode(self.travis_button.encode())
        btn_img                         = io.BytesIO(btn_png_bytes)
        btn_pil_img                     = Image.open(btn_img)
        self.btn_img                    = ImageTk.PhotoImage(btn_pil_img)

        # populate header frame 
        self.populate_header_frame()


    def populate_header_frame(self) -> 'None':
        """ create header frame and populate widgets """

        logging.info(friday_reusable.get_function_name())

        # Add first drop down label on the top frame 
        first_level_label               = tk.Label(self.header_frame, 
                                                   text='Operation Name', 
                                                   font=self.header_font_text, 
                                                   bg=self.travis_header_color, 
                                                   fg=self.header_font_color)
        first_level_label.grid(row=0, column=0, sticky="w", padx=50)
           

        # Add second drop down level on the top frame 
        second_drop_label               = tk.Label(self.header_frame, 
                                                   text='Sub-Operation Name', 
                                                   font=self.header_font_text, 
                                                   bg=self.travis_header_color, 
                                                   fg=self.header_font_color)
        second_drop_label.grid(row=0, column=1, sticky="w", padx=50)
     

        # Add Application name 
        application_label               = tk.Label(self.header_frame, 
                                                   text='Application', 
                                                   font=self.header_font_text, 
                                                   bg=self.travis_header_color, 
                                                   fg=self.header_font_color)
        application_label.grid(row=0, column=2, sticky="w", padx=50)
       

        # Add Enviornment Label
        environment_label               = tk.Label(self.header_frame, 
                                                   text='Environment', 
                                                   font=self.header_font_text, 
                                                   bg=self.travis_header_color, 
                                                   fg=self.header_font_color)
        environment_label.grid(row=0, column=3, sticky="w", padx=50)

  
        # Add Deloitte Logo in row 0
        org_png_bytes                   = base64.b64decode(self.travis_org_logo.encode())
        org_img                         = io.BytesIO(org_png_bytes)
        org_pil_img                     = Image.open(org_img)
        org_pil_img                     = org_pil_img.resize((150,30))
        self.org_logo                   = ImageTk.PhotoImage(org_pil_img)
        logo_img_label                  = tk.Label(self.header_frame, image=self.org_logo)
        logo_img_label.grid(row=0, column=4, sticky="ne")  


        # child widgets for each label 
        self.first_level_text           = tk.StringVar()
        self.first_level_text.set('Please Select')
        self.first_level_dropdown       = tk.OptionMenu(self.header_frame, 
                                                        self.first_level_text, 
                                                        *self.app_config, 
                                                        command=self.evaluate_first_option)
        self.first_level_dropdown.grid(row=1, column=0, pady=5, sticky="w", padx=50)

        # create second level drop down list 
        self.second_level_text          = tk.StringVar()
        self.second_level_text.set("Please Select")
        self.second_level_choices       = ["", ]
        self.second_level_dropdown      = tk.OptionMenu(self.header_frame, 
                                                        self.second_level_text, 
                                                        *self.second_level_choices, 
                                                        command=None)
        self.second_level_dropdown.grid(row=1, column=1, pady=5, sticky="w", padx=50)
        self.second_level_dropdown.configure(state='disabled')


        # create entry for Application name 
        self.application_name_text      = tk.StringVar()
        self.application_name_text.set("")
        self.application_entry          = tk.Entry(self.header_frame, 
                                                   textvariable=self.application_name_text, 
                                                   width=30, 
                                                   borderwidth=self.entry_widget_border, 
                                                   font=self.label_font_text, 
                                                   bg=self.app_entry_box_color, 
                                                   fg=self.app_entry_text_color)
        self.application_entry.grid(row=1, column=2, pady=5, sticky="w", padx=50)
        self.application_entry.configure(state='disabled')

        # create entry for Environment name 
        self.environment_name_text      = tk.StringVar()
        self.environment_name_text.set("")
        self.environment_entry          = tk.Entry(self.header_frame, 
                                                   textvariable=self.environment_name_text, 
                                                   width=30, 
                                                   borderwidth=self.entry_widget_border, 
                                                   font=self.label_font_text, 
                                                   bg=self.app_entry_box_color, 
                                                   fg=self.app_entry_text_color)
        self.environment_entry.grid(row=1, column=3, pady=5, sticky="w", padx=50)        
        self.environment_entry.configure(state='disabled')

        # set grid configurations for column of header frame
        self.header_frame.grid_propagate(False)
        self.header_frame.grid_columnconfigure(0, weight=1, uniform='a')
        self.header_frame.grid_columnconfigure(1, weight=1, uniform='a')
        self.header_frame.grid_columnconfigure(2, weight=1, uniform='a')
        self.header_frame.grid_columnconfigure(3, weight=1, uniform='a')
        self.header_frame.grid_columnconfigure(4, weight=1, uniform='a')


    def evaluate_first_option(self, selection) -> 'None':
        """ evaluate selection made and update the header and entry frame """

        logging.info(friday_reusable.get_function_name())

        self.first_option_selected      = selection 

        # reset gui 
        self.reset_gui()

        # set submit button to disabled
        # self.submit_button.config(state='disabled')

        # copy app config in temp variable 
        temp_config                     = (self.app_config.get(selection)).copy()
        
        # get application name 
        app_name                        = temp_config.pop('Application', None)
        env_name                        = temp_config.pop('Environment', None)

        # create event for application entry update
        self.application_entry.configure(state='normal')
        self.application_name_text.trace('w', lambda name, 
                                         index, 
                                         mode, 
                                         application_name_text=self.application_name_text:self.set_app_env_name(application_name_text, selection, 'Application'))
        self.application_name_text.set(app_name)

        # create event for environment entry update
        self.environment_entry.configure(state='normal')
        self.environment_name_text.trace('w', lambda name, 
                                         index, 
                                         mode, 
                                         environment_name_text=self.environment_name_text:self.set_app_env_name(environment_name_text, selection, 'Environment'))
        self.environment_name_text.set(env_name)



        # create drop down list for second drop down
        self.second_level_dropdown.configure(state='normal')
        self.second_level_text.set("Please Select")
        self.second_level_dropdown['menu'].delete(0, 'end')
        second_level_selected = tk.StringVar()
        second_level_selected.set("")
        menu = temp_config.keys()
        for option in menu:
            self.second_level_dropdown['menu'].add_command(label=option, 
                                                        command=tk._setit(second_level_selected, 
                                                                        option, 
                                                                        lambda second_level_selected: self.populate_entry_frame(selection, second_level_selected)))

    def set_app_env_name(self, text_description, selection, key) -> 'None':
        """ get the entry text value updated """

        logging.info(friday_reusable.get_function_name())

        self.app_config[selection][key] = text_description.get()

        # print (self.application_name_text.get())


    def populate_entry_frame(self, first_level_selection, second_level_selection) -> 'None':
        """ get first and second level selection from option menu and populate entry frame """

        logging.info(friday_reusable.get_function_name())

        # update drop down value 
        self.second_level_text.set(second_level_selection)

        # set variables 
        self.first_option_selected      = first_level_selection
        self.second_option_selected     = second_level_selection

        # destory child widgets of entry frame if any                           
        self.reset_gui()
        
        # get the configuration data for first and second level selection 
        temp_config                     = (self.app_config[first_level_selection].get(second_level_selection)).copy()

        # put description lable at the top 
        description_text                = temp_config.pop('description')
        description_label               = tk.Label(self.entry_frame, 
                                                   text=description_text, 
                                                   font=self.header2_font_text, 
                                                   bg=self.travis_window_color, 
                                                   fg=self.header2_font_color)
        description_label.grid(row = 0, column=0, columnspan=2, sticky='ns', padx=100, pady=20)

        # put widgets on entry panel
        self.populate_gui_widgets(None, 1, 1, first_level_selection, second_level_selection, temp_config)
        
        # update state of submit button
        # self.submit_button.config(state='normal')


    def populate_gui_widgets(self, label_frame, frame_row, frame_column, first_level_selection, second_level_selection, config_data, layout_counter=0) -> 'None':
        """ populate entry frame with gui widgets with data passed """

        logging.info(friday_reusable.get_function_name())

        # initial row and column value for labelframe 
        rValue                          = 0
        cValue                          = 0

        # vertical padding for row = 0 of labelframe 
        initial_vertical_padding        = 50 

        # horizontal padding for column = 0 for labelframe 
        initial_horizontal_padding      = 100

        # horizonal spacing between labelframes 
        horizontal_padding              = 20

        # vertical spacing between labelframes 
        vertical_padding                = 20 

        # iterate over each item in yaml configuration
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

                    # create labelframe container for dicitonary items 
                    label_frame         = tk.LabelFrame(self.entry_frame, 
                                                        text=value.get('description',""), 
                                                        bg=self.travis_window_color, 
                                                        fg=self.section_font_color,
                                                        font=self.section_font_text,
                                                        relief='ridge')
                    label_frame.grid(column=frame_column, 
                                     row=frame_row, 
                                     padx=(initial_horizontal_padding, horizontal_padding),
                                     pady=(initial_vertical_padding, vertical_padding), 
                                     sticky='nsew')
                
                # for quadrant = 1
                elif layout_counter     == 1:
                    frame_column        = 1 
                    frame_row           = 1

                    # create labelframe container for dicitonary items 
                    label_frame         = tk.LabelFrame(self.entry_frame, 
                                                        text=value.get('description',""), 
                                                        bg=self.travis_window_color, 
                                                        fg=self.section_font_color,
                                                        font=self.section_font_text,
                                                        relief='ridge')
                    label_frame.grid(column=frame_column, 
                                     row=frame_row, 
                                     padx=(horizontal_padding, horizontal_padding),
                                     pady=(initial_vertical_padding, vertical_padding), 
                                     sticky='nsew')                    
                # for quadrant = 2
                elif layout_counter     == 2:
                    frame_column        = 0 
                    frame_row           = 2

                    # create labelframe container for dicitonary items 
                    label_frame         = tk.LabelFrame(self.entry_frame, 
                                                        text=value.get('description',""), 
                                                        bg=self.travis_window_color, 
                                                        fg=self.section_font_color,
                                                        font=self.section_font_text,
                                                        relief='ridge')
                    label_frame.grid(column=frame_column, 
                                     row=frame_row, 
                                     padx=(initial_horizontal_padding, horizontal_padding),
                                     pady=(vertical_padding, vertical_padding), 
                                     sticky='nsew')                    
                # for quadrant = 3
                elif layout_counter     == 3: 
                    frame_column        = 1
                    frame_row           = 2

                    # create labelframe container for dicitonary items 
                    label_frame         = tk.LabelFrame(self.entry_frame, 
                                                        text=value.get('description',""), 
                                                        bg=self.travis_window_color, 
                                                        fg=self.section_font_color,
                                                        font=self.section_font_text,
                                                        relief='ridge')
                    label_frame.grid(column=frame_column, 
                                     row=frame_row, 
                                     padx=(horizontal_padding, horizontal_padding),
                                     pady=(vertical_padding, vertical_padding), 
                                     sticky='nsew')                         

                # set grid weight for label frame 
                label_frame.grid_columnconfigure(0, weight=3)
                label_frame.grid_columnconfigure(1, weight=4)
                label_frame.grid_columnconfigure(2, weight=1)

                # increment the layout counter
                layout_counter += 1

                # recursive call for populating key-value pair
                self.populate_gui_widgets(label_frame, frame_row, frame_column, first_level_selection, second_level_selection, config_data[key], layout_counter)


            # check if value is boolean type
            elif isinstance(value, bool):

                # set the lable name of the input data string
                label_key               = key.replace("_", " ")
                label                   = tk.Label(label_frame, 
                                                   text=label_key.title(),
                                                   font=self.label_font_text, 
                                                   bg=self.travis_window_color,
                                                   fg=self.label_font_color)
                label.grid(row=rValue, 
                           column=cValue, 
                           padx=(self.horizontal_spacing, self.entry_widget_width-len(key)), 
                           pady=self.vertical_spacing, 
                           sticky='w')  
                
                label.grid_propagate(False)

                # create boolean radio button
                text_string = tk.BooleanVar()
                text_string.trace('w', lambda name, 
                                 index, 
                                 mode, 
                                 text_string=text_string : self.update_yaml_data(text_string, self.first_option_selected, self.second_option_selected, name, key))
                text_string.set(bool(value))

                text_radio_true         = tk.Radiobutton(label_frame, 
                                                         text='True', 
                                                         variable=text_string, 
                                                         value=True, 
                                                         bg=self.travis_window_color, 
                                                         fg=self.label_font_color, 
                                                         activebackground=self.travis_window_color, 
                                                         activeforeground=self.label_font_color, 
                                                         selectcolor=self.travis_window_color, 
                                                         font=self.label_font_text)

                text_radio_false        = tk.Radiobutton(label_frame, 
                                                         text='False', 
                                                         variable=text_string, 
                                                         value=False, 
                                                         bg=self.travis_window_color, 
                                                         fg=self.label_font_color, 
                                                         activebackground=self.travis_window_color, 
                                                         activeforeground=self.label_font_color,
                                                         selectcolor=self.travis_window_color, 
                                                         font=self.label_font_text)
                
                text_radio_true.grid(row=rValue, 
                                     column=cValue + 1, 
                                     padx=(self.horizontal_spacing, self.horizontal_spacing), 
                                     pady=self.vertical_spacing, 
                                     sticky='w')
                text_radio_true.grid_propagate(False)

                text_radio_false.grid(row=rValue, 
                                      column=cValue + 2, 
                                      padx=(self.horizontal_spacing, self.horizontal_spacing), 
                                      pady=self.vertical_spacing, 
                                      sticky='e')
                text_radio_false.grid_propagate(False)

            # Display numeric data
            elif isinstance(value, int):

                # set the lable name of the input data string
                label_key               = key.replace("_", " ")
                label                   = tk.Label(label_frame, 
                                                   text=label_key.title(),
                                                   font=self.label_font_text, 
                                                   bg=self.travis_window_color,
                                                   fg=self.label_font_color)
                label.grid(row=rValue, 
                           column=cValue, 
                           padx=(self.horizontal_spacing, self.entry_widget_width-len(key)), 
                           pady=self.vertical_spacing, 
                           sticky='w')

                # set numeric data to configuration
                text_string = tk.IntVar()
                text_string.trace('w', lambda name, 
                                 index, 
                                 mode, 
                                 text_string=text_string : self.update_yaml_data(text_string, self.first_option_selected, self.second_option_selected, name, key))

                text_string.set(int(value))

                # add entry panel on GUI
                text_entry              = tk.Entry(label_frame, 
                                                   textvariable=text_string, 
                                                   width=self.entry_widget_width, 
                                                   borderwidth=self.entry_widget_border,
                                                   font=self.entry_font_text, 
                                                   fg=self.num_entry_text_color, 
                                                   bg=self.num_entry_box_color)
                text_entry.grid(row=rValue, 
                                column=cValue + 1, 
                                sticky='e',
                                padx=(self.horizontal_spacing, self.horizontal_spacing), 
                                pady=self.vertical_spacing, 
                                columnspan=2)   
                text_entry.grid_propagate(False)

            # Display string data 
            elif isinstance(value, str):

                # set the lable name of the input data string
                label_key = key.replace("_", " ")
                label_key_list = label_key.split()

                label                   = tk.Label(label_frame, 
                                                   text=label_key.title(),
                                                   font=self.label_font_text, 
                                                   bg=self.travis_window_color,
                                                   fg=self.label_font_color)
                label.grid(row=rValue, 
                           column=cValue, 
                           padx=(self.horizontal_spacing, self.entry_widget_width-len(key)), 
                           pady=self.vertical_spacing, 
                           sticky='w')

                # set string data on the entry panel
                text_string = tk.StringVar()
                text_string.trace('w', lambda name, 
                                 index, 
                                 mode, 
                                 text_string=text_string : self.update_yaml_data(text_string, self.first_option_selected, self.second_option_selected, name, key))

                text_string.set(value)

                # check if password field and hide the informtion on screen
                if 'password' in label_key.lower():
                    text_entry          = tk.Entry(label_frame, 
                                                   textvariable=text_string, 
                                                   show='*', 
                                                   width=self.entry_widget_width, 
                                                   borderwidth=self.entry_widget_border,
                                                   font=self.entry_font_text, 
                                                   fg=self.str_entry_text_color, 
                                                   bg=self.str_entry_box_color)
                    
                    text_entry.grid(row=rValue, 
                                    column=cValue + 1, 
                                    sticky='e', 
                                    padx=(self.horizontal_spacing, self.horizontal_spacing), 
                                    pady=self.vertical_spacing, columnspan=2)    
                    text_entry.grid_propagate(False)

                elif 'location' in label_key.lower():
                    text_entry          = tk.Entry(label_frame, 
                                                   textvariable=text_string, 
                                                   width=self.entry_widget_width,
                                                   borderwidth=self.entry_widget_border,
                                                   state='disabled',
                                                   font=self.entry_font_text, 
                                                   fg=self.str_entry_text_color, 
                                                   bg=self.str_entry_box_color)
                    
                    text_entry.grid(row=rValue, 
                                    column=cValue + 1, 
                                    sticky='e', 
                                    padx=(self.horizontal_spacing, self.horizontal_spacing), 
                                    pady=self.vertical_spacing, columnspan=2) 
                    
                    button              = tk.Button(label_frame, 
                                                    image=self.open_img, 
                                                    command=lambda text_string=text_string, key=key : self.open_directory_location(text_string, key))
                    button.grid(row=rValue, 
                                column=cValue+2, 
                                sticky='e', 
                                padx=(self.horizontal_spacing, self.horizontal_spacing), 
                                pady=self.vertical_spacing)                    
                    text_entry.grid_propagate(False)

                elif label_key_list[-1].lower() == 'file':
                    text_entry          = tk.Entry(label_frame, 
                                                   textvariable=text_string, 
                                                   width=self.entry_widget_width,
                                                   borderwidth=self.entry_widget_border,
                                                   state='disabled',
                                                   font=self.entry_font_text, 
                                                   fg=self.str_entry_text_color, 
                                                   bg=self.str_entry_box_color)
                    
                    text_entry.grid(row=rValue, 
                                    column=cValue + 1, 
                                    sticky='e', 
                                    padx=(self.horizontal_spacing, self.horizontal_spacing), 
                                    pady=self.vertical_spacing, columnspan=2) 
                    
                    button              = tk.Button(label_frame, 
                                                    image=self.open_img, 
                                                    command=lambda text_string=text_string, key=key : self.open_file_location(text_string, key))
                    button.grid(row=rValue, 
                                column=cValue+2, 
                                sticky='e', 
                                padx=(self.horizontal_spacing, self.horizontal_spacing), 
                                pady=self.vertical_spacing)                    
                    text_entry.grid_propagate(False)                    

                else:
                    text_entry          = tk.Entry(label_frame, 
                                                   textvariable=text_string,
                                                   width=self.entry_widget_width,
                                                   borderwidth=self.entry_widget_border,
                                                   font=self.entry_font_text, 
                                                   fg=self.str_entry_text_color, 
                                                   bg=self.str_entry_box_color)
                    
                    text_entry.grid(row=rValue, 
                                    column=cValue + 1, 
                                    sticky='e', 
                                    padx=(self.horizontal_spacing, self.horizontal_spacing), 
                                    pady=self.vertical_spacing, 
                                    columnspan=2)                       


                    text_entry.grid_propagate(False)

            # print comma separated values if it is a list
            elif isinstance(value, list):

                # set the lable name of the input data string
                label_key               = key.replace("_", " ")
                label                   = tk.Label(label_frame, 
                                                   text=label_key.title(),
                                                   font=self.label_font_text, 
                                                   bg=self.travis_window_color,
                                                   fg=self.label_font_color)
                label.grid(row=rValue, 
                           column=cValue, 
                           padx=(self.horizontal_spacing, self.entry_widget_width-len(key)), 
                           pady=self.vertical_spacing, 
                           sticky='w')

                # set string data in entry panel
                text_string             = tk.StringVar()
                text_string.trace('w', lambda name, 
                                 index, 
                                 mode, 
                                 text_string=text_string : self.update_yaml_data(text_string, self.first_option_selected, self.second_option_selected, name, key))

                text_string.set(','.join(str(list_data).strip() for list_data in value))

                text_entry              = tk.Entry(label_frame, 
                                                   textvariable=text_string, 
                                                   width=self.entry_widget_width,
                                                   borderwidth=self.entry_widget_border,
                                                   font=self.entry_font_text, 
                                                   fg=self.list_entry_text_color, 
                                                   bg=self.list_entry_box_color)
                text_entry.grid(row=rValue, 
                                column=cValue + 1, 
                                sticky='e', 
                                padx=(self.horizontal_spacing, self.horizontal_spacing), 
                                pady=self.vertical_spacing, 
                                columnspan=2)
                text_entry.grid_propagate(False)


            label_frame.update()
            rValue += 1

        self.submit_button              = tk.Button(self.entry_frame, image=self.btn_img, command=self.process_request)
        self.submit_button.grid(row=5, 
                                column=0, 
                                sticky='sw', 
                                padx=(initial_horizontal_padding, horizontal_padding),
                                pady=(initial_vertical_padding, vertical_padding))
        # self.submit_button.config(state='disabled')            


    def open_directory_location(self, text_string, key) -> 'None':
        """ Opens up file dialog for user to select the folder for input and output """

        logging.info(friday_reusable.get_function_name())

        # print (text_string, key, self.first_option_selected, self.second_option_selected)
        title = f"Select {key}"
        location = filedialog.askdirectory(initialdir='./', title=title)

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
        self.menubar = tk.Menu(self)

        self.file_menu = tk.Menu(self.menubar, tearoff=False)

        self.file_menu.add_command(label='Show Log', command=lambda: self.show_log_file())
        self.file_menu.add_command(label='Show Workspace', command=lambda : self.show_location())
        self.file_menu.add_separator()
        self.file_menu.add_command(label='Reset GUI', command=self.reset_gui)
        self.file_menu.add_command(label='Clean Workspace', command=lambda : friday_reusable.purge_workspace_folders(current_location = self.mypath))
        self.file_menu.add_separator()
        self.file_menu.add_command(label='Exit', command=self.destroy)

        # add the File menu to the menubar
        self.menubar.add_cascade(label="File", menu=self.file_menu)

        # create the Help menu
        self.help_menu = tk.Menu(self.menubar, tearoff=0)

        self.help_menu.add_command(label='About', command=lambda:self.show_travis())

        # add the Help menu to the menubar
        self.menubar.add_cascade(label="Help", menu=self.help_menu)
        
        self.config(menu=self.menubar)


    def show_travis(self) -> 'None':
        """ Show travis popup window with details """

        logging.info(friday_reusable.get_function_name())

        self.about              = tk.Toplevel(self)

        screen_width            = self.winfo_screenwidth()
        screen_height           = self.winfo_screenheight()

        self.about.geometry(f"350x200+{screen_width//2 - 350//2}+{screen_height//2 - 200//2}")        
        self.about.title("TRAVIS About")
        travis_png_bytes        = base64.b64decode(self.travis_icon.encode())
        travis_img              = io.BytesIO(travis_png_bytes)
        self.travis_logo        = ImageTk.PhotoImage(Image.open(travis_img))
        ttk.Label(self.about, image=self.travis_logo).pack()
        tk.Label(self.about, text="By Deloitte Version 1.0").pack(pady=15)


    def show_log_file(self) -> 'None':
        """ Open log files from Menu option """

        logging.info(friday_reusable.get_function_name())

        log = os.path.join(self.mypath, 'Travis.log')
        show_notepad = 'notepad.exe %s' %(log)
        target = os.system(show_notepad)  


    def show_location(self) -> 'None': 
        """ Show workspace location """

        logging.info(friday_reusable.get_function_name())

        file_path = os.path.join(os.getenv('WINDIR',""), 'explorer.exe')

        # explorer would choke on forward slashes
        path = os.path.normpath(self.mypath)

        if os.path.isdir(path):
            subprocess.run([file_path, path])

        elif os.path.isfile(path):
            subprocess.run([file_path, '/select,', os.path.normpath(path)])


    # reset gui is not working at this moment
    def reset_gui(self) -> 'None':
        """ Destory widgets in the entry frame """

        logging.info(friday_reusable.get_function_name())
        
        # destory child widgets of entry frame if any                           
        child_widgets = self.entry_frame.winfo_children()

        # check if more than one child widget present on entry frame
        if len(child_widgets) > 0: 
            for widget in child_widgets:
                widget.destroy()


    # Log error message and display
    def log_error_message(self, valid_indicator, message):
        '''LOG ERROR MESSAGE AND EXIT FROM THE APPLICATION'''
        
        logging.info(friday_reusable.get_function_name())
        logging.info('ERROR MESSAGE FOR: '+ str(valid_indicator) + ' MESSAGE PASSED '+ str(message))
        
        if not valid_indicator:
            logging.error(message)
            messagebox.showinfo('F.R.I.D.A.Y', message)
            return

    # Log error message and display
    def log_exception_message(self, message):
        '''LOG ERROR MESSAGE AND EXIT FROM THE APPLICATION'''
        
        logging.info(friday_reusable.get_function_name())
        logging.critical('CRITICAL EXCEPTION ' + str(message))
        messagebox.showinfo('F.R.I.D.A.Y', 'CRITICAL ERROR OCCURED. PLEASE CHECK LOG FILE')
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
                                                                              col_nbr, 
                                                                              self.app_config))
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
                                                                             progress_bar,
                                                                             col_nbr))
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
                                                                           progress_bar,
                                                                           col_nbr))
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


    def create_progress_bar(self) -> 'tuple[tk.Label, tk.Label, ttk.Progressbar, int]':
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
        current_process_label       = tk.Label(self.submit_frame, 
                                               text=str(self.second_option_selected), 
                                               bg=self.travis_header_color, 
                                               fg=self.entry_font_color)
        current_process_label.grid(row=row_nbr, column=col_nbr, padx=5, pady=2, sticky='w')

        # get real time progress on this label 
        progress_label              = tk.Label(self.submit_frame, 
                                               text=str(self.second_option_selected), 
                                               bg=self.travis_header_color, 
                                               fg=self.entry_font_color)
        progress_label.grid(row=row_nbr+2, column=col_nbr, padx=5, pady=2, sticky='w')

        # get real time chaging progress bar on this 
        progress_bar                = ttk.Progressbar(self.submit_frame, 
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


    # @staticmethod
    # def compare_file_metadata(root, configuration, first_option_selected, second_option_selected, mypath, progress_label, gui_config, application_name, environment_name, current_process_label, progress_bar, col_nbr) -> 'None':
    #     """ Process JSON compare request """

    #     logging.info(friday_reusable.get_function_name())

    #     try:
    #         compare_file              = CompareMetaData(configuration, 
    #                                                     first_option_selected, 
    #                                                     second_option_selected, 
    #                                                     mypath, 
    #                                                     progress_label, 
    #                                                     gui_config, 
    #                                                     application_name, 
    #                                                     environment_name)
    #         # compare_file.show()
    #         message = compare_file.compare_files_metadata()
    #         messagebox.showinfo('FRIDAY', message)       

    #     except ValidationException as e: 
    #         master.log_error_message(root, False, e)

    #     except ProcessingException as e: 
    #         master.log_error_message(root, False, e)           

    #     except Exception as e: 
    #         master.log_exception_message(root, e)   
        
    #     # check if successful then show success message
    #     master.destroy_progress_bar(root, progress_label, progress_bar, current_process_label, col_nbr, second_option_selected)


    @staticmethod
    def stream_and_compare_json_files(root, configuration, first_option_selected, second_option_selected, mypath, progress_label, gui_config, application_name, environment_name, current_process_label, progress_bar, col_nbr, app_config=None) -> 'None':
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

            elif second_option_selected == 'JSON_Cross_Compare':
                compare_json        = JsonCrossCompare(configuration, 
                                                         first_option_selected, 
                                                         second_option_selected, 
                                                         mypath, 
                                                         progress_label, 
                                                         gui_config, 
                                                         application_name, 
                                                         environment_name, 
                                                         app_config)
                
                message = compare_json.perform_json_cross_compare()                

            messagebox.showinfo('FRIDAY', message)       

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

            messagebox.showinfo('FRIDAY', message)       

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

            messagebox.showinfo('FRIDAY', message)       

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

            messagebox.showinfo('FRIDAY', message)       

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

            messagebox.showinfo('FRIDAY', message)       

        except ValidationException as e: 
            master.log_error_message(root, False, e)

        except ProcessingException as e: 
            master.log_error_message(root, False, e)           

        except Exception as e: 
            master.log_exception_message(root, e)   
        
        # check if successful then show success message
        master.destroy_progress_bar(root, progress_label, progress_bar, current_process_label, col_nbr, second_option_selected)


    # @staticmethod
    # def process_mongo_request(root, configuration, first_option_selected, second_option_selected, mypath, progress_label, gui_config, application_name, environment_name, current_process_label, progress_bar, col_nbr, app_config) -> 'None':
    #     """ Process Mongo migration request """
       
    #     try: 
    #         message = ""
    #         mongo_stats         = MongoUtilities(configuration,
    #                                              first_option_selected,
    #                                              second_option_selected, 
    #                                              mypath, 
    #                                              progress_label, 
    #                                              gui_config,
    #                                              application_name, 
    #                                              environment_name, 
    #                                              app_config,
    #                                              'CSV_COMPARE', 
    #                                              'CSV_Dynamic_Compare')
            
    #         message             = mongo_stats.perform_mongo_operations()

    #         messagebox.showinfo('FRIDAY', message)       

    #     except ValidationException as e: 
    #         master.log_error_message(root, False, e)

    #     except ProcessingException as e: 
    #         master.log_error_message(root, False, e)           

    #     except Exception as e: 
    #         master.log_exception_message(root, e)   
        
    #     # check if successful then show success message
    #     master.destroy_progress_bar(root, progress_label, progress_bar, current_process_label, col_nbr, second_option_selected)        


    # @staticmethod
    # def process_migration_request(root, configuration, first_option_selected, second_option_selected, mypath, progress_label, gui_config, application_name, environment_name, current_process_label, progress_bar, col_nbr, app_config) -> 'None':
    #     """ Process Migration request """ 

    #     try: 
    #         message = ""
    #         aws_object          = MigrationUtilities(configuration,
    #                                                      first_option_selected,
    #                                                      second_option_selected, 
    #                                                      mypath, 
    #                                                      progress_label, 
    #                                                      gui_config,
    #                                                      application_name, 
    #                                                      environment_name)
    #         message             = aws_object.perform_aws_operation()
            
    #         messagebox.showinfo('FRIDAY', message)       

    #     except ValidationException as e: 
    #         master.log_error_message(root, False, e)

    #     except ProcessingException as e: 
    #         master.log_error_message(root, False, e)           

    #     except Exception as e: 
    #         master.log_exception_message(root, e)   
        
    #     # check if successful then show success message
    #     master.destroy_progress_bar(root, progress_label, progress_bar, current_process_label, col_nbr, second_option_selected)        


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