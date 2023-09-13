''' 
    Created By: Rohit Abhishek 
    Function: Show the launch window and validate the token entered by the user. 
              Get the yaml file and create workspace for the user. 
'''

import functools
import multiprocessing
import os
import sys
import time
import tkinter as tk
from tkinter import messagebox, ttk

import friday_reusable
import ttkbootstrap as tkb
import yaml
from friday_constants import (CONFIG_FILE_NAME, FILE_TYPES, LOG_FILE_NAME,
                              MESSAGE_LOOKUP, SPECIAL_CHARACTERS,
                              STATIC_FOLDER_NAME, TEMPLATE_FOLDER_NAME)
from PIL import Image, ImageTk


class FridayConfig(tkb.Toplevel): 
    ''' Splashscreen class to show animation and load configurations for app to start up '''

    def __init__(self, root, **kwargs): 

        # implement init method of tkinter top level method 
        tkb.Toplevel.__init__(self, root, **kwargs)
        
        # mark self to variables 
        self.root = root 
        self.elements = {}

        # hide this window 
        root.withdraw()

        # make window non resizable and without bars 
        self.overrideredirect(True)
        self.resizable(False, False)

        # configure row and column 
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        # Placeholder Vars that can be updated externally to change the status message
        self.init_str = tkb.StringVar()
        self.init_str.set('Loading...')

        # get current wolring directory location
        self.pwd = os.path.dirname(os.path.abspath(__file__))
        self.valid_date = None

        # create a gif image object
        self.gif_file = os.path.join(self.pwd, 'static', 'Travis.gif')

        self.gif_info = Image.open(self.gif_file)     
        self.gif_frames = self.gif_info.n_frames   
        self.gif_list = [tkb.PhotoImage(file=self.gif_file, format=f'gif -index {i}') for i in range(self.gif_frames)]
        self.init_image = self.gif_list[0]

        self.screen = FridayConfig.Screen(self)
        self._position()

        # Add gif image to the label 
        self.gif_img_lbl = tkb.Label(self.screen, image=self.init_image, bootstyle="default")
        self.gif_img_lbl.grid(column=0, row=0, sticky='nswe')

        # Connects to the tk.StringVar so we can updated while the startup process is running
        self.label = tkb.Label(self.screen, textvariable=self.init_str, anchor='center', bootstyle="default")
        self.label.grid(column=0, row=1, sticky='nswe')

        self.count = 0
        self.started = False

    def _position(self, xfact=0.5, yfact=0.5): 
        ''' create a place where you want to position your widget '''

        # splash window size 
        splash_width = 300
        splash_height = 200

        # get the dimension of the window 
        screen_width = self.winfo_screenwidth()
        screen_height = self.winfo_screenheight()

        # get the cordinates for splash window 
        cord_x = (screen_width * xfact) - (splash_width * xfact)
        cord_y = (screen_height * yfact) - (splash_height * yfact)

        # self.wm_title("TRAVIS IS LOADING")
        self.geometry('%dx%d+%d+%d' %(splash_width, splash_height, cord_x, cord_y))


    def update(self, thread_queue=None):
        
        ''' update method to update the parent object and check for any threads ''' 

        # update the splashscreen class 
        super().update()

        # get the text put on thread queue 
        if thread_queue and not thread_queue.empty():
            new_item = thread_queue.get()

            # parse the new_item from the thread queue 
            index, message, return_object = new_item[0], str(new_item[1]), str(new_item[2])

            print ("Getting", index, message)

            # change the content of the text appearing on the screen
            if message and message != self.init_str.get():
                self.init_str.set(message)
            
            if index == 1:
                self.config = return_object
            elif index == 2:
                self.pwd = return_object
            elif index == 3:
                self.valid_date = return_object
            
            # check if already started. If not start it and forget it 
            if not self.started: 
                self.update_img(self.count)
                self.started = True


    def get_config(self): 

        return self.pwd, self.config, self.valid_date

    def update_img(self, count): 

        # update the splashscreen class 
        super().update()

        self.gif_img_lbl.configure(image=self.gif_list[self.count])

        self.count += 1

        if self.count == self.gif_frames: 
            self.count = 0

        self.after(100, lambda : self.update_img(self.count))


    # declare a static method within the Splashscreen class for calling it without instantiating the object
    @staticmethod
    def show(root, function, static_folder=None, config_file=None, callback=None, position=None, **kwargs): 
        ''' create threads and splash screen for projecting the message '''

        # create a multi processing manager and queue
        manager = multiprocessing.Manager()
        thread_queue = manager.Queue()

        # startup the multiprocessing with thread pool
        process_startup = multiprocessing.Process(target=functools.partial(function,
                                                                           thread_queue=thread_queue, 
                                                                           static_folder=static_folder, 
                                                                           config_file=config_file))
        process_startup.start()

        # instantiate splashscreen object
        splash = FridayConfig(root=root, **kwargs)

        # check if the threads are still alive. If so update the splash screen with text message
        while process_startup.is_alive():
            splash.update(thread_queue)

        pwd, config, valid_date = splash.get_config()

        # terminate the process 
        process_startup.terminate()

        # remove splashscreen from the display 
        FridayConfig.remove_splash_screen(splash, root)

        # if callback is set, return callback else nothing
        return pwd, config, valid_date


    # destroy the splash screen 
    @staticmethod
    def remove_splash_screen(splash, root):
        splash.destroy()
        del splash
        root.deiconify()        

    # child class for splash screen frame
    class Screen(tk.Frame):

        # Options screen constructor class
        def __init__(self, parent):
            tk.Frame.__init__(self, master=parent)
            self.grid(column=0, row=0, sticky='nsew')
            self.columnconfigure(0, weight=1)
            self.rowconfigure(0, weight=1)


def startup_process(thread_queue, static_folder, config_file):

    # print (static_folder, config_file)

    # Just a fun method to simulate loading processes
    startup_messages = ["Please wait...","Getting Configurations","Creating Workspace","Validity Check","Almost Done"]

    for i, n in enumerate(startup_messages):
        if i == 0:
            thread_queue.put([n, i, ""])
        elif i == 1: 
            config_data = friday_reusable.get_config_data(config_file)
            print ("data read")
            thread_queue.put([n, i, config_data])
        elif i == 2: 
            gui_config = config_data["TravisConfig"]["gui_config"]
            workspace_directory = gui_config.get("workspace_directory", None)
            workspace_directory = friday_reusable.setup_user_workspace(workspace_directory)
            thread_queue.put([n, i, workspace_directory])
        elif i == 3:
            travis_key = gui_config.get("travis_key")
            travis_file_object = friday_reusable.TravisData(travis_key, os.path.join(static_folder, "travis.dat"))
            valid_indicator, fernet_object, valid_date = travis_file_object.validate_travis_file()

            # validate return code from the Fenert 
            if not valid_indicator:
                messagebox.showerror("TRAVIS.", (MESSAGE_LOOKUP.get(5)) %("Token", "TRAVIS Support"))
                sys.exit()                

            thread_queue.put([n, i, valid_date])

        elif i==4:
            thread_queue.put([n, i, "Done"])

        print ("putting", [n, i])

        time.sleep(.5)