"""
    This is a landing page document where we have option for TRAVIS1 & 2 user to select and launch. This is a simple GUI application. 
"""

import base64
import io
import os
import pathlib
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from multiprocessing import Process, get_context
from tkinter import filedialog, messagebox, ttk

import friday_reusable
import ttkbootstrap as tkb
from cryptography.fernet import Fernet
from friday_constants import (CONFIG_FILE_NAME, HOME_PAGE_TITLE,
                              STATIC_FOLDER_NAME, TEMPLATE_FOLDER_NAME)
from idlelib.tooltip import Hovertip
from PIL import Image, ImageTk


class HomePage(tkb.Window):

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)

        # get configuration filename and static directory location
        self.__config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                               CONFIG_FILE_NAME) 
        self.__static_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                               STATIC_FOLDER_NAME)  

        # get token validity 
        self.__get_configuration_data()
        current_date, start_date, days_used, valid_days = friday_reusable.get_travis_tokens(self.__travis_key, 
                                                                                            os.path.join(self.__static_directory, "travis.dat"))
        start_date_formatted = datetime.fromtimestamp(float(start_date))
        self.__till_date = (start_date_formatted+timedelta(days=float(valid_days))).strftime("%b %d, %Y")

        # set up GUI 
        self.title(HOME_PAGE_TITLE)
        self.geometry('900x350')
        self.grid_columnconfigure(0,weight=1)
        self.iconphoto(False, ImageTk.PhotoImage(self.__travis_image_resize))
        self.iconphoto(True, ImageTk.PhotoImage(self.__travis_image_resize))

        # get user details         
        self.__user=os.getlogin()
        self.__populate_gui()


    def __populate_gui(self):

        # create labels and grid them on the window 
        deloitte_label = tkb.Label(self, image=self.__travis_deloitte_dlogo_image)
        deloitte_label.grid(row=0, rowspan=3, column=0, sticky="w", padx=10)

        user_label = tkb.Label(self, text=f"User: {self.__user}")
        user_label.grid(row=0, column=1, columnspan=2, sticky='e', padx=10)

        validity = tkb.Label(self, text=f"TRAVIS Validity Till: {self.__till_date}")
        validity.grid(row=1, column=1, columnspan=2, sticky='e', padx=10)

        # add a separator 
        separator = tkb.Separator(self)
        separator.grid(row=4, column=0, columnspan=3, padx=(10,10), pady=(20, 20), sticky="ew")        

        # Add select option label 
        selection_label = tkb.Label(self, text="Select Option: ", font="-weight bold")
        selection_label.grid(row=5, column=0, sticky="w", padx=10)

        # Add new radio buttons 
        self.option_selected = tkb.StringVar()  
        r1 = tkb.Radiobutton(self, text='Migration Quality Studio', value='TRAVIS1', variable=self.option_selected)
        r1.grid(row=6,column=0,pady=5,sticky='w',padx=(10,0))

        r2 = ttk.Radiobutton(self, text='Data Quality Studio', value='TRAVIS2', variable=self.option_selected)
        r2.grid(row=7,column=0,sticky='w',pady=5,padx=(10,0))

        r3 = ttk.Radiobutton(self, text='CATE', value='CATE', variable=self.option_selected)
        r3.grid(row=8,column=0,sticky='w',pady=5,padx=(10,0))

        self.open_tool = tkb.Button(
            self ,
            text='Open Selected Tool',
            command=self.__open_travis_tool
            )

        self.open_tool.grid(row=9, column=0, padx=10, pady=20, sticky="w")
        # Add a submit/Go button
    

    def __get_configuration_data(self):

        # get configurations from the ymal file 
        self.__config_data = friday_reusable.get_config_data(self.__config_file)
        self.__gui_config = self.__config_data["TravisConfig"]["gui_config"]
        self.__image_config = self.__config_data["TravisConfig"]["image_config"]
        
        # now get the deloitte image data 
        self.__travis_deloitte_dlogo_bstream = self.__image_config.get("deloitte_d_logo", None)
        self.__travis_deloitte_dlogo_image = ImageTk.PhotoImage(self.__load_deloitte_dlogo_image())

        # get travis image data 
        self.__travis_bstream = self.__image_config.get("travis_logo", None)
        self.__travis_image = self.__load_travis_image()
        self.__travis_image_resize              = self.__travis_image.resize((256,256))

        # get travis key 
        self.__travis_key = self.__gui_config.get("travis_key", None)



    def __load_deloitte_dlogo_image(self) -> Image:
        """ Load submit button image """
        travis_dlogo_bytes                    = base64.b64decode(self.__travis_deloitte_dlogo_bstream.encode())
        travis_dlogo_stream                   = io.BytesIO(travis_dlogo_bytes)
        travis_dlogo_pil                      = Image.open(travis_dlogo_stream)

        return travis_dlogo_pil
    

    def __load_travis_image(self) -> Image: 
        """ Load travis image """
        travis_png_bytes                    = base64.b64decode(self.__travis_bstream.encode())
        travis_img_stream                   = io.BytesIO(travis_png_bytes)
        travis_pil                          = Image.open(travis_img_stream)

        return travis_pil    
    

    def open_travis1_py(self):
        fullpath=os.path.join(pathlib.Path(__file__).parent.resolve(),'friday_launch_oop.py')
        print (os.getcwd(), os.path.dirname(__file__), pathlib.Path(__file__).parent.resolve())
        subprocess.run(["python", fullpath])

    def open_travis2_py(self):
        fullpath=os.path.join(pathlib.Path(__file__).parent.resolve(),'travis2.py')
        subprocess.run(["python", fullpath])

    def open_travis1_exe(self):
        subprocess.run(["friday_launch_oop.exe"])

    def open_travis2_exe(self):
        subprocess.run(["TRAVIS2.exe"])        


    def __open_travis_tool(self):
        
        # check if current directory has py files or exe files 
        file_list = os.listdir(os.path.dirname(os.path.abspath(__file__)))
 
        if any(i in file_list for i in ["friday_launch_oop.py", "travis2.py"]):
            if self.option_selected.get() == 'TRAVIS1':
                x = threading.Thread(target=self.open_travis1_py)
                x.start()
                
            elif self.option_selected.get()=='TRAVIS2':
                x = threading.Thread(target=self.open_travis2_py)
                x.start()
        else:
            if self.option_selected.get() == 'TRAVIS1':
                x = threading.Thread(target=self.open_travis1_exe)
                x.start()
                
            elif self.option_selected.get()=='TRAVIS2':
                x = threading.Thread(target=self.open_travis2_exe)
                x.start()            

if __name__ == "__main__":
    app = HomePage()
    app.mainloop()
        


