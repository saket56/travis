import tkinter as tk
from tkinter import ttk
import subprocess
import os
import threading
import time
from datetime import datetime, timedelta
from tkinter import messagebox
import sys
from cryptography.fernet import Fernet
import pathlib

from TRAVIS_Core.friday_launch_oop import *
from TRAVIS_EDA.travis_eda import *
from idlelib.tooltip import Hovertip



class HomePage(tk.Tk):

    def __init__(self):
        super().__init__()
        
        till_date=self.check_using_token()
        # self=tk.Tk()
        self.title('TRAVIS Home Page')
        self.geometry('550x200')
        # screen_width = self.winfo_screenwidth()
        # screen_height = self.winfo_screenheight()
        # self.geometry('{w}x{h}+0+0'.format(w=screen_width, h=screen_height))
        self.grid_columnconfigure(0,weight=1)
        self.configure(bg="#0C7A79")
        self.fontSize=9
        
        
        user=os.getlogin()
        tk.Label(self, text=" User: "+user,bg="#0C7A79",fg="yellow", font=("Arial", self.fontSize+1)).grid(row=0,columnspan=2,padx=(100,0),sticky='e')
        tk.Label(self, text=" Valid Till: "+till_date,bg="#0C7A79",fg="yellow", font=("Arial", self.fontSize)).grid(row=1,columnspan=2,padx=(100,0),sticky='e')
        tk.Label(self, text=" Select Option: ",bg="#0C7A79",fg="yellow", font=("Arial", self.fontSize+3)).grid(row=1,columnspan=2,padx=(10,0),sticky='w')

        # tk.Label(self, text=" ",bg="#0C7A79",fg="yellow", font=("Arial", self.fontSize+3)).grid(row=2,padx=(100,0),sticky='w')

        self.selected = tk.StringVar()
    

        r1 = ttk.Radiobutton(self, text='Migration Quality Studio  ' , value='Migration Quality Studio', variable=self.selected).grid(row=3,column=0,pady=5,sticky='w',padx=(10,0))
        tk.Label(self, text="Json and CSV files comparisons / File Manipulation at high speed ",bg="#0C7A79",fg="white", font=("Arial", self.fontSize)).grid(row=3,column=1,padx=(0,0),sticky='w')

        r2 = ttk.Radiobutton(self, text='Data Quality Studio            ', value='Data Quality Studio', variable=self.selected).grid(row=4,column=0,sticky='w',pady=5,padx=(10,0))
        tk.Label(self, text="Cloud migration testing / Data migration Testing / Data Quality check ",bg="#0C7A79",fg="white", font=("Arial", self.fontSize)).grid(row=4,column=1,padx=(0,0),sticky='w')

        ttk.Radiobutton(self,      text='CATE                                     ', value='CATE', variable=self.selected).grid(row=5,column=0,sticky='w',pady=5,padx=(10,0))
        tk.Label(self, text="Cloud Analytics Implementation / Enterprise Data Lake Validation",bg="#0C7A79",fg="white", font=("Arial", self.fontSize)).grid(row=5,column=1,padx=(0,0),sticky='w')

        self.open_tool = tk.Button(
            self ,
            text='Open Selected Tool ',
            command=self.selectTestMethod,
            height= 1, width=25, font=("Arial", self.fontSize)
            )

        self.open_tool.grid(row=6,columnspan=2,padx=(70,0),pady=5)
        # Add a submit/Go button
   



 


    def getStringFromDB(self):
        try:
            with open('token.dat','r') as f:
                return f.read()
        except:
            messagebox.showerror('Error',' Token Not Found')
            sys.exit()

     


    def check_using_token(self):
        try:
            ret=self.getStringFromDB()
            encrypted_string=bytes(ret, 'utf-8')
            key=b'3i60NGv2MC7jAyGPt9ownm-g_3W8KapWAReESdJlXBA='
            f = Fernet(key)

            # Decrypt the encrypted string
            decrypted_string = f.decrypt(encrypted_string)
            # Print the original and decrypted strings
            start_date,days_valid=decrypted_string.decode().split('#')
            
            curr_date=time.time()
            dt1 = datetime.datetime.fromtimestamp(float(start_date))
            dt2 = datetime.datetime.fromtimestamp(curr_date)
            d=dt2-dt1
            hrs=d.total_seconds()/(60*60)
            days_used=hrs/24
            
            if(float(curr_date)<float(start_date)):
                messagebox.showerror('Error','Token Will be valid from later date.')
                sys.exit()

            if(days_used>float(days_valid)):
                messagebox.showerror('Error','Token Expired, contact Deloitte Travis Team for new token.')
                sys.exit()

            return (dt1+timedelta(days=float(days_valid))).strftime("%b %d, %Y")


            
        except Exception as err:
             
            print('Token Error')
            sys.exit()



    def open_travis1(self):
        fullpath=os.path.join(pathlib.Path(__file__).parent.resolve(),'TRAVIS_Core\\friday_launch_oop.py')
        subprocess.run(["python", fullpath])
        print(fullpath)

    def open_travis2(self):
        fullpath=os.path.join(pathlib.Path(__file__).parent.resolve(),'TRAVIS_EDA\\travis_eda.py')
        subprocess.run(["python", fullpath])
        print(fullpath)


    def selectTestMethod(self):
        # exec(open('friday_launch.py'))
        print(os.getcwd())
        print(pathlib.Path(__file__).parent.resolve())
        
        
        if self.selected.get()=='Migration Quality Studio':
            
            x = threading.Thread(target=self.open_travis1)
            x.start()
            
        elif self.selected.get()=='Data Quality Studio':
            
            x = threading.Thread(target=self.open_travis2)
            x.start()
            # subprocess.run(["python", fullpath])

        
        

        
 


if __name__ == "__main__":
  app = HomePage()
#   app.setDaemon(True)
 
  app.mainloop()
        


