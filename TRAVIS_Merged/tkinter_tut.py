import tkinter as tk

class MyGUI:

    def __init__(self):
        self.root = tk.Tk()
        self.label = tk.Label(self.root, text = "something", font=('Arial',18), padx=122, pady=122)
        self.label.grid()




        self.root.mainloop()

MyGUI()