from PIL import Image

logo = Image.open("C:\\Users\\roabhishek\\Documents\\PythonWorkspace\\TRAVIS\\TRAVIS_App\\TRAVIS_Merged\\static\\icon.png")
logo.save("C:\\Users\\roabhishek\\Documents\\PythonWorkspace\\TRAVIS\\TRAVIS_App\\TRAVIS_Merged\\static\\icon.ico", format='ICO', sizes=[(40,40)])