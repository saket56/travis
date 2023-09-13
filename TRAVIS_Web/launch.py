import datetime
import itertools
import json
import logging
import os
import queue
import shutil
import time
from distutils.log import debug
from fileinput import filename

from flask import (Flask, Response, flash, render_template, request,
                   send_from_directory, url_for)
# import packages for flask sqlalchemy 
from flask_sqlalchemy import SQLAlchemy
# import packages from Flask's What the forms
from flask_wtf import FlaskForm
from wtforms import MultipleFileField, StringField, SubmitField, SelectField, RadioField, FieldList, BooleanField
from wtforms.validators import DataRequired, InputRequired

import friday_reusable 
from friday_process import CompareMetaData

# instantiate the Flask application 
app = Flask(__name__)

# create a secret key for web forms 
app.config["SECRET_KEY"] = "OAI Web Interface"

# create current directory location configuration
app.config["CURRENT_DIRECTORY"] = os.path.dirname(__file__)

# create SQLite database location 
database_location = os.path.join(app.config["CURRENT_DIRECTORY"], "instance", "travis_webapp.db")
app.config["SQLALCHEMY_DATABASE_URI"] = f'sqlite:///{database_location}'

# provide the reference of app to SQLAlchemy and intitalize the database
db = SQLAlchemy(app)
app.app_context().push()

# create database model (table)
class TravisMaster(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    workspace = db.Column(db.String(200), nullable=False)
    operation_performed = db.Column(db.String(100), nullable=False)
    date_added = db.Column(db.DateTime, default=datetime.datetime.utcnow())

# create metadata compare form 
class MetadataCompare(FlaskForm):
	base_files = MultipleFileField("Base Files", validators=[InputRequired()])
	release_files = MultipleFileField("Release Files", validators=[InputRequired()])
	file_keys = StringField("File Keys", validators=[DataRequired()])
	include_matching = BooleanField("Include Matching")
	submit = SubmitField("Submit")

class Other(FlaskForm):
	pass


def setup_logger(name, log_file, formatter, level=logging.INFO):
	""" set up logger for logging the application and operation logs """

	# create folder if does not exists
	if not os.path.exists(os.path.dirname(log_file)):
		os.makedirs(os.path.dirname(log_file))
	handler = logging.FileHandler(log_file)
	handler.setFormatter(formatter)
	logger = logging.getLogger(name)
	logger.setLevel(level)
	logger.addHandler(handler)

	return logger

# Create server logger
formatter = logging.Formatter('%(asctime)s - {%(name)s : %(lineno)d} - %(levelname)s - %(message)s')
server_logger = setup_logger("server_logger", os.path.join(app.config["CURRENT_DIRECTORY"], "logs", "travis_server.log"), formatter)

# create database object at first request
# @app.before_first_request
# def create_tables():
#     db.create_all()

with app.app_context():
	db.create_all()

config = friday_reusable.get_config_data()

# create a form object dictionary 
form_object = {"Metadata_Compare" : [MetadataCompare, "metadata_compare.html", "Compare_Report.html"], 
	       "Other" : [Other, "other.html"]}


# create routing for home page
@app.route('/')
def main():
	return render_template('index.html', config=config)

@app.route("/selection/<parent_key>/<child_key>", methods=["POST", "GET"])
def selection(parent_key, child_key):
	form = None 

	# get the form and template 
	form_name, template_name, report_name = get_template_form(parent_key, child_key)
	form = form_name()

	# check the form if is submitted 
	if form.validate_on_submit():
		app_config = config[parent_key][child_key]
		template_directory = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "reports")
		deloitte_image = config["TravisConfig"]["image_config"]["deloitte_logo"]
		travis_image = config["TravisConfig"]["image_config"]["travis_logo"]
		application_name = "Metadata_Compare"
		environment_name = "Test"
		run_id = os.path.basename(mypath)
		travis_status_queue = queue.Queue()
		progress_treeview = None

		compare_file = CompareMetaData(app_config, 
				 parent_key, 
				 child_key, 
				 mypath, 
				 template_directory, 
				 deloitte_image, 
				 travis_image,
				 application_name,
				 environment_name,
				 run_id,
				 travis_status_queue,
				 progress_treeview)
		
		message = compare_file.compare_files_metadata()

		print (message)

		# return render_template(report_name, config=config, parent_key=parent_key, child_key=child_key, form=form)


	# get the configuration for selected child element 
	return render_template(template_name, config=config, parent_key=parent_key, child_key=child_key, form=form)



def get_template_form(parent_key, child_key):

	# get the configuration for given parent and child key 
	if child_key == "Metadata_Compare":
		form_name, template_name, report_name = form_object.get(child_key)
	else:
		form_name, template_name, report_name = form_object.get("Other")

	return form_name, template_name, report_name

if __name__ == '__main__':

	mypath = friday_reusable.setup_user_workspace()

	app.run(debug=True)