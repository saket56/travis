import datetime
import os
from flask import Flask, request, make_response, render_template
from flask_sqlalchemy import SQLAlchemy
from flask_wtf import FlaskForm
from wtforms import MultipleFileField, StringField, SubmitField, SelectField, RadioField, FieldList, BooleanField
from wtforms.validators import DataRequired, InputRequired
import logging
import friday_reusable


# create metadata compare form 
class MetadataCompare(FlaskForm):
	base_files = MultipleFileField("Base Files", validators=[InputRequired()])
	release_files = MultipleFileField("Release Files", validators=[InputRequired()])
	file_keys = StringField("File Keys", validators=[DataRequired()])
	include_matching = BooleanField("Include Matching")
	submit = SubmitField("Submit")


class Other(FlaskForm):
	pass

class EndpointHandler(object):
    """ Create endpoint object for various urls """

    def __init__(self, action):
        self.action = action

    def __call__(self, *args, **kwargs):
        response = self.action(*args, **request.view_args)
        return make_response(response)
    

class FlaskAppWrapper(object):

    def __init__(self, app, **configs):
        self.app = app 

        # add the configurations 
        self.app.config["SQLALCHEMY_DATABASE_URI"] = f'sqlite:///travis_webapp.db'
        app.config["SECRET_KEY"] = "OAI Web Interface"

        # create database         
        self.db = SQLAlchemy(self.app)
        self.app.app_context().push()

        # create database model 
        class TravisMaster(self.db.Model):
            id = self.db.Column(self.db.Integer, primary_key=True)
            workspace = self.db.Column(self.db.String(200), nullable=False)
            operation_performed = self.db.Column(self.db.String(100), nullable=False)
            date_added = self.db.Column(self.db.DateTime, default=datetime.datetime.utcnow())            

        # create all the database
        with self.app.app_context():
            self.db.create_all()

        # set up configurations 
        self.configs(**configs)

    def configs(self, **configs):
        for config, value in configs:
            self.app.config[config.upper()] = value 

    def add_endpoint(self, endpoint=None, endpoint_name=None, handler=None, methods=['GET'], *args, **kwargs):
        self.app.add_url_rule(endpoint, endpoint_name, EndpointHandler(handler), methods=methods, *args, **kwargs)

    def run(self, **kwargs):
        self.app.run(**kwargs)


def index():
    global config
    return render_template('index.html', config=config)

def selection(parent_key, child_key):
    global config 

    # evaluate the selection made and generate Flask WTF 
    form = None 
    template = "index.html"
    if child_key == "Metadata_Compare":
        form, template = metadata_compare_process(parent_key, child_key)

    return render_template(template, config=config, parent_key=parent_key, child_key=child_key, form=form)


def save_files_to_workspace(directory, files_data):
    """ save the files to specified directory """

    for file in files_data:
        if not os.path.exists(os.path.dirname(os.path.join(directory, file.filename))):
            os.makedirs(os.path.dirname(os.path.join(directory, file.filename)))
        file.save(os.path.join(directory, file.filename))        


def metadata_compare_process(parent_key, child_key):
    """ Create Metadata update compare process """
    global config 

    form = None 
    form = MetadataCompare()

	# check the form if is submitted 
    if form.validate_on_submit():

        # get the file details from the form 
        base_files = form.base_files.data 
        release_files = form.release_files.data

        # save uploaded files to workspace directory 
        mypath = friday_reusable.setup_user_workspace(workspace_directory=os.path.dirname(__file__))

        # create base directory 
        base_directory = os.path.join(mypath, "Base_Files")
        release_directory = os.path.join(mypath, "Release_Files")

        # save the files to base and release directory within the workspace directory 
        save_files_to_workspace(base_directory, base_files)
        save_files_to_workspace(release_directory, release_files)

        # update yaml data 
        config[parent_key][child_key]["BaseConfig"]["Base_Location"] = base_directory
        config[parent_key][child_key]["ReleaseConfig"]["Release_Location"] = release_directory
        config[parent_key][child_key]["BaseConfig"]["Base_Files"] = base_files
        config[parent_key][child_key]["BaseConfig"]["Release_Files"] = release_files


    return form, "metadata_compare.html"



# create  flask application
flask_app = Flask(__name__)
app = FlaskAppWrapper(flask_app)

# get the configuration data 
config = friday_reusable.get_config_data()

# register all action functions to app
app.add_endpoint('/', 'index', index)
app.add_endpoint('/selection/<parent_key>/<child_key>', "selection", selection)

if __name__ == '__main__':
    app.run(debug=True)        