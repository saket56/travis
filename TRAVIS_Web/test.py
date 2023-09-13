from flask import Flask, request, make_response

class EndpointHandler(object):

    def __init__(self, action):
        self.action = action 

    def __call__(self, *args, **kwargs):
        response = self.action(*args, **request.view_args)
        return make_response(response)

class FlaskAppWrapper(object):

    def __init__(self, app, **configs):
        self.app = app
        self.configs(**configs)

    def configs(self, **configs):
        for config, value in configs:
            self.app.config[config.upper()] = value

    def add_endpoint(self, endpoint=None, endpoint_name=None, handler=None, methods=['GET'], *args, **kwargs):
        self.app.add_url_rule(endpoint, endpoint_name, EndpointHandler(handler), methods=methods, *args, **kwargs)

    def run(self, **kwargs):
        self.app.run(**kwargs)

def action1(name):
    """
    This function takes `name` argument and returns `Hello name`.
    """
    return "Hello " + name

def action2():
    """
    This function returns "Action2 invoked"
    """

    return "Action2 invoked"

def action3(number):
    """
    This function returns the cube of the number 
    """
    return "Cube of {0} is {1}".format(number, number**3)



flask_app = Flask(__name__)
app = FlaskAppWrapper(flask_app)

# register all action functions to app

app.add_endpoint('/action1/<string:name>', 'action1', action1)
app.add_endpoint('/action2/', 'action2', action2)
app.add_endpoint('/action3/<int:number>', 'action3', action3)

if __name__ == '__main__':
    app.run(debug=True)