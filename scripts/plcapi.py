import getpass
import xmlrpclib

class PlcApiException(Exception):
    def __init__(self, message):
        self.message = message
    
    def __str__(self):
        return self.message


def create_auth(username, password):
    # Create an empty dictionary (XML-RPC struct)
    auth = {}
    auth['AuthMethod'] = 'password'
    auth['Username'] = username
    auth['AuthString'] = password
    return auth

def get_auth(username=None):
    ''' Gets the username and password from the user.
        Optionally, the username can be pre-provided.
        If the user fails to provide the username, throws PlcApiException'''
    if not username:
        username = raw_input('Enter username:')

    password= getpass.getpass()
    if not password or not username:
        raise PlcApiException('Username and password are required.')

    return create_auth(username, password)

def api_server():
    return xmlrpclib.ServerProxy('https://www.planet-lab.org/PLCAPI/',
                                       allow_none=True)

def authorized_api_server(username=None):
    ''' Creates a proxy server for the PLC API, and checks the authorization.
        A failure will raise a PlcApiException
        returns (auth struct, xmlrpclib.ServerProxy)'''
    server = api_server()
    auth = get_auth(username)
    try:
        server.AuthCheck(auth)
        return auth, server
    except xmlrpclib.Fault as e:
        raise PlcApiException(str(e))

def get_nodes(server, auth, nodehost_filters=None):
    slices = server.GetSlices(auth)
    my_slice = slices[0]

    slice_filter = {'node_id':my_slice['node_ids']}
    if nodehost_filters != None:
        slice_filter['hostname'] = nodehost_filters

    return_fields = ['node_id', 'hostname']
    nodes = server.GetNodes(auth, slice_filter, return_fields)

    return list(node for node in nodes if node['node_id'] % 2 == 0)

