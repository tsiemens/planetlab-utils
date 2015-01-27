#!/usr/bin/env python
import os
import sys
from subprocess import Popen, PIPE
import getpass
import xmlrpclib
from optparse import OptionParser

import plcapi

def main():
    parser = OptionParser('doplnodes.py [options] ARG \n' +
                          '         NODEHOST1 [NODEHOST2 ...] \n\n' +
                         'By default ARG is the name of a bash script')
    parser.add_option('-u', '--username',
                      action='store', type='string', dest='username',
                      help='The username for the planetlab account.')
    parser.add_option('-A', '--all',
                      action='store_true', dest='all_nodes', default=False,
                      help='Run the script on every node in the slice.')
    parser.add_option('--scp',
                      action='store', dest='scp_dir',
                      help='Instead of executing ARG, ARG will be ' +
                            'uploaded to SCP_DIR on the nodes')
    parser.add_option('-l', '--literal',
                      action='store_true', dest='literal_script',
                      default=False,
                      help='Instead of executing ARG as a file, ' +
                            'it will be executed directly.')
    parser.add_option('-i', '--identity-file',
                      action='store', dest='identity_file',
                      help='The private key which ssh/scp will use. ' +
                            'Not required if an ssh agent is being used.')

    options, args = parser.parse_args()

    username = None
    if options.username:
        username = options.username

    try:
        auth, api_server = plcapi.authorized_api_server(username)
        print 'Authorized!'
        runner = None
        if options.scp_dir:
            runner = ScpRunner(options.scp_dir, args[0],
                               options.identity_file)
        elif options.literal_script:
            runner = SshScriptRunner(args[0], args[0],
                                    options.identity_file)
        else:
            f = open(args[0], 'r')
            scripttext = f.read()
            f.close()
            runner = SshScriptRunner(args[0], scripttext)

        nodehosts = args[1:]
        if options.all_nodes:
            nodehosts = None
        nodes = plcapi.get_nodes(api_server, auth,
                                 nodehost_filters=nodehosts)

        for node in nodes:
            runner.do_on_node(node)

    except (xmlrpclib.Fault, plcapi.PlcApiException) as e:
        print 'Error: {0}'.format(e)


class SshScriptRunner():
    def __init__(self, scriptname, scripttext, identity_file=None):
        self.scriptname = scriptname
        self.scripttext = scripttext
        self.identity_file = identity_file

    def do_on_node(self, node):
        print('Running {0} on {1}'.format(self.scriptname, node['hostname']))
        identity_args = []
        if self.identity_file != None:
            identity_args = ['-i', self.identity_file]
        p = Popen(['ssh', '-o', 'StrictHostKeyChecking=no'] + identity_args +
                   ['ubc_eece411_2@{0}'.format(node['hostname']),
                   'bash -s'], stdin=PIPE)
        p.communicate(self.scripttext)


class ScpRunner():
    def __init__(self, directory, to_copy, identity_file=None):
        self.directory = directory
        self.to_copy = to_copy
        self.identity_file = identity_file

    def do_on_node(self, node):
        print('Copying {0} to {1}'.format(self.to_copy, node['hostname']))

        identity_args = []
        if self.identity_file != None:
            identity_args = ['-i', self.identity_file]
        p = Popen(['scp', '-o', 'StrictHostKeyChecking=no'] + identity_args +
                   ['-rp', self.to_copy,
                   'ubc_eece411_2@{0}:{1}'.format(node['hostname'],
                                                  self.directory)])


if __name__ == "__main__":
    main()
    quit()

