#!/usr/bin/env python
from optparse import OptionParser
import csv
import StringIO

import plcapi
from plcapi import PlcApiException

def main():
    parser = OptionParser('getnodes.py [options]')
    parser.add_option('-u', '--username',
                      action='store', type='string', dest='username',
                      help='The username for the planetlab account.')

    options, args = parser.parse_args()
    try:
        auth, api_server = plcapi.authorized_api_server(options.username)
        nodes = plcapi.get_nodes(api_server, auth)

        csvbuff = StringIO.StringIO()
        writer = csv.writer(csvbuff, quoting=csv.QUOTE_MINIMAL)
        for node in nodes:
            writer.writerow([node['node_id'], node['hostname']])
        
        print(csvbuff.getvalue())
        csvbuff.close()

    except PlcApiException as e:
        print('Error: {0}'.format(e))


if __name__ == "__main__":
    main()
