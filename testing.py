#! /usr/bin/python2
#
#
## This is the program to grab data directly from the Yahoo
## website. Needs to use the oauth tokens, pulls data, stores
## it in the PostgreSQL database.

## Client ID:     dj0yJmk9MUVRWHJzZjRwanFSJmQ9WVdrOVZVOWlNWFZVTkhNbWNHbzlNQS0tJnM9Y29uc3VtZXJzZWNyZXQmeD00ZQ--
## Client Secret: 4533be02755c3fe5c217534651d4ea64012b6b67
## Yahoo MLB 2016 Game ID = 357


import sys
import xml.etree.ElementTree as ET

def main():
    ''' This is the main function. Starts the call to all the other subroutines.
    Needs to authorize with Yahoo first, and then start calling various URLs based
    on the parameters given at the commandline. '''
    
    tree = ET.parse('/home/chris/python/football/draft5.xml')
    root = tree.getroot()
    root.findall('.')
    for child in root:
            for subchild in child:
                print subchild.tag, subchild.attrib

    namespaces = {'ns': 'http://fantasysports.yahooapis.com/fantasy/v2/base.rng}draft_results'}
    root.findall('ns:draft_results', namespaces)

    for result in root.findall('ns:draft_results', namespaces):
        for child in result:
            print child.text

if __name__ == "__main__":
    main()
