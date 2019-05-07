from urlparse import urlparse

from lib.data import DATA
import re

#
# extract path,file and param from Proxy History
#

class DataExtractor():

    def __init__(self, host, analyzedRequest):

        self.coreProcessor(analyzedRequest)
        DATA.host = host

    def coreProcessor(self,analyzedRequest):


        url = str(analyzedRequest.getUrl())

        path = urlparse(url).path
        DATA.path, DATA.file = self.formatPathFile(path)
        paramsObject = analyzedRequest.getParameters()
        DATA.params = self.processParamsObject(paramsObject)


    # format path and file
    def formatPathFile(self, path):

        sepIndex = path.rfind('/')
        # EX: http://xxx/?id=1
        if path == '/':
            file = ''
            path = ''
        # EX: http://xxx/index.php
        # file = index.php
        elif sepIndex == 0:
            file = path[1:]
            path = ''
        # EX: http://xxx/x/index.php
        # path = /x/
        # file = index.php

        else:
            file = path[sepIndex + 1:]
            x = re.match(r'\S*\.\S{1,10}', file)

            # EX: http://xxx/x/index
            # path = /x/index
            # file = ''

            if x:
                path = path[:sepIndex + 1]
            else:
                file = ''

        return path, file

    # extract params
    def processParamsObject(self, paramsObject):

        # get parameters
        params = []
        for paramObject in paramsObject:

            # don't process Cookie's Pamrams
            if paramObject.getType() == 2:
                continue
            param = paramObject.getName()
            if param.startswith('_'):
                continue
            params.append(param)
        return ','.join(params)