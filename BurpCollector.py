#!coding:utf-8

from lib.model import MysqlController
from lib.processing import DataExtractor
from lib.data import DATA
from lib.common import filterFile, filterHost

from burp import IBurpExtender
from burp import IExtensionStateListener
from burp import IContextMenuFactory
from burp import IProxyListener
from burp import IHttpListener
from burp import IExtensionHelpers

from java.io import PrintWriter

import sys

reload(sys)

sys.setdefaultencoding('utf-8')

class BurpExtender(IBurpExtender, IExtensionStateListener, IContextMenuFactory, IHttpListener, IExtensionHelpers):

    def registerExtenderCallbacks(self, callbacks):

        # Set extension's name
        callbacks.setExtensionName('Burp Collector')

        self._callbacks = callbacks

        self._helpers = callbacks.getHelpers()

        # 获取我们的输出流
        self._stdout = PrintWriter(callbacks.getStdout(), True)

        print("================================")
        print("         author:ckj123          ")
        print("       version:v0.000001        ")
        print("================================")
        # test the database connection
        MysqlController().connectTest()

        # retrieve the context menu factories
        callbacks.registerContextMenuFactory(self)

        # register ourselves as an Extension listener
        callbacks.registerExtensionStateListener(self)

        # register ourselves as an HTTP listener
        callbacks.registerHttpListener(self)

    #
    # 实现 IHttpListener
    #

    def processHttpMessage(self, toolFlag ,messageIsRequest, messageInfo):
        if toolFlag == 4 or toolFlag == 16 or toolFlag == 8 or toolFlag == 64:
            # proxy or scanner or spider
            if not messageIsRequest:
                ###### response
                # response = messageInfo.getResponse()
                # analyzedResponse = self._helpers.analyzeResponse(response)
                # headerList = analyzedResponse.getHeaders()
                # body = response[analyzedResponse.getBodyOffset():].tostring()
                # print("response")
                # print(analyzedResponse.getHeaders())
                # print(body)
                pass
            else:
                request = messageInfo.getRequest()
                httpService = messageInfo.getHttpService()
                # body = request[analyzedRequest.getBodyOffset():].tostring()
                analyzedRequest = self._helpers.analyzeRequest(httpService,request)
                # method = analyzedRequest.getMethod()

                ###### DataExtractor
                DataExtractor(httpService.getHost(), analyzedRequest)
                print(DATA.params)
                if filterFile(DATA.file) and filterHost(DATA.host):
                    MysqlController().coreProcessor()
