import json
import warnings
import pymysql
import os


warnings.filterwarnings("ignore")

class MysqlController():

    def __init__(self):
        self.loadMysqlConfig()
        self.loadMysqlPayload()

    # log file to Mysql
    def coreProcessor(self, pathLog, fileLog, paramLog):
        
        self._pathLog = pathLog
        self._fileLog = fileLog
        self._paramLog = paramLog

        with open('../config.ini')as config_f:
            config = json.load(config_f)

        self._whiteHosts = config.get('whiteHosts')

        '''
        dataDict = {
            'path':[
                ('host','path')],
            'file':[
                ('host','file')],
            'param':[
                ('host','param')]}
        '''
        dataDict = {
            'path':self.getDataFromLog(self._pathLog),
            'file':self.getDataFromLog(self._fileLog),
            'param':self.getDataFromLog(self._paramLog)}

        self.dataStorage(dataDict)

    def loadMysqlConfig(self):
    	
    	with open('../config.ini')as config_f:
    		mysql_config = json.load(config_f).get('mysql')

        self._host = mysql_config.get('host')
        self._user = mysql_config.get('user')
        self._password = mysql_config.get('password')
        self._port = int(mysql_config.get('port'))
        self._database = mysql_config.get('database')

    def loadMysqlPayload(self):

        self._createDatabase = '''
            create database if not exists {} charset utf8;
            '''.format(self._database)

        self._createTableParam = '''
            create table if not exists param(
            id int not null primary key auto_increment,
            host varchar(100),
            param varchar(300) not null,
            count int default 1);
            '''
        self._createTablepath = '''
            create table if not exists path(
            id int not null primary key auto_increment,
            host varchar(100),
            path varchar(300) not null,
            count int default 1);
            '''
        self._createTableFile = '''
            create table if not exists file(
            id int not null primary key auto_increment,
            host varchar(100),
            file varchar(300) not null,
            count int default 1);
            '''
        self._selectTableParam = '''
            select count(*) from param where
            host = %s and param = %s
            '''
        self._selectTablepath = '''
            select count(*) from path where
            host = %s and path = %s
            '''
        self._selectTableFile = '''
            select count(*) from file where
            host = %s and file = %s
            '''
        self._insertTableParam = '''
            insert into param(
            host,param) values(%s,%s)
            '''
        self._insertTablepath = '''
            insert into path(
            host,path) values(%s,%s)
            '''
        self._insertTableFile = '''
            insert into file(
            host,file) values(%s,%s)
            '''
        self._updateTableParamCount = '''
            update param set count = count + 1
            where host = %s and param = %s
            '''
        self._updateTablepathCount = '''
            update path set count = count + 1
            where host = %s and path = %s
            '''
        self._updateTableFileCount = '''
            update file set count = count + 1
            where host = %s and file = %s
            '''
        self._createTableParam_dict = '''
            create table if not exists param_dict(
            id int not null primary key auto_increment,
            param varchar(300) not null,
            count int default 1);
            '''
        self._createTablepath_dict = '''
            create table if not exists path_dict(
            id int not null primary key auto_increment,
            path varchar(300) not null,
            count int default 1);
            '''
        self._createTableFile_dict = '''
            create table if not exists file_dict(
            id int not null primary key auto_increment,
            file varchar(300) not null,
            count int default 1);
            '''
        self._selectTableParam_Dict = '''
            select count(*) from param_dict where param = %s
            '''
        self._selectTablepath_Dict = '''
            select count(*) from path_dict where path = %s
            '''
        self._selectTableFile_Dict = '''
            select count(*) from file_dict where file = %s
            '''
        self._insertTableParam_Dict = '''
            insert into param_dict(param) values(%s)
            '''
        self._insertTablepath_Dict = '''
            insert into path_dict(path) values(%s)
            '''
        self._insertTableFile_Dict = '''
            insert into file_dict(file) values(%s)
            '''
        self._updateTableParam_DictCount = '''
            update param_dict set count = count + 1
            where param = %s
            '''
        self._updateTablepath_DictCount = '''
            update path_dict set count = count + 1
            where path = %s
            '''
        self._updateTableFile_DictCount = '''
            update file_dict set count = count + 1
            where file = %s
            '''
    # testing mysql connections
    def connectTest(self):

        try:
            connection = pymysql.connect(host=self._host,
                user=self._user,
                password=self._password,
                port=self._port,
                cursorclass=pymysql.cursors.DictCursor)
            print('\n'+'='*10)
            print('MySQL Connection Succession')
            print('='*10+'\n')

        except Exception as e:

            if e[0] == 1045:
                print('MySQL Access denied !!')
        else:

            cursor = connection.cursor()

            cursor.execute(self._createDatabase)
            connection.commit()

            connection.select_db(self._database)
            cursor.execute(self._createTableParam)
            cursor.execute(self._createTablepath)
            cursor.execute(self._createTableFile)
            cursor.execute(self._createTableParam_dict)
            cursor.execute(self._createTablepath_dict)
            cursor.execute(self._createTableFile_dict)
            connection.commit()

            connection.close()

            return True

    # @return [(host,orther),]
    def getDataFromLog(self, logFile):

        tempData = []

        with open(logFile)as log_f:

            for line in log_f:
                line = line.strip()
                host,orther = line.split('\t')

                # The host that is not on the whitelist will be reset to empty
                # for whiteHost in self._whiteHosts:
                #     if not whiteHost or not host.endswith(whiteHost):
                #         host = ''
                #     else:
                #         host = '*.{}'.format(whiteHost)

                tempData.append((host, orther))
        print(tempData)
        return tempData

    '''
    dataDict = {
        'path':[
            ('host','path')],
        'file':[
            ('host','file')],
        'param':[
            ('host','param')]}
    '''
    def dataStorage(self, dataDict):

        try:
            connection = pymysql.connect(host=self._host,
                user=self._user,
                password=self._password,
                port=self._port,
                db=self._database,
                cursorclass=pymysql.cursors.DictCursor)

        except Exception as e:

            if e[0] == 1045:
                print('MySQL Access denied !!')                
        else:

            cursor = connection.cursor()

            if dataDict.get('path'):
                for host, paths in dataDict.get('path'):
                    self.operateTablepath(cursor, host, paths)
                    for path in paths[1:-1].split('/'):
                        self.operateTablepath_dict(cursor, path)
                connection.commit()

            if dataDict.get('file'):
                for host, files in dataDict.get('file'):
                    self.operateTableFile(cursor, host, files)
                    self.operateTableFile_dict(cursor, files)

                connection.commit()

            if dataDict.get('param'):
                for host,params in dataDict.get('param'):
                    for param in params.split(','):
                        self.operateTableParam(cursor, host, param)
                        self.operateTableParam_dict(cursor,param)
                connection.commit()

            connection.close()

    def operateTableParam(self, cursor, host, param):
        
        cursor.execute(self._selectTableParam, (host, param))
        itemCount = cursor.fetchone()

        if not itemCount.get('count(*)'):
            cursor.execute(self._insertTableParam, (host, param))
        else:
            cursor.execute(self._updateTableParamCount, (host, param))

    def operateTablepath(self, cursor, host, path):
        
        cursor.execute(self._selectTablepath, (host, path))
        itemCount = cursor.fetchone()

        if not itemCount.get('count(*)'):
            cursor.execute(self._insertTablepath, (host, path))
        else:
            cursor.execute(self._updateTablepathCount, (host, path))

    def operateTableFile(self, cursor, host, file):
        
        cursor.execute(self._selectTableFile, (host, file))
        itemCount = cursor.fetchone()

        if not itemCount.get('count(*)'):
            cursor.execute(self._insertTableFile, (host, file))
        else:
            cursor.execute(self._updateTableFileCount, (host, file))

    def operateTableParam_dict(self, cursor, param):
        
        cursor.execute(self._selectTableParam_Dict, (param))
        itemCount = cursor.fetchone()

        if not itemCount.get('count(*)'):
            cursor.execute(self._insertTableParam_Dict, (param))
        else:
            cursor.execute(self._updateTableParam_DictCount, (param))

    def operateTablepath_dict(self, cursor, path):
        
        cursor.execute(self._selectTablepath_Dict, (path))
        itemCount = cursor.fetchone()

        if not itemCount.get('count(*)'):
            cursor.execute(self._insertTablepath_Dict, (path))
        else:
            cursor.execute(self._updateTablepath_DictCount, (path))

    def operateTableFile_dict(self, cursor, file):
        
        cursor.execute(self._selectTableFile_Dict, [file])
        itemCount = cursor.fetchone()

        if not itemCount.get('count(*)'):
            cursor.execute(self._insertTableFile_Dict, [file])
        else:
            cursor.execute(self._updateTableFile_DictCount, [file])



# if config.get('test') :
#     with open('../config.ini')as config_f:
#         config = json.load(config_f)
#     os.chdir('log')
#     MysqlController().connectTest()
#     MysqlController().coreProcessor('20190111221523path.log', '20190111221523file.log', '20190111221523param.log')