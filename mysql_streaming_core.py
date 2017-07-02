#  -*-  coding: utf-8 -*-
import sys, os
default_encoding = 'utf-8'
if sys.getdefaultencoding() != default_encoding:
    reload(sys)
    sys.setdefaultencoding(default_encoding)

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
    TableMapEvent,
)

import ConfigParser
from database import Database
from  process_streaming import  Tables
import datetime

class  Params:
        """
        :param daemon_code: load config file by different daemon_code
        :return: 0,1
        """
        def __init__(self,daemon_code):
                cf = ConfigParser.ConfigParser()
                cf.read('default.conf')
                cf.sections()
                home_dir = cf.get(daemon_code,'home_directory')
                os.chdir(home_dir )
                port = cf.getint(daemon_code,'port')
                self.mysql_settings = {
                        "host": cf.get(daemon_code,'host'),
                        "port": int(port),
                        "user": cf.get(daemon_code,'user'),
                        "password": cf.get(daemon_code,'password')
                }
                self.server_id = int(cf.get(daemon_code,'server_id'))
                self.only_schemas = cf.get(daemon_code,'only_schemas').split(",")
                self.only_tables = cf.get(daemon_code,'only_tables').split(",")

                # destination db server
                self.dest_host = cf.get('destination','host')
                self.dest_user = cf.get('destination','user')
                self.dest_password = cf.get('destination','password')
                self.dest_database = cf.get('destination','database')

class  MySQLStreaming(object):
        """
        #MySQL my.cnf configuration
        # [mysqld]
        #log-bin=mysql-bin
        #server-id=1
        ##binlog-format= row
        #log_slave_updates=true
        #

        events = set((
                        QueryEvent,
                        RotateEvent,
                        StopEvent,
                        FormatDescriptionEvent,
                        XidEvent,
                        GtidEvent,
                        BeginLoadQueryEvent,
                        ExecuteLoadQueryEvent,
                        UpdateRowsEvent,
                        WriteRowsEvent,
                        DeleteRowsEvent,
                        TableMapEvent,
                        HeartbeatLogEvent,
                        NotImplementedEvent,
                        ))

        """
        def __init__(self, schema):
                """
                :param daemon_code:
                        start different daemon by this code
                        replicate different database by this code
                :return: None
                """

                # get configuration
                params = Params(schema)

                # create destination database pool
                self.__dest_db = Database(host = params.dest_host, user = params.dest_user,
                                   password = params.dest_password, db=params.dest_database )

                self.__process_tables = Tables(self.__dest_db)
                self.__mapped_table = self.__assemble_sql(schema)
                #create mysql streaming
                EVENTS=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent]

                commit_logfile = schema + '_streaming_commit.rct'

                (self.__commit_file, log_file,log_pos) = self.__get_log_position(commit_logfile)
                if log_file == None:
                        exit(1)

                self.stream = BinLogStreamReader(connection_settings = params.mysql_settings,
                            only_events = EVENTS,
                            log_file = log_file,
                            log_pos = log_pos,            #resume_stream should be true
                            server_id = params.server_id,                      # must set it
                            resume_stream  = True ,
                            only_schemas = params.only_schemas,
                            only_tables = params.only_tables,
                            blocking = True)

        def  __get_log_position(self, commit_log):

                """

                :param commit_log:
                :param init_log_file:
                :param init_log_pos:
                :return:
                """

                # create commit log file if not exists
                if not os.path.exists(commit_log):
                        os.system(r'touch %s' % commit_log)

                commit_file = open(commit_log,'r+')
                line = commit_file.readlines()
                if len(line) == 1 and line[0].count(":") == 1:
                        (log_file,log_pos) = line[0].split(":")
                else:
                        msg = "Error: commit_log file "  + commit_log + " is error or no log_file:log_pos specified in commit log file"
                        self.msg( msg )
                        (log_file,log_pos) = (None,-1)

                return (commit_file,log_file,int(log_pos))

        def producer(self):

                for binlogevent in self.stream :
                        try:
                                if isinstance(binlogevent, TableMapEvent):
                                        binlogevent.dump()
                                else:
                                        for row in binlogevent.rows:
                                                event = {"schema": binlogevent.schema, "table": binlogevent.table}

                                                if isinstance(binlogevent, DeleteRowsEvent):
                                                        event["action"] = "delete"
                                                        event["values"] = dict(row["values"].items())
                                                        event = dict(event.items())
                                                elif isinstance(binlogevent, UpdateRowsEvent):
                                                        event["action"] = "update"
                                                        event["before_values"] = dict(row["before_values"].items())
                                                        event["values"] = dict(row["after_values"].items())
                                                        event = dict(event.items())
                                                elif isinstance(binlogevent, WriteRowsEvent):
                                                        event["action"] = "insert"
                                                        event["values"] = dict(row["values"].items())
                                                        event = dict(event.items())

                                                # change all keys into lower
                                                event={key.lower():value for key,value in event.items()}
                                                event["values"]  = {key.lower():value for key,value in  event["values"].items()}
                                                print self.stream.log_file, self.stream.log_pos,binlogevent.table
                                                result = self.__process_msg(event)
                                                if result == 1:
                                                        self.msg(  binlogevent.dump() )
                                                        self.close()
                        except  Exception,e:
                                self.msg( e.message)
                                self.msg(  binlogevent.dump() )
                                self.close()

        def close(self):
                self.msg("stop streaming")
                self.stream.close()
                exit(1)

        def  __process_msg(self, event):
                """
                :param table_name:  execute method by table_name
                :param action:  delete , update, insert
                :return: 0,1
                """
                result = 1
                if self.__mapped_table.has_key (event['table']):
                        result = self.__process_normal(event)
                else:
                        process = getattr(Tables,event['table'])
                        result = process(event)

                if result == 0:
                        self.__commit_file.truncate
                        self.__commit_file.seek(0)
                        self.__commit_file.writelines(self.stream.log_file + ":" + str(self.stream.log_pos) + " "*50)
                        self.__commit_file.close()
                        self.__commit_file = open(self.__commit_file.name,"r+")

                return result

        def msg(self,msg):
                print msg
                sys.stdout.flush()

        def __process_normal(self,event):
                """
                :param event:
                :return:
                """

                action = event['action']
                table = event['table']
                try:
                        for  result in self.__mapped_table[table][action]:
                                nwq_val = []
                                for val in result['val']:
                                        value = ''
                                        if val.count(".") > 0:
                                                objs = val.split(".")
                                                value = event["values"][objs[0]]

                                                for i in range(1, len(objs)):
                                                        obj = objs[i].split("::")
                                                        process = getattr(self.__process_tables,obj[0])
                                                        if len(obj) == 1:
                                                                value = process(value)
                                                        elif len(obj) == 2:
                                                                value = process(value, obj[1])
                                                        elif len(obj)==3:
                                                                value = process(value, obj[1], obj[2])
                                                        elif len(obj)==4:
                                                                value = process(value, obj[1], obj[2], obj[3])

                                        else :
                                                if val == '-1':
                                                    value = '-1'
                                                else :
                                                    value = event["values"][val]

                                        nwq_val.append(value )

                                self.__dest_db.execute(result['sql'],nwq_val)

                        self.__dest_db.commit()

                        return 0
                except  Exception,e:
                        self.msg(e.message)
                        self.msg( event )
                        return 1


        def __assemble_sql(self,schema):
                """
                assambly sql
                :param schema:
                :return:  assembled sql and val
                """
                mapped_conf = ConfigParser.ConfigParser()
                map_file = schema + "_mapped_tables.conf"
                if os.path.exists(map_file):
                        mapped_conf.read(map_file)
                else:
                        return {}

                assemble = {}

                for  table in mapped_conf.sections():
                        table=table.strip()
                        if  table.find(".") >-1:
                                i=0
                                # print "nothing doing"
                        else:
                                assemble[table] = {}
                                assemble[table]['delete'] = []
                                assemble[table]['insert'] = []
                                assemble[table]['update'] = []

                                if mapped_conf.has_option(table,"map"):
                                    mapped_tables = mapped_conf.get(table,"map").split(",")
                                else:
                                        continue

                                for mapped_table in mapped_tables:

                                        mapped_table=mapped_table.strip()
                                        source_pk = mapped_conf.get(mapped_table,"source_pk").lower().split(",")
                                        dest_pk = mapped_conf.get(mapped_table,"dest_pk").lower()

                                        bind = ("%s," * len( dest_pk.split(",") ))[:-1]

                                        sql = "delete from " + mapped_table + " where (" + dest_pk + ") = (" + bind + ") "

                                        assemble[table]['delete'].append({'sql':sql, 'val':source_pk})

                                        mapped_fields = mapped_conf.items(mapped_table)
                                        sql_insert = "insert into " + mapped_table + "(" + dest_pk + ","
                                        val_insert = source_pk[:]       # must copy value
                                        sql_update = "update " + mapped_table + " set "
                                        val_update = []
                                        for (field,mapped_field) in mapped_fields:
                                                if field != 'source_pk' and field !='dest_pk':
                                                        sql_insert = sql_insert + field + ","
                                                        sql_update = sql_update + field + " = %s, "

                                                        # value name
                                                        if mapped_field.count(".") == 0:
                                                                val_insert.append(mapped_field.lower() )
                                                                val_update.append(mapped_field.lower() )
                                                        else:
                                                                val_insert.append(mapped_field )
                                                                val_update.append(mapped_field)

                                        sql_update = sql_update.strip()[:-1] + " where (" + dest_pk + ") = (" + bind + ") "


                                        for i in range(len(source_pk)):
                                                val_update.append(source_pk[i])

                                        sql_insert = sql_insert[:-1] + ") values (" + ("%s," * len(val_insert))[:-1] + ")"
                                        assemble[table]['insert'] .append({'sql':sql_insert, 'val':val_insert})
                                        assemble[table]['update'] .append({'sql':sql_update, 'val':val_update})

                return assemble
