from pymodbus.client import ModbusTcpClient
from pymodbus.register_write_message import WriteSingleRegisterRequest  
from pymodbus.exceptions import ModbusException, ModbusIOException 
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb import InfluxDBClient
from collections import OrderedDict  
from datetime import datetime, timedelta
from telnetlib import Telnet
from itertools import groupby
from PyQt5.QtWidgets import QMessageBox
from PyQt5.QtWidgets import *
from PyQt5.QtCore import Qt
from pydub import AudioSegment
from pydub.playback import play
from pydub import AudioSegment
from pypinyin import pinyin, Style
from dateutil import parser 
import pandas as pd 
import pyautogui 
import threading
import queue  
import socket
import subprocess
import time
import timeit
import json
import os
import sys
import pytz  
import logging
import tkinter as tk
from tkinter import ttk
from PIL import Image, ImageTk
import psutil
import atexit 
import msvcrt 
import fifobuffer as FIFO
import  traceback


class Alarm(object):

    def __init__(self):
        self.write_cache_from_alert_note = FIFO.FIFOBufffer(1000)
        self.write_cache_from_alert_settled = FIFO.FIFOBufffer(1000)

    def alert_m(self, alr, station, line, current_time):
        self.alert_mark = alr
        self.station = station
        self.line = line

        alert_settled_data = \
        {
            "measurement":          "alert_settled",
            "time":                 current_time,
            "fields":
            {
                "恢复内容":         self.alert_mark,
                "报警解除时间":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
            },
            "tags":
            {
                "行车方向":         self.line,
                "站点":             self.station
            }   
        }
        self.write_cache_from_alert_settled.push(alert_settled_data)

    def alert_n(self, alr, station, line, current_time):
        self.alert_note = alr
        self.station = station
        self.line = line

        alert_record_data = \
        {
            "measurement":          "alert_record",
            "time":                 current_time,
            "fields":
            {
                "报警内容":          self.alert_note,
                "报警响应时间":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
            },
            "tags":
            {
                "行车方向":          self.line,
                "站点":              self.station
            }   
        }
        self.write_cache_from_alert_note.push(alert_record_data)


class PerformanceMonitoring(object):

    def __init__(self):
        self.write_cache_from_collect_data = FIFO.FIFOBufffer(1000)
        self.write_cache_from_process_data = FIFO.FIFOBufffer(1000)
        self.write_cache_from_sep_table_data = FIFO.FIFOBufffer(1000)
        self.write_cache_from_write_db_data = FIFO.FIFOBufffer(1000)

    def DataBase_collect_time_send(self, loop_time, current_time):
        collect_time = \
        {
            "measurement":                  "collect_time",
            "time":                         current_time,
            "fields":
            {
                "采集周期":                loop_time,
                "采集频率":                1/(loop_time/1000),
            }
        }
        self.write_cache_from_collect_data.push(collect_time)

    def DataBase_process_time_send(self, data_process_time, current_time):
        process_time = \
        {
            "measurement":                  "process_time",
            "time":                         current_time,
            "fields":
            {
                "数据处理时间":              data_process_time
            }
        }
        self.write_cache_from_process_data.push(process_time)
    
    def DataBase_sep_table_time_send(self, sep_table_time, current_time):
        sep_time = \
        {
            "measurement":                  "sep_table_time",
            "time":                         current_time,
            "fields":
            {
                "数据库分表处理时间":         sep_table_time
            }
        }
        self.write_cache_from_sep_table_data.push(sep_time)

    def DataBase_write_time_send(self, write_time, current_time):
        write_db_time = \
        {
            "measurement":                  "write_time",
            "time":                         current_time,
            "fields":
            {
                "写入数据库时间":            write_time
            }
        }
        self.write_cache_from_write_db_data.push(write_db_time) 
    
    


class PSD_Monitoring(object):
    dict_cfg = {}
    #DBclient_sep = None
    DBclient_alert_settle = None
    
    def __init__(self):
        self.setting_addr = "D:\PSDmonitor\profile\SettingMenu.json"
        # Initial Config
        with open(file=self.setting_addr, encoding='utf-8') as cfg:
            # getting config
            self.dict_cfg = json.load(cfg)
        self.data_sender = [] 
        self.database_info = \
        {
            "host":                            self.display_clients()['database']['host'],
            "port":                            self.display_clients()['database']['port'],
            "username":                        self.display_clients()['database']['username'],
            "password":                        self.display_clients()['database']['password'],
            'DBname-station':                  self.display_clients()['database']['DBname-station'],
            'DBname-station_SAVE':             self.display_clients()['database']['DBname-station_SAVE'],
            'DBname-station_HISTORY':          self.display_clients()['database']['DBname-station_HISTORY'],
            'DBname-train':                    self.display_clients()['database']['DBname-train'],
            "DBname-network":                  self.display_clients()['database']['DBname-network'],
            "DBname-alert_record":             self.display_clients()['database']['DBname-alert_record'],
            "DBname-alert_check":              self.display_clients()['database']['DBname-alert_check'],
            "DBname-alert_settled":            self.display_clients()['database']['DBname-alert_settled'],
            "DBname-alert_time_consuming":     self.display_clients()['database']['DBname-alert_time_consuming'],
            "DBname-collect_time":             self.display_clients()['database']['DBname-collect_time'],
            "DBname-process_time":             self.display_clients()['database']['DBname-process_time'],
            "DBname-write_time":               self.display_clients()['database']['DBname-write_time'],
            "DBname-sep_table_time":           self.display_clients()['database']['DBname-sep_table_time']
        }
        self.station_client = {}
        self.server_client = {}
        self.collect_ls = []
        self.Relay_list = []
        self.collect_nop = []
        self.previous_data = []
        self.current_data = []
        self.notes_nop = []
        self.door_nop = []
        self.collect_notes = None
        self.current_note = None
        self.lock_file = True
        self.sep = False
        self.dele = False
        self.create_sep_thread = True
        self.create_proc_thread = True
        self.create_write_db_thread = True
        self.q = queue.Queue() 
        self.time_count_connect = []
        self.relay_buff = []
        self.relay_sort_ls = []
        self.alert_event = threading.Event()
        self.sep_event = threading.Event()
        self.now_datetime = datetime.now()
        self.source_measurement = self.database_info['DBname-station_HISTORY']
        self.alarm_object = Alarm()
        self.monitor_object = PerformanceMonitoring()
        self.Tc = self.dict_cfg['InitialConfig']['Tc']
        self.base_time = self.dict_cfg['InitialConfig']['base_time']
        self.save_all = self.dict_cfg['InitialConfig']['save_all']
        self.station_info = self.dict_cfg['TerminalClient']['T1']['station']
        self.debug_monitor = self.dict_cfg["InitialConfig"]["debug_monitor"]
        
        # FIFO队列缓冲区
        self.write_cache_from_station = FIFO.FIFOBufffer(1000)
        self.write_cache_from_station_SAVE = FIFO.FIFOBufffer(1000)
        self.write_cache_from_station_HISTORY = FIFO.FIFOBufffer(1000)


    def ff(self, result, time):
        result = hex(result)[2:]
        l = len(result)
        if l == 1:
           if   time == 'S':
                t = int("0x" + str(result[0:1]), base=16)
           if   time == 'Q':
                return int("0x" + str(0), base=16)
        if l == 2:
           if   time == 'S':
                return int("0x" + str(result[0:2]), base=16)
           if   time == 'Q':
                return int("0x" + str(0), base=16)
        if l == 3:
            if time == 'Y' or time == 'D' or time == 'Q':
                return int("0x" + str(result[1:3]), base=16)
            if time == 'M' or time == 'H' or time == 'S':
                return int("0x" + str(result[0:1]), base=16)
        if l == 4:
            if time == 'Y' or time == 'D' or time == 'Q':
                return int("0x" + str(result[2:4]), base=16)
            if time == 'M' or time == 'H' or time == 'S':
                return int("0x" + str(result[0:2]), base=16)
        if time == 'ms':
            if l == 3:
                return int("0x" + str(result[0:3]), base=16)
            if l == 2:
                return int("0x" + str(result[0:2]), base=16)
            if l == 1:
                return int("0x" + str(result[0:1]), base=16)
            
    
    def data_connect(self, data, relay, channel):
        group, CH_data = [], []
        # 获取relay的键和值，以及channel的值  
        relay_keys = list(relay.keys())  
        relay_values = list(relay.values())  
        # 翻转数据序列 
        ch_data = data[channel][::-1] 
        CH_data.append(ch_data)
        for i in range (len(CH_data)):
            for j in relay_keys:
                group.append(CH_data[i][int(j)])
            break
        return group


    def display_clients(self):
        T_dict = {}
        for i in range(self.dict_cfg['TerminalNum']):
            T = "T" + str(i+1)
            Th = self.dict_cfg['TerminalClient'][T]['host']
            Tp = self.dict_cfg['TerminalClient'][T]['port']
            T_dict[T] = {'host': Th, 'port': Tp}  
        # 数据库
        DBhost = self.dict_cfg['DatabaseClient']['host']
        DBport = self.dict_cfg['DatabaseClient']['port']
        DBusername = self.dict_cfg['DatabaseClient']['username']
        DBpassword = self.dict_cfg['DatabaseClient']['password']
        # 站点记录
        DBname_station = self.dict_cfg['DatabaseClient']['DBname'][0]
        DBname_station_SAVE = self.dict_cfg['DatabaseClient']['DBname'][1]
        DBname_station_HISTORY = self.dict_cfg['DatabaseClient']['DBname'][2]
        # 过车记录
        DBname_train = self.dict_cfg['DatabaseClient']['DBname'][3]
        # 网络记录
        DBname_network = self.dict_cfg['DatabaseClient']['DBname'][4]
        # 报警记录
        DBname_alert_record = self.dict_cfg['DatabaseClient']['DBname'][5]
        DBname_alert_check = self.dict_cfg['DatabaseClient']['DBname'][6]
        DBname_alert_settled = self.dict_cfg['DatabaseClient']['DBname'][7]
        DBname_alert_time_consuming = self.dict_cfg['DatabaseClient']['DBname'][8]
        # 采集与处理时间
        DBname_collect_time = self.dict_cfg['DatabaseClient']['DBname'][9]
        DBname_process_time = self.dict_cfg['DatabaseClient']['DBname'][10]
        DBname_write_time = self.dict_cfg['DatabaseClient']['DBname'][11]
        DBname_sep_table_time = self.dict_cfg['DatabaseClient']['DBname'][12]
        # 处理集合
        dict_info = {
            'terminal': 
                T_dict,
            'database':{
                'host': DBhost,
                'port': DBport,
                'username': DBusername,
                'password': DBpassword,
                'DBname-station': DBname_station,
                'DBname-station_SAVE': DBname_station_SAVE,
                'DBname-station_HISTORY': DBname_station_HISTORY,
                'DBname-train': DBname_train,
                'DBname-network': DBname_network,
                'DBname-alert_record': DBname_alert_record,
                'DBname-alert_check': DBname_alert_check,
                'DBname-alert_settled': DBname_alert_settled,
                'DBname-alert_time_consuming': DBname_alert_time_consuming,
                'DBname-collect_time': DBname_collect_time,
                'DBname-process_time': DBname_process_time,
                'DBname-write_time': DBname_write_time,
                "DBname-sep_table_time": DBname_sep_table_time
            }
        }
        return dict_info
    

    def play_alarm(self) -> None:
        self.window_show_flag = True
        while True:
            self.alert_event.wait() 
            if self.window_show_flag:
                self.window_show_flag = False
                alert_show_time = datetime.now()
                alert_show_time = self.time_split_point(alert_show_time.strftime('%Y-%m-%d %H:%M:%S.%f'), 3)
            self.box = QMessageBox.warning(None, "监测异常", "报警时间：{}".format(alert_show_time), QMessageBox.Ok)
            self.alert_event.clear() 
    
             
    def Notes_MessageBox(self, note):
        self.box = QMessageBox.warning(None, "警告", note, QMessageBox.Ok)

            
    def timestamp_to_localtime_str(self, timestamp):
        # 将时间戳转换为datetime对象
        dt_obj = datetime.fromtimestamp(timestamp)
        # 使用strftime格式化为本地时间字符串
        localtime_str = dt_obj.strftime('%Y-%m-%d %H:%M:%S')
        return localtime_str
    

    def time_split_point(self, time_str, f_num):
        # 分割整数秒和小数秒  
        parts = time_str.split('.')  
        if len(parts) > 1:  
            # 保留小数秒的前3位，并重新组合字符串  
            formatted_time_str = f"{parts[0]}.{parts[1][:f_num]}"  
        else:  
            # 如果没有小数点，则原样返回（但这种情况在实际的时间字符串中不太可能发生）  
            formatted_time_str = time_str
        return formatted_time_str
      
                                    
    def DataBase_connect(self, HOST, PORT, USER, PASSWORD, save_all):
        # connect to database 
        try:
            # station 
            self.DBclient = InfluxDBClient(HOST, PORT, USER, PASSWORD)
            station = self.database_info['DBname-station']
            self.DBclient.switch_database(station)
            query_retention = f'CREATE RETENTION POLICY "station_retention" ON "{station}" DURATION 1h REPLICATION 1 DEFAULT'
            self.DBclient.query(query_retention) 
            # station history
            self.DBclient = InfluxDBClient(HOST, PORT, USER, PASSWORD)
            station = self.database_info['DBname-station_HISTORY']
            self.DBclient.switch_database(station)
            query_retention = f'CREATE RETENTION POLICY "station_HISTORY_retention" ON "{station}" DURATION 3d REPLICATION 1 DEFAULT'
            self.DBclient.query(query_retention) 
            # alert record
            self.DBclient_alarm = InfluxDBClient(HOST, PORT, USER, PASSWORD)
            station = self.database_info["DBname-alert_record"]
            self.DBclient_alarm.switch_database(station)
            query_retention = f'CREATE RETENTION POLICY "alert_record_retention" ON "{station}" DURATION 1h REPLICATION 1 DEFAULT'
            self.DBclient_alarm.query(query_retention) 
            # alert settle
            self.DBclient_alert_settle = InfluxDBClient(HOST, PORT, USER, PASSWORD)
            station = self.database_info["DBname-alert_settled"]
            self.DBclient_alert_settle.switch_database(station)
            query_retention = f'CREATE RETENTION POLICY "alert_settled_retention" ON "{station}" DURATION 1h REPLICATION 1 DEFAULT'
            self.DBclient_alert_settle.query(query_retention) 

            # 用于监控设备状态
            if self.debug_monitor:
                # collect time
                self.DBclient_collect_time = InfluxDBClient(HOST, PORT, USER, PASSWORD)
                station = self.database_info["DBname-collect_time"]
                self.DBclient_collect_time.switch_database(station)
                query_retention = f'CREATE RETENTION POLICY "collect_time_retention" ON "{station}" DURATION 1h REPLICATION 1 DEFAULT'
                self.DBclient_collect_time.query(query_retention) 
                # data process time
                self.DBclient_data_process_time = InfluxDBClient(HOST, PORT, USER, PASSWORD)
                station = self.database_info["DBname-process_time"]
                self.DBclient_data_process_time.switch_database(station)
                query_retention = f'CREATE RETENTION POLICY "process_time_retention" ON "{station}" DURATION 1h REPLICATION 1 DEFAULT'
                self.DBclient_data_process_time.query(query_retention)
                # sep table time
                self.DBclient_sep_table_time = InfluxDBClient(HOST, PORT, USER, PASSWORD)
                station = self.database_info["DBname-sep_table_time"]
                self.DBclient_sep_table_time.switch_database(station)
                query_retention = f'CREATE RETENTION POLICY "sep_table_time_retention" ON "{station}" DURATION 1h REPLICATION 1 DEFAULT'
                self.DBclient_sep_table_time.query(query_retention)
                # write db time
                self.DBclient_write_db_time = InfluxDBClient(HOST, PORT, USER, PASSWORD)
                station = self.database_info["DBname-write_time"]
                self.DBclient_write_db_time.switch_database(station)
                query_retention = f'CREATE RETENTION POLICY "write_time_retention" ON "{station}" DURATION 1h REPLICATION 1 DEFAULT'
                self.DBclient_write_db_time.query(query_retention)

             
            # clearing database and then creating a new database again
            if save_all == False:
                self.DBclient.drop_database(self.database_info['DBname-station'])   
                self.DBclient.create_database(self.database_info['DBname-station'])
                
                self.DBclient.drop_database(self.database_info['DBname-station_SAVE'])   
                self.DBclient.create_database(self.database_info['DBname-station_SAVE'])
                
                self.DBclient.drop_database(self.database_info['DBname-station_HISTORY'])   
                self.DBclient.create_database(self.database_info['DBname-station_HISTORY'])
                
                self.DBclient.drop_database(self.database_info['DBname-network'])   
                self.DBclient.create_database(self.database_info['DBname-network'])
                
                self.DBclient.drop_database(self.database_info['DBname-train'])   
                self.DBclient.create_database(self.database_info['DBname-train'])
                
                self.DBclient.drop_database(self.database_info['DBname-network'])   
                self.DBclient.create_database(self.database_info['DBname-network'])
                
                self.DBclient_alarm.drop_database(self.database_info['DBname-alert_record'])
                self.DBclient_alarm.create_database(self.database_info['DBname-alert_record'])
                
                self.DBclient.drop_database(self.database_info['DBname-alert_check'])
                self.DBclient.create_database(self.database_info['DBname-alert_check'])
                
                self.DBclient_alert_settle.drop_database(self.database_info['DBname-alert_settled'])
                self.DBclient_alert_settle.create_database(self.database_info['DBname-alert_settled'])
                
                self.DBclient.drop_database(self.database_info['DBname-alert_time_consuming'])
                self.DBclient.create_database(self.database_info['DBname-alert_time_consuming'])

                self.DBclient_collect_time.drop_database(self.database_info['DBname-collect_time'])
                self.DBclient_collect_time.create_database(self.database_info['DBname-collect_time'])

                self.DBclient_data_process_time.drop_database(self.database_info['DBname-process_time'])
                self.DBclient_data_process_time.create_database(self.database_info['DBname-process_time'])

                self.DBclient.drop_database(self.database_info['DBname-write_time'])
                self.DBclient.create_database(self.database_info['DBname-write_time'])
                
            print('database connected')    
        except Exception as e:
            print('database connect fail:' + str(e))           
   
        
    ##################################################################################################################     
        
    def DataBase_train_send(self, DBname, deviceIP, Channel, line, train_num, dell_time, pass_frequency):
        # send data to data base
        self.DBclient.switch_database(DBname) 
        train_data = \
        [
            {
                "measurement":             DBname,
                "tags": 
                {
                    "DeviceIP":            deviceIP,
                    "Channel":             Channel,
                    "Line":                line
                },
                "fields": 
                {
                    "train_num":           train_num,
                    "dell_time":           dell_time,
                    "pass_frequency":      pass_frequency
                }
            }
        ]
        self.DBclient.write_points(train_data)
           
    ##################################################################################################################     
        
    def DataBase_network_send(self, DBname, deviceIP, connection_status, current_time):
        self.DBclient.switch_database(DBname)
        network_data = \
         [
            {
                "measurement":             DBname,
                "tags": 
                {
                    "DeviceIP":            deviceIP
                },
                "time":                    current_time,
                "fields":                 
                {
                    "connection_status":   connection_status
                }
            }
        ]
        self.DBclient.write_points(network_data)
        
 
    ##################################################################################################################

    def kill_service_by_name(self, service_name):
    # 遍历所有的进程
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                # 检查进程名称是否匹配
                if proc.info['name'] == service_name:
                    # print(f"Killing process {proc.info['name']} with PID {proc.info['pid']}")
                    proc.kill()  # 杀死进程
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass  # 处理不存在的进程或访问被拒绝的情况

    def Modbustcp_close(self):
        self.TCPclient.close()


    def separation_table(self):
        # 备份历史记录数据库,分离出一个新表
        self.sep_event.wait()
        if self.sep_event.is_set():
            now = datetime.now()
            min = 0
            #create a influxdb client
            sep_client = InfluxDBClient(self.database_info['host'], self.database_info['port'], self.database_info['username'], self.database_info['password'])
            yesterday = (now - timedelta(1)).strftime('%Y-%m-%d')
            day_before_yesterday = (datetime.now() - timedelta(2)).strftime('%Y-%m-%d')
            yesterday_utc = (now - timedelta(1)).strftime('%Y-%m-%dT16:00:00Z')
            self.source_measurement = self.database_info['DBname-station_HISTORY']
            new_measurement = self.source_measurement + "_" + now.strftime('%Y%m%d')
            new_measurement = new_measurement.replace(now.strftime('%Y%m%d'), yesterday.replace('-', '')) 
            # self.new_HISTORY_set["measurement"] = new_measurement
            # self.new_HISTORY_set["tags"] = {"line": self.line, "Channel": self.channel, "StationName": self.station}
            # self.new_HISTORY_set["time"] = self.alarm_time_UTC
            # self.new_HISTORY_set["fields"] = self.door_HISTORY_set
            # new_history_data = [self.new_HISTORY_set]
            #self.DBclient.switch_database(self.source_measurement)
            sep_client.switch_database(self.source_measurement)
            while True: 
                start_time = datetime.now() 
                # if(self.sep == True):
                #     # 结束线程 
                #     sep_client.close()
                #     self.sep_event.clear()     
                #     self.create_sep_thread = True
                #     break
                try:
                    # yesterday_utc = (datetime.now(pytz.utc) - timedelta(1)).strftime('%Y-%m-%dT%H:%M:%SZ')
                    # day_before_yesterday = (datetime.now() - timedelta(2)).strftime('%Y-%m-%d')
                    # day_day_before_yesterday = (datetime.now() - timedelta(3)).strftime('%Y-%m-%d')
                    # day_before_yesterday_utc = (datetime.now(pytz.utc) - timedelta(2)).strftime('%Y-%m-%dT%H:%M:%SZ')
                    # Two_days_later = (self.now_datetime + timedelta(2)).strftime('%Y-%m-%d')
                    pass_one_day = parser.parse("{}T16:00:00Z".format(day_before_yesterday))
                    days_from = pass_one_day + timedelta(minutes=(min))
                    days_to = pass_one_day + timedelta(minutes=(min+1))
                    days_from = days_from.strftime('%Y-%m-%dT%H:%M:%SZ')
                    days_to = days_to.strftime('%Y-%m-%dT%H:%M:%SZ')
                    #############################################################################################################
  
                    #开始是UTC时间 16：00：00 t 从 0 开始
                    query_separate = f"SELECT * FROM {self.source_measurement} WHERE time > '{days_from}' and time <= '{days_to}'"
                    print(query_separate)
                    result = sep_client.query(query_separate)
                    acc = 0
                    queryagain = 0
                    points = []
                    while True:
                        if(len(result) > 0):
                            points = list(result.get_points())   
                            break
                        elif(queryagain > 0):
                            break
                        time.sleep(0.2)
                        acc += 1
                        if(acc > 20):
                            query_separate = f"SELECT * FROM {self.source_measurement} WHERE time > '{days_from}' and time <= '{days_to}'"
                            print(query_separate)
                            result = sep_client.query(query_separate)
                            acc = 0
                            time.sleep(1)
                            queryagain = 1                                      
                    # 获取新point
                    new_points = []  
                    i = 0
                    for point in points: 
                        fields = {}
                        for relay in self.relay_values:
                            fields[relay] = point[relay]
                        fields['alert'] = point['alert']
                        # 假设 point 是一个字典，包含了 time, tags, 和 fields  
                        new_point = {  
                            "measurement": new_measurement,  
                            "tags": {"line": point['line'], "Channel": point['Channel'], "StationName": point['StationName']},
                            "fields": fields,
                            "time": point['time']
                        }
                        self.new_measurement = new_measurement
                        new_points.append(new_point) 
                        i+=1
                    if new_points:
                        sep_client.write_points(new_points)
                    min += 1
                    if min >= 24*60: 
                        # 分完但未清表 
                        min = 0
                        self.sep = True      
                        self.dele = False 
                        self.create_sep_thread = True
                         # 结束线程 
                        sep_client.close()
                        self.sep_event.clear()     
                        break 
                except Exception as e:
                    self.sep = True
                    self.dele = True
                    print('run to write points exception:' + str(e))
                    traceback.print_exc()

                end_time = datetime.now() 
                sep_time = end_time - start_time
                milliseconds_sep_time = sep_time.total_seconds() * 1000
                if self.debug_monitor:
                    self.monitor_object.DataBase_sep_table_time_send(milliseconds_sep_time, self.alarm_time_UTC)
         
    ##################################################################################################################             

    def data_process(self):
        # 进行数据处理
        start_time = datetime.now()  
        alarm_time_UTC  = datetime.utcnow()

        now = datetime.now()   
        current_minute = now.minute
        terminal_time = now.strftime("%H:%M:%S")
        # terminal_date = now.strftime('%Y-%m-%d %H:%M:%S')
        local_time = time.localtime(time.time())  
        export_time = self.dict_cfg['InitialConfig']['export_time'] 
        export_db_start_time = self.dict_cfg['InitialConfig']['export_db_start_time']    
        export_db_end_time = self.dict_cfg['InitialConfig']['export_db_end_time']  
        delete_db_start_time = self.dict_cfg['InitialConfig']['delete_db_start_time']
        delete_db_end_time = self.dict_cfg['InitialConfig']['delete_db_end_time']
        end_time = time.time()
        # separate_time = "00:00:00"
        exp_time = datetime.strptime(export_db_start_time, "%H:%M:%S")  
        one_minute = timedelta(minutes=1)  
        create_sep_time = exp_time - one_minute  
        create_sep_time = create_sep_time.strftime("%H:%M:%S") 
        self.debug_monitor = self.dict_cfg["InitialConfig"]["debug_monitor"]

        # create_sep_time <= terminal_time < "23:59:59"
        if (create_sep_time <= terminal_time < export_db_start_time) and (self.create_sep_thread == True):
            sep = threading.Thread(target=self.separation_table)
            sep.start()
            self.create_sep_thread = False

        
        self.LINE = {"上行": 'S', "下行": 'X'}
        station_info = self.dict_cfg['TerminalClient']['T1']['station']
        StationNames = list(station_info.keys())
        for station in StationNames:
            lines = list(station_info[station].keys())
            for line in lines:
                door_set, base_set = {}, {}
                door_SAVE_set, base_SAVE_set = {}, {}
                self.door_HISTORY_set, base_HISTORY_set = {}, {}
                new_door_HISTORY_set, new_HISTORY_set = {}, {}
                RELAY, CHANNEL, CLASS = station_info[station][line]['relay'], station_info[station][line]['channel'], station_info[station][line]['class']
                NAME = station_info[station][line]['name']
                self.name = NAME
                self.relay = RELAY
                line_bool = self.LINE[line]
                # 获取动作逻辑配置
                LOGIC = station_info[station][line]['ActionLogic']
                logic_num = len(LOGIC)
                # 获取继电器端口采集数据
                relay_keys = list(RELAY.keys())
                self.relay_values = list(RELAY.values())
                channel = CHANNEL
                channel_values = list(channel.values())
                ch_ls = ['CH' + str(ch) for ch, group in groupby(sorted(channel_values))]
                ch_num = [str(ch) for ch, group in groupby(sorted(channel_values))]
                channel = ' '.join(ch_ls) 
                ch_num = ''.join(ch_num)
                ch_num = int(ch_num)
                for ch in channel_values:
                    collect = self.data_connect(self.data_item, RELAY, 'CH' + str(ch))
                    collect = [int(item) for item in collect]
                    for i in self.relay_values:
                        matching_key = next(k for k, v in RELAY.items() if v == i)
                        door_set[i] = collect[int(self.relay_values.index(i))]
                        self.door_HISTORY_set[i] = collect[int(self.relay_values.index(i))]
                        new_door_HISTORY_set[i] = collect[int(self.relay_values.index(i))]
                        door_SAVE_set[i] = collect[int(self.relay_values.index(i))]  
                        
                # 排序处理
                collect_ = {str(num1).zfill(2): num2 for num1, num2 in zip(relay_keys, collect)}
                collect_sorted = sorted(collect_.keys())
                collect_sorted_values = [collect_[key] for key in collect_sorted]

                # 判断继电器序列中所有机电电源端口和信号电源端口
                relay =  OrderedDict(sorted(RELAY.items()))
                relay_ls = list(relay.values())
                port_class = OrderedDict(sorted(CLASS.items()))
                port_class = {value: list(key for key, val in port_class.items() if val == value) for value in port_class.values()}
                EM_power, S_power = [], []
                for key in port_class: 
                    if key == 2:
                        for i in port_class[2]:
                            EM_power.append(relay[str(i)])
                    if key == 3:
                        for j in port_class[3]:
                            S_power.append(relay[str(j)])         
                EM_power_ls, S_power_ls = [], []
                collect_ls = collect_sorted_values
                for i in EM_power:
                    EM_power = int(collect_sorted_values[relay_ls.index(i)])
                    EM_power_ls.append(EM_power)
                for j in S_power:       
                    S_power = int(collect_sorted_values[relay_ls.index(j)])
                    S_power_ls.append(S_power)
                                
                # 进去逻辑判断报警 
                if ((EM_power_ls != []) or (S_power_ls != [])) and ((0 in EM_power_ls) or (0 in S_power_ls)):
                    alert = ["异常", "Warning"]
                    alert_mark = 0
                    door_set["AlertMark"] = alert_mark
                    door_SAVE_set["AlertMark"] = alert_mark
                    door_set["alert"] = alert[0] 
                    self.door_HISTORY_set["alert"] = alert[0]
                    new_door_HISTORY_set["alert"] = alert[0]
                    self.alert_event.set()
                
                                        
                    # 判断机电电源和信号电源是否异常
                    if (0 in EM_power_ls):
                        state = ["机电电源异常", "EM Power Warning"]
                        door_set["Note"] = state[0]
                        door_SAVE_set["Note"] = state[1]
                        self.door_nop.append(door_set['Note'])
                        if self.EM_alert_flag:
                            self.normal_flag = True
                            self.warning_flag = True
                            self.undefine = True
                            self.CanNotJudge = True
                            self.EM_alert_flag = False
                            self.S_alert_flag = True
                            self.EM_S_alert_flag = True
                            self.alarm_record = state[0]
                            self.alarm_object.alert_n(self.alarm_record, station, line, self.alarm_time_UTC)
                               
                    if (0 in S_power_ls):
                        state = ["信号电源异常", "Signal Power Warning"]
                        door_set["Note"] = state[0]  
                        door_SAVE_set["Note"] = state[1]
                        self.door_nop.append(door_set['Note']) 
                        if self.S_alert_flag:
                            self.normal_flag = True
                            self.warning_flag = True
                            self.undefine = True
                            self.CanNotJudge = True
                            self.EM_alert_flag = True
                            self.S_alert_flag = False
                            self.EM_S_alert_flag = True
                            self.alarm_record = state[0]
                            self.alarm_object.alert_n(self.alarm_record, station, line, self.alarm_time_UTC)
                                    
                    if (0 in EM_power_ls) and (0 in S_power_ls):
                        state = ["机电/信号电源异常", "EM/Signal Power Warning"]
                        door_set["Note"] = state[0]     
                        door_SAVE_set["Note"] = state[1]
                        self.door_nop.append(door_set['Note'])
                        if self.EM_S_alert_flag:
                            self.normal_flag = True
                            self.warning_flag = True
                            self.undefine = True
                            self.CanNotJudge = True
                            self.EM_alert_flag = True
                            self.S_alert_flag = True
                            self.EM_S_alert_flag = False
                            self.alarm_record = state[0]
                            self.alarm_object.alert_n(self.alarm_record, station, line, self.alarm_time_UTC)
                                
                elif (logic_num != 0):
                    for lgc in range(logic_num):
                        # 解析配置文件
                        SEQUENCE, STATE, LABEL = LOGIC[str(lgc)]["sequence"], LOGIC[str(lgc)]["state"], LOGIC[str(lgc)]["label"]
                        sequence = OrderedDict(sorted(SEQUENCE.items()))
                        sequence_ls = list(sequence.values())
                        # 判断动作状态
                        if len(sequence_ls) == len(collect_ls) and (all(sequence_ls == collect_ls for sequence_ls, collect_ls in zip(sequence_ls, collect_ls)) and (LABEL == 1)):
                            door_set["Note"] = STATE
                            self.door_nop.append(door_set['Note'])
                            alert = ["正常", 'Normal']
                            alert_mark = LABEL 
                            door_set["AlertMark"] = alert_mark
                            door_SAVE_set["AlertMark"] = alert_mark
                            door_SAVE_set["Note"] = 'normal'
                            self.alarm_object.alert_m(self.alert, self.station, self.line, self.alarm_time_UTC)

                        if len(sequence_ls) == len(collect_ls) and (all(sequence_ls == collect_ls for sequence_ls, collect_ls in zip(sequence_ls, collect_ls)) and (LABEL == 0)):
                            door_set["Note"] = STATE
                            self.door_nop.append(door_set['Note'])
                            alert = ["异常", "Warning"]
                            alert_mark = LABEL
                            door_set["AlertMark"] = alert_mark
                            door_SAVE_set["AlertMark"] = alert_mark
                            door_SAVE_set["Note"] = 'warning'
                            self.alert_event.set()
                                                               
                        if len(sequence_ls) == len(collect_ls) and (all(sequence_ls != collect_ls for sequence_ls, collect_ls in zip(sequence_ls, collect_ls))):
                            state = ["无法判断", "Unable"]
                            door_set["Note"] = state[0]
                            door_SAVE_set["Note"] = state[1]
                            self.door_nop.append(door_set['Note'])
                            alert = ["异常", "Warning"]
                            alert_mark = -1
                            door_set["AlertMark"] = alert_mark
                            door_SAVE_set["AlertMark"] = alert_mark
                            door_set["alert"] = alert[0] 
                            self.door_HISTORY_set["alert"] = alert[0]
                            new_door_HISTORY_set["alert"] = alert[0]
                            self.alert_event.set()
                            self.alert = alert[0]
                            self.station = station
                            self.line = line
                            self.alert_event.set()

                elif logic_num == 0:
                    state = ["正常", 'Normal']
                    door_set["Note"] = state[0]
                    door_SAVE_set["Note"] = state[1]
                    alert = ["正常", 'Normal']
                    alert_mark = 1
                    door_set["AlertMark"] = alert_mark
                    door_SAVE_set["AlertMark"] = alert_mark
                    door_set["alert"] = alert[0] 
                    self.door_HISTORY_set["alert"] = alert[0]
                    new_door_HISTORY_set["alert"] = alert[0]
                    self.window_show_flag = True
                    if self.undefine:
                        self.normal_flag = True
                        self.warning_flag = True
                        self.undefine = True
                        self.CanNotJudge = True
                        self.EM_alert_flag = True
                        self.S_alert_flag = True
                        self.EM_S_alert_flag = True
                        self.alarm_object.alert_m(self.alert, self.station, self.line, self.alarm_time_UTC)
        
                # 整理成统一形式                                                    
                door_set["deviceIP"] = self.Device_IP  
                base_set["measurement"] = self.database_info['DBname-station']                
                base_set["tags"] = {"line": line, "Channel": channel, "StationName": station}            
                base_set["time"] = alarm_time_UTC
                base_set["fields"] = door_set
                                                        
                door_SAVE_set["StationName"] = ''.join([word[0] for word in pinyin(station, style=Style.NORMAL)])
                base_SAVE_set["measurement"] = self.database_info['DBname-station_SAVE']
                base_SAVE_set["tags"] = {"line": line_bool, "Channel": channel}
                base_SAVE_set["time"] = alarm_time_UTC
                base_SAVE_set["fields"] = door_SAVE_set
                            
                base_HISTORY_set["measurement"] = self.source_measurement
                base_HISTORY_set["tags"] = {"line": line, "Channel": channel, "StationName": station}
                base_HISTORY_set["time"] = alarm_time_UTC
                base_HISTORY_set["fields"] = self.door_HISTORY_set
                                
                # Getting effective data to database
                monitor_data = base_set
                save_data = base_SAVE_set
                history_data = base_HISTORY_set
                #print(monitor_data)

                self.new_HISTORY_set = new_HISTORY_set
                self.alarm_time_UTC = alarm_time_UTC
                self.line = line
                self.channel = channel
                self.station = station
                self.alert = alert[0]

                ##################################################################################

                # 需要写入数据库的数据 
                self.write_cache_from_station.push(monitor_data)
                # self.write_cache_from_station_SAVE.push(save_data)
                self.write_cache_from_station_HISTORY.push(history_data) 

        # if current_minute == 0:
        #       # connect to database 
        #     station = self.database_info['DBname-station']
        #     self.DBclient.switch_database(station)
        #     query_retention = f'DROP RETENTION POLICY "station_retention" ON "{station}"'
        #     self.DBclient.query(query_retention) 
        #     query_retention = f'CREATE RETENTION POLICY "station_retention" ON "{station}" DURATION 1h REPLICATION 1 DEFAULT'
        #     self.DBclient.query(query_retention) 
        #     self.DBclient.switch_database(station)
        #     # alert record"
        #     station = self.database_info["DBname-alert_record"]
        #     self.DBclient_alarm.switch_database(station)
        #     query_retention = f'DROP RETENTION POLICY "alert_record_retention" ON "{station}"'
        #     self.DBclient_alarm.query(query_retention) 
        #     query_retention = f'CREATE RETENTION POLICY "alert_record_retention" ON "{station}" DURATION 1h REPLICATION 1 DEFAULT'
        #     self.DBclient_alarm.query(query_retention) 
        #     self.DBclient_alarm.switch_database(station)
        #     # alert settle
        #     station = self.database_info["DBname-alert_settled"]
        #     self.DBclient_alert_settle.switch_database(station)
        #     query_retention = f'DROP RETENTION POLICY "alert_settled_retention" ON "{station}"'
        #     self.DBclient_alert_settle.query(query_retention) 
        #     query_retention = f'CREATE RETENTION POLICY "alert_settled_retention" ON "{station}" DURATION 1h REPLICATION 1 DEFAULT'
        #     self.DBclient_alert_settle.query(query_retention) 
        #     self.DBclient_alert_settle.switch_database(station)
        #     # collect time
        #     station = self.database_info["DBname-collect_time"]
        #     self.DBclient_collect_time.switch_database(station)
        #     query_retention = f'DROP RETENTION POLICY "collect_time_retention" ON "{station}"'
        #     self.DBclient_collect_time.query(query_retention) 
        #     query_retention = f'CREATE RETENTION POLICY "collect_time_retention" ON "{station}" DURATION 1h REPLICATION 1 DEFAULT'
        #     self.DBclient_collect_time.query(query_retention)
        #     self.DBclient_collect_time.switch_database(station)
        #     # data process
        #     station = self.database_info["DBname-process_time"]
        #     self.DBclient_data_process_time.switch_database(station)
        #     query_retention = f'DROP RETENTION POLICY "process_time_retention" ON "{station}"'
        #     self.DBclient_data_process_time.query(query_retention) 
        #     query_retention = f'CREATE RETENTION POLICY "process_time_retention" ON "{station}" DURATION 1h REPLICATION 1 DEFAULT'
        #     self.DBclient_data_process_time.query(query_retention)
        #     self.DBclient_data_process_time.switch_database(station)
    
        if (export_db_start_time < terminal_time <= export_db_end_time) and (self.sep == False):        # 到时间且未进行分表 
            self.new_HISTORY_set = new_HISTORY_set
            self.alarm_time_UTC = alarm_time_UTC
            self.line = line
            self.channel = channel
            self.station = station
            self.alert = alert[0]
            self.sep_event.set()
            self.sep = True
        else:
            if self.debug_monitor:
                self.monitor_object.DataBase_sep_table_time_send(0, alarm_time_UTC)
     
        if (delete_db_start_time < terminal_time <= delete_db_end_time) and (self.dele == False):     # 到时间且未进行清表则执行
            yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
            day_before_yesterday_utc = (datetime.now(pytz.utc) - timedelta(2)).strftime('%Y-%m-%dT%H:%M:%SZ')
            try:
                # 清除前天表历史记录中数据
                # query_delete = f"DELETE FROM {self.source_measurement} WHERE time > '{day_before_yesterday_utc}' - 1d"
                # self.DBclient.query(query_delete) 
                # 进入当天，已进行清表，重新置分表flag，等待下次到时间进入
                self.dele = True  
                self.sep = False
            except:
                self.dele = False
                self.sep = False
        end_time = datetime.now()  
        # process_time = end_time - start_time
        #milliseconds_process_time = process_time.total_seconds() * 1000
        #self.DataBase_process_time_send("process_time", milliseconds_process_time, alarm_time_UTC)

        process_time = end_time - start_time
        milliseconds_process_time = process_time.total_seconds() * 1000
        if self.debug_monitor:
            self.monitor_object.DataBase_process_time_send(milliseconds_process_time, alarm_time_UTC)
           
    
    def write_databases(self):
        while True:
            alarm_time_UTC  = datetime.utcnow()
            start_time = datetime.now() 

            # 送入“station”数据库
            if self.write_cache_from_station.is_empty() == False:
                self.write_cache_to_database(self.write_cache_from_station.pop(100), 'DBname-station') 
            if self.write_cache_from_station_HISTORY.is_empty() == False:  
                self.write_cache_to_database(self.write_cache_from_station_HISTORY.pop(100), 'DBname-station_HISTORY')
            # self.write_cache_to_database(self.write_cache_from_station_SAVE, 'DBname-station_SAVE') 

            if self.alarm_object.write_cache_from_alert_note.is_empty() == False:
                self.write_cache_to_alarm_database(self.alarm_object.write_cache_from_alert_note.pop(100))
            if self.alarm_object.write_cache_from_alert_settled.is_empty() == False:
                self.write_cache_to_alert_settle_database(self.alarm_object.write_cache_from_alert_settled.pop(100))
            
            if self.debug_monitor:
                if (self.monitor_object.write_cache_from_collect_data.is_empty() == False):
                    self.write_cache_to_collect_time_database(self.monitor_object.write_cache_from_collect_data.pop(100))
                if (self.monitor_object.write_cache_from_process_data.is_empty() == False):
                    self.write_cache_to_process_time_database(self.monitor_object.write_cache_from_process_data.pop(100))
                if (self.monitor_object.write_cache_from_sep_table_data.is_empty() == False):
                    self.write_cache_to_sep_table_time_database(self.monitor_object.write_cache_from_sep_table_data.pop(100))
                    #shiji zhege danci shi cache bushi cache

            end_time = datetime.now()  
            write_time = end_time - start_time

            if write_time.total_seconds() < 1:
                sleepT = 1 - write_time.total_seconds()
                time.sleep(sleepT)

            if self.debug_monitor:
                end_time = datetime.now()  
                write_time = end_time - start_time
                milliseconds_write_time = write_time.total_seconds() * 1000
                self.monitor_object.DataBase_write_time_send(milliseconds_write_time, alarm_time_UTC)
                if (self.monitor_object.write_cache_from_write_db_data.is_empty() == False):
                    self.write_cache_to_write_db_time_database(self.monitor_object.write_cache_from_write_db_data.pop(100))
            

    def write_cache_to_database(self, cache, db_name):
        self.DBclient.switch_database(self.database_info[db_name])
        #for point in cache:
        try:
            self.DBclient.write_points(cache)
        except Exception as e:
            print("写入失败:" + str(self.database_info[db_name]) )

    def write_cache_to_alarm_database(self, cache):
        try:
            self.DBclient_alarm.write_points(cache)
        except Exception as e:
            print("写入失败:" + str(self.database_info["DBname-alert_record"]) )

    def write_cache_to_alert_settle_database(self, cache):
        try:
            self.DBclient_alert_settle.write_points(cache)
        except Exception as e:
            print("写入失败:" + str(self.database_info["DBname-alert_settled"]) )

    def write_cache_to_collect_time_database(self, cache):
        try:
            self.DBclient_collect_time.write_points(cache)
        except Exception as e:
            print("写入失败:" + str(self.database_info["DBname-collect_time"]) )
    
    def write_cache_to_process_time_database(self, cache):
        try:
            self.DBclient_data_process_time.write_points(cache)
        except Exception as e:
            print("写入失败:" + str(self.database_info["DBname-process_time"]) )

    def write_cache_to_sep_table_time_database(self, cache):
        try:
            self.DBclient_data_process_time.write_points(cache)
        except Exception as e:
            print("写入失败:" + str(self.database_info["DBname-sep_table_time"]) )

    def write_cache_to_write_db_time_database(self, cache):
        try:
            self.DBclient_write_db_time.write_points(cache)
        except Exception as e:
            print("写入失败:" + str(self.database_info["DBname-write_time"]) )

    ################################################################################################################## 
    
    def main(self, clients, cycle, continous, network_connect_flag, base_time):
        Device_IP, Device_Port = clients[1], clients[2]
        IP_num = int(Device_IP.split('.')[-1])

        # IP地址和端口号
        self.Device_IP = Device_IP
        # 创建Modbus TCP客户端
        fix_source_address = (Device_IP, 8888)
        # 创建Modbus TCP客户端
        self.TCPclient = ModbusTcpClient(Device_IP, Device_Port)
        # # 获取底层的socket对象
        # sct = self.TCPclient.socket
        # # 设置源地址
        # sct.bind(fix_source_address)

        TCP_connect_flag = self.TCPclient.connect()

        # 初始化下位机发送重启命令
        # if TCP_connect_flag:
        #     print("Connected to Modbus Server")  

        #     address = 80  # 40080 - 40000  
        #     value = 65535  
      
        #     # 构造写入请求  
        #     request = WriteSingleRegisterRequest(address, value)  
      
        #     # 发送请求（注意：这通常不是直接调用 request 的方式，这里仅为展示如何构造请求）  
        #     # 实际上，应该使用 client.write_register 方法  
        #     response = self.TCPclient.write_register(address, value, unit=1)  # unit 是从站地址，默认为 1  
      
        #     # 检查响应  
        #     if not response.isError():  
        #         print("Register written successfully")  
        #     else:  
        #         print("Failed to write register")  
      
        #     # 关闭连接  
        #     self.TCPclient.close()  
        # else:  
        #     print("Failed to connect to Modbus Server")
        # time.sleep(3)
        # TCP_connect_flag = self.TCPclient.connect()

        ###############################################################################################################

        change_flag = True
        # 过车（目前不用）
        train_num = 0
        dwell_time = 0
        pass_frequency = 0
        count_num = 0
        n = 0
        beginning_time = timeit.default_timer() 
        start_time = time.time()
        time_info = start_time
        # 报警切换标志位
        self.normal_flag = False
        self.warning_flag = True
        self.CanNotJudge = True
        self.undefine = False
        self.EM_alert_flag = True
        self.S_alert_flag = True
        self.EM_S_alert_flag = True
        self.door_alert_flag = True
        self.alarm_record = ""

        try:
            while True:  
                try:
                    start_time = datetime.now()  
                    alarm_time = datetime.now()
                    self.debug_monitor = self.dict_cfg["InitialConfig"]["debug_monitor"]
                    if base_time:
                        alarm_time_UTC = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
                    count_num += 1

                    ##################################################################################################
                
                    if TCP_connect_flag == False:   
                        print("device: {} disconnect".format(clients[0]))  
                        TCP_connect_flag = self.TCPclient.connect()
                        #time.sleep(1)
                        count_num = 0 
                        alarm_time_UTC  = datetime.utcnow()
                        self.time_count_connect.append(alarm_time_UTC)
                        connection_status = "网络连接中断"
                        DBname_network = self.database_info['DBname-network']
                        self.time_count_connect.append(alarm_time_UTC)
                        self.DataBase_network_send(DBname_network, clients[1], connection_status, self.time_count_connect[0])
                        if TCP_connect_flag:
                            count_num = 1         
                   
                    else:
                        pass
                    alarm_time_UTC  = datetime.utcnow()
                    if count_num == 1:
                        network_connect_flag = True
                
                    # 判断网络是否连接
                    self.time_count_connect.clear()
                    if network_connect_flag:
                        alarm_time_UTC  = datetime.utcnow()
                        self.time_count_connect.append(alarm_time_UTC)
                        connection_status = "网络已连接"
                        DBname_network = self.database_info['DBname-network']
                        self.DataBase_network_send(DBname_network, clients[1], connection_status, self.time_count_connect[0])
                        network_connect_flag = False

                    
                    ###################################################################################################
                
                    # reading Modbus
                    read_result = self.TCPclient.read_holding_registers(0, 100)  

                    # running time counter
                    count_time = timeit.default_timer() 
                    total_time = count_time - beginning_time
                    total_time = int(total_time)
    
                    # every data are not erro or void, then display
                    if not read_result.isError():   
                        # getting data that starts with modbus bit 1 from the registers
                        read_result = read_result.registers
                        # 读到的所有寄存器数据
                        self.data_item = \
                        {
                            # 表头
                            # 'head':  "0x" + str(hex(read_result[0])[2:]) + str(hex(read_result[1])[2:]),
                            # 日期时间校正
                            # 'Y':      self.ff(read_result[2], 'Y'),
                            # 'M':      self.ff(read_result[2], 'M'),
                            # 'D':      self.ff(read_result[3], 'D'),
                            # 'H':      self.ff(read_result[3], 'H'),
                            # 'Q':      self.ff(read_result[4], 'Q'),
                            # 'S':      self.ff(read_result[4], 'S'),
                            # 'ms':     self.ff(read_result[5], 'ms'),
                            # 各通道数据读取
                            'CH1':   '{:016b}'.format(read_result[7]) + '{:016b}'.format(read_result[6]),
                            'CH2':   '{:016b}'.format(read_result[9]) + '{:016b}'.format(read_result[8]),
                            'CH3':   '{:016b}'.format(read_result[11]) + '{:016b}'.format(read_result[10]),
                            'CH4':   '{:016b}'.format(read_result[13]) + '{:016b}'.format(read_result[12])
                        }

                        if base_time == False:
                            alarm_time_UTC = datetime(2000 + self.data_item['Y'], self.data_item['M'], self.data_item['D'], self.data_item['H'] - 8, self.data_item['Q'], self.data_item['S'])
                            alarm_time_UTC = alarm_time_UTC.strftime("%Y:%m:%d %H:%M:%S" + ".{}".format(self.data_item['ms']))
              
                        self.data_process()
                        
                    ##################################################################################################
                
                except ModbusException as e:
                    # 捕获modebus相关异常并处理
                    print('reconnecting...' + str(e))
                    TCP_connect_flag = False
                    self.handle_modbus_exception(e)
                    if self.TCPclient is not None:
                        self.TCPclient.close()
                    time.sleep(4)   
                    
                except Exception as e:
                    print('reconnecting...' + str(e))
                    TCP_connect_flag = False
                    if self.TCPclient is not None:
                        self.TCPclient.close()
                    time.sleep(4)  

                      
                # if (delete_db_start_time < terminal_time <= delete_db_end_time) and (self.dele == False):     # 到时间且未进行清表则执行
                #     day_before_yesterday_utc = (datetime.now(pytz.utc) - timedelta(2)).strftime('%Y-%m-%dT%H:%M:%SZ')
                #     try:
                #         # 清除前天表历史记录中数据
                #         query_delete = f"DELETE FROM {self.source_measurement} WHERE time > '{day_before_yesterday_utc}' - 1d"
                #         self.DBclient.query(query_delete) 
                #         self.dele = True  # 进入当天，已进行清表，重新置分表flag，等待下次到时间进入
                #     except:
                #         self.dele = False

        #             try:
        #                 # 从influxdb导出CSV格式文件操作
        #                 export_path = 'D:\\PSDmonitor\\InfluxDB\\export_data'
        #                 folder_path = 'D:\\PSDmonitor\\InfluxDB\\influxdb-1.7.7-1'  
        #                 influx_exe = '.\influxd.exe'  
        #                 database = self.database_info['DBname-station_SAVE']

        #                 #####################################################################################

        #                 for station in self.StationName:
        #                     # 构造完整路径  
        #                     package_path = os.path.join(export_path, station)  
        #                     # 创建文件夹  
        #                     try:  
        #                         os.makedirs(package_path)  
        #                         print(f"文件夹 '{package_path}' 创建成功")  
        #                     except FileExistsError:  
        #                         print(f"文件夹 '{package_path}' 已存在")  
        #                     except Exception as e:  
        #                         print(f"创建文件夹时发生错误: {e}")

        #                     #####################################################################################

        #                     for line in self.line:
        #                         time_info = end_time
        #                         file_name = station + '_' + line + '_' + end_time_info_minus_one_day
        #                         output_file = '{}.csv'.format(file_name)
        #                         direction = self.LINE[line]
        #                         StationName = ''.join([word[0] for word in pinyin(station, style=Style.NORMAL)])
        #                         query = f"SELECT * FROM {database} WHERE StationName = \'{StationName}\' AND line = \'{direction}\'"
        #                         # 构建 PowerShell 命令字符串  
        #                         powershell_cmd = f"""  
        #                         cd '{folder_path}'  
        #                         & '{influx_exe}' -database '{database}' -execute "{query}" -format csv > '{output_file}'  
        #                         """  
        #                         # 使用 subprocess 调用 PowerShell 并执行命令  
        #                         process = subprocess.Popen(['powershell', '-Command', powershell_cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE)  
        #                         stdout, stderr = process.communicate()  
        #                         if process.returncode != 0:  
        #                             print(f"Error executing PowerShell commanD: {stderr.decode()}")  
        #                         else:  
        #                             print("Command executed successfully.") 
        #                             # 设置CSV文件和Excel文件的路径  
        #                             csv_file_path = folder_path + '\\' + output_file  
        #                             excel_file_name = '{}.xlsx'.format(file_name) 
        #                             # 确保Excel文件夹存在，如果不存在则创建  
        #                             if not os.path.exists(export_path):  
        #                                 os.makedirs(export_path) 
        #                             # 读取CSV文件  
        #                             df = pd.read_csv(csv_file_path, encoding='utf-16') 
        #                             # 删除源CSV文件
        #                             if os.path.exists(csv_file_path):  
        #                                 os.remove(csv_file_path) 
        #                             # 将DataFrame写入Excel文件，保存到指定文件夹  
        #                             excel_file_path = os.path.join(package_path, excel_file_name)  
        #                             df.to_excel(excel_file_path, index=False)  
        #                             df = df.drop(df.columns[0], axis=1) 
        #                             df.to_excel(excel_file_path, index=False) 
        #                             # 时间戳转换
        #                             df = pd.read_excel(excel_file_path) 
        #                             df['s']=df['time'].apply(lambda x:int(str(x)[:10]))
        #                             df['s'] = df['s'] + 28800
        #                             df['time'] = pd.to_datetime(df['s'],unit='s')
        #                             del df['s']
        #                             df['time'] = df['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        #                             df.to_excel(excel_file_path, index=False)  
        #                             # 处理一下表头
        #                             heads = df.columns.tolist()
        #                             for key, value in self.relay.items(): 
        #                                 for head in heads:
        #                                     if value == head:  
        #                                         index_realy = heads.index(head) 
        #                                         heads[index_realy] = self.name[key]
        #                             new_heads = pd.DataFrame([heads], columns=df.columns)  
        #                             df = pd.concat([new_heads, df], ignore_index=True)
        #                             df.to_excel(excel_file_path, index=False) 
        #                             original_excel_path = excel_file_path
        #                             new_excel_path = excel_file_path
        #                             df = pd.read_excel(original_excel_path, skiprows=1) 
        #                             df.to_excel(new_excel_path, index=False)  

        #                 # 清除导出缓存库
        #                 self.DBclient.drop_database(self.database_info['DBname-station_SAVE'])
        #                 self.DBclient.create_database(self.database_info['DBname-station_SAVE'])
                    
        #                 #####################################################################################   
                     
        #             except:
        #                 pass 
            
        #             # query_separate = f"SELECT * INTO {new_measurement} FROM {self.source_measurement} WHERE time > now() - '{now.strftime('%Y-%m-%d')}T16:00:00Z'"  
        #             # self.DBclient.query(query_separate)

   
                end_time = datetime.now()  
                collect_time = end_time - start_time
               # milliseconds_collect_time = collect_time.total_seconds() * 1000
               # self.DataBase_collect_time_send("collect_time", milliseconds_collect_time, alarm_time_UTC)
                # print('                          "采集运行时间:"    {}'.format(collect_time))

                if collect_time.total_seconds() < 0.18:
                    sleepT = 0.18 - collect_time.total_seconds()
                    time.sleep(sleepT)

                end_time = datetime.now()  
                collect_time = end_time - start_time
                milliseconds_collect_time = collect_time.total_seconds() * 1000
                if self.debug_monitor:
                    self.monitor_object.DataBase_collect_time_send(milliseconds_collect_time, alarm_time_UTC)   

        except Exception as e:
            print(f"捕获异常:{e}")
            # 在调试时确保清理操作被执行
            self.Modbustcp_close()

            ##################################################################################################################################################################### 
                
    def doConnect(host,port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try :
            sock.connect((host, port))
        except :
            pass
        return sock  
    
    def handle_modbus_exception(self, e):  
        if isinstance(e, ModbusIOException):  
            # 处理I/O异常，如连接问题  
            print("I/O Exception:" + str(e))  
            # 可以尝试重新连接或通知用户检查网络连接  
        elif isinstance(e, ModbusException):  
            # 处理其他Modbus异常  
            # 注意：这里不会直接区分Modbus协议中的具体异常代码  
            # 但你可以根据异常消息进行更细致的处理  
            print("Modbus Exception:" + str(e))  
            if "Illegal function" in str(e):  
                # 处理非法功能码异常  
                print("Illegal function requested.")  
            # ... 进行相应的处理  
            elif "Slave device or server failure" in str(e):  
                # 处理从设备或服务器故障  
                print("Slave device or server failure.")  
                # ... 进行相应的处理  
            # ... 可以添加更多基于消息内容的判断  
        else:  
            # 处理未知异常  
            print("Unknown Exception:" + str(e))  
                     
                   
    def main_thread(self, T_cls, T, cycle, continous, base_time):  
        connect_flag = True
        sk = None
       #time.sleep(T)   
        if connect_flag:
            for cls in T_cls.keys():
                clients = [cls, T_cls[cls]['host'], T_cls[cls]['port']]
                # starting thread to collect data
                network_connect_flag = True
                collect = threading.Thread(target=self.main, kwargs={'clients': clients, 'cycle': cycle, 'continous': continous, 'network_connect_flag': network_connect_flag, 'base_time': base_time})
                collect.start()
                
   
    def run(self, cycle, continous, base_time, save_all):    
        # conncting to the Influxdb 
        self.DataBase_connect(self.database_info['host'], self.database_info['port'], self.database_info['username'], self.database_info['password'], save_all)
        # ModbusTCP connecting and create threads in listenning network loop one by one
        T_cls = self.display_clients()['terminal']
        self.main_thread(T_cls, 0, cycle, continous, base_time)
        time.sleep(3)
        write_db = threading.Thread(target=self.write_databases)
        write_db.start()
        time.sleep(3)
        alert_thread = threading.Thread(target=self.play_alarm())
        alert_thread.start()
        print("starting thread")



class StartupApp:
    def __init__(self, root, background_image_path):
        self.root = root
        self.root.title("地铁屏蔽门监测系统")
        self.root.iconbitmap('D:\\PSDmonitor\\project\\subway\\PSDmonitor_MainProgramme\\fav32.ico')
        
        # Load background image
        self.background_image = Image.open(background_image_path)
        self.background_photo = ImageTk.PhotoImage(self.background_image)
        
        # Get image dimensions
        width, height = self.background_image.size
        
        # Set window size to match background image
        self.root.geometry(f"{width}x{height}")
        self.root.resizable(False, False)  # Disable window resizing
        
        # Place background image using Canvas
        self.canvas = tk.Canvas(root, width=width, height=height)
        self.canvas.create_image(0, 0, image=self.background_photo, anchor='nw')
        self.canvas.pack(fill=tk.BOTH, expand=True)
        
        # Create a progress bar
        self.progress_bar = ttk.Progressbar(root, orient='horizontal', mode='determinate', length=300)
        self.progress_bar.place(relx=0.5, rely=1.0, anchor='s', y=-30)
        self.progress_bar['value'] = 0
        
        # Start the loading process
        self.progress = 0
        self.update_progress_bar()


    def update_progress_bar(self):
        if self.progress < 100:
            self.progress += 1
            self.progress_bar['value'] = self.progress
            # Update every X ms
            self.root.after(50, self.update_progress_bar) 
        else:
            # Close the window after loading completes
            self.root.destroy()  

       
def app_close():
    # 释放文件锁
    if lock_file:
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        lock_file.close()


def acquire_lock(file_path):
    try:
        # 打开锁文件
        lock_file = open(file_path, 'w')
        msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)  # 非阻塞锁定
        return lock_file  # 返回锁文件句柄以防止文件被关闭
    except OSError:
        return None  # 如果锁定失败则返回 None

                
if __name__ == "__main__":
    # 获取文件锁
    lock_file_path = "app.lock"
    lock_file = acquire_lock(lock_file_path)
    if lock_file is None:
        print("另一个实例已经在运行，程序即将退出。")
        sys.exit(1)

    # create the terminal
    terminal = PSD_Monitoring()

    # 注册退出时调用
    atexit.register(terminal.Modbustcp_close)
    atexit.register(app_close)

    # kill process
    terminal.kill_service_by_name('influxd.exe')
    terminal.kill_service_by_name('grafana.exe')
    terminal.kill_service_by_name('msedge.exe')
    terminal.kill_service_by_name('WmiPrvSE.exe')    

    # starting up Influxdb 
    os.chdir('D:\\PSDmonitor\\InfluxDB\\influxdb-1.7.7-1')
    os.popen('influxd.exe -config influxdb.conf') 
    
    # starting up Grafana
    os.chdir('D:\\PSDmonitor\\grafana-enterprise-10.2.0.windows-amd64\\grafana-10.2.0\\bin')
    os.popen('grafana.exe server') 

    # Start the loading process
    root = tk.Tk()
    app = StartupApp(root, "D:\PSDmonitor\project\subway\PSDmonitor_MainProgramme\startup.png") 
    root.mainloop()

    # PyQt5 app init
    app = QApplication(sys.argv)
    w = QWidget()
    
    # open to the Web
    url = 'http://127.0.0.1/srp-frame-example-pbm/Pbm/index.html'
    edge_path = 'C:\\Program Files (x86)\\Microsoft\\Edge\Application\\msedge.exe' 
    subprocess.Popen([edge_path, url])
    time.sleep(4)
    pyautogui.press('F5')

    # starting up terminal
    Tc = terminal.Tc  # 采样周期
    base_time = terminal.base_time
    save_all = terminal.save_all
    terminal.run(cycle=Tc, continous=False, base_time=base_time, save_all=save_all)  

    while True:
        time.sleep(10)  
