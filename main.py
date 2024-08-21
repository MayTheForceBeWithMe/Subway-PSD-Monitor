from pymodbus.client import ModbusTcpClient
from pymodbus.register_write_message import WriteSingleRegisterRequest  
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


logging.getLogger('influxdb').setLevel(logging.CRITICAL)  
logging.getLogger('httpd').setLevel(logging.CRITICAL)   
# 或者，如果你不确定具体的日志器名称，可以尝试禁用所有非警告级别的日志  
logging.basicConfig(level=logging.CRITICAL)


class PSD_Monitoring(object):
    
    def __init__(self):
        self.setting_addr = "D:\PSDmonitor\profile\SettingMenu.json"
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
            "DBname-alert_time_consuming":     self.display_clients()['database']['DBname-alert_time_consuming']
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
        self.q = queue.Queue() 
        self.time_count_connect = []
        self.relay_buff = []
        self.relay_sort_ls = []
        self.alert_event = threading.Event()
        self.now_datetime = datetime.now()
        self.source_measurement = self.database_info['DBname-station_HISTORY']
        # Initial Config
        with open(file=self.setting_addr, encoding='utf-8') as cfg:
            # getting config
            dict_cfg = json.load(cfg)
        self.Tc = dict_cfg['InitialConfig']['Tc']
        self.base_time = dict_cfg['InitialConfig']['base_time']
        self.save_all = dict_cfg['InitialConfig']['save_all']


    def ff(self, result, time):
        result = hex(result)[2:]
        l = len(result)
        if l == 1:
           if   time == 'S':
                return int("0x" + str(result[0:1]), base=16)
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
        with open(file=self.setting_addr, encoding='utf-8') as cfg:
            # getting config
            dict_cfg = json.load(cfg)
            T_dict = {}
            for i in range(dict_cfg['TerminalNum']):
                T = "T" + str(i+1)
                Th = dict_cfg['TerminalClient'][T]['host']
                Tp = dict_cfg['TerminalClient'][T]['port']
                T_dict[T] = {'host': Th, 'port': Tp}  
            # 数据库
            DBhost = dict_cfg['DatabaseClient']['host']
            DBport = dict_cfg['DatabaseClient']['port']
            DBusername = dict_cfg['DatabaseClient']['username']
            DBpassword = dict_cfg['DatabaseClient']['password']
            # 站点记录
            DBname_station = dict_cfg['DatabaseClient']['DBname'][0]
            DBname_station_SAVE = dict_cfg['DatabaseClient']['DBname'][1]
            DBname_station_HISTORY = dict_cfg['DatabaseClient']['DBname'][2]
            # 过车记录
            DBname_train = dict_cfg['DatabaseClient']['DBname'][3]
            # 网络记录
            DBname_network = dict_cfg['DatabaseClient']['DBname'][4]
            # 报警记录
            DBname_alert_record = dict_cfg['DatabaseClient']['DBname'][5]
            DBname_alert_check = dict_cfg['DatabaseClient']['DBname'][6]
            DBname_alert_settled = dict_cfg['DatabaseClient']['DBname'][7]
            DBname_alert_time_consuming = dict_cfg['DatabaseClient']['DBname'][8]
            # 处理
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
                    'DBname-alert_time_consuming': DBname_alert_time_consuming
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
        try:
            # connect to database 
            self.DBclient = InfluxDBClient(HOST, PORT, USER, PASSWORD)
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
                
                self.DBclient.drop_database(self.database_info['DBname-alert_record'])
                self.DBclient.create_database(self.database_info['DBname-alert_record'])
                
                self.DBclient.drop_database(self.database_info['DBname-alert_check'])
                self.DBclient.create_database(self.database_info['DBname-alert_check'])
                
                self.DBclient.drop_database(self.database_info['DBname-alert_settled'])
                self.DBclient.create_database(self.database_info['DBname-alert_settled'])
                
                self.DBclient.drop_database(self.database_info['DBname-alert_time_consuming'])
                self.DBclient.create_database(self.database_info['DBname-alert_time_consuming'])
                
            print('database connected')    
        except:
            print('database connect fail')           
   
        
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
        
    def DataBase_alert_record_send(self, DBname, records, _station_, _line_, current_time):
        self.DBclient.switch_database(DBname)
        alert_record_data = \
        [
            {
                "measurement":           DBname,
                "time":                  current_time,
                "fields":           
                {

                    "报警内容":           records,
                    "报警响应时间":       datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                },
                "tags":
                {
                    "行车方向":          _line_,
                    "站点":              _station_
                }    
            }
        ]
        self.DBclient.write_points(alert_record_data)
        
    def DataBase_alert_check_send(self, DBname, current_time):
        self.DBclient.switch_database(DBname)
        alert_check_data = \
        [
            {
                "measurement":          DBname,
                "time":                 current_time,
                "fields":
                {
                    "发现时间":          datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
            }
        ]
        self.DBclient.write_points(alert_check_data)
        
    def DataBase_alert_settled_send(self, DBname, records, _station_, _line_, current_time):
        self.DBclient.switch_database(DBname)
        alert_settled_data = \
        [
            {
                "measurement":          DBname,
                "time":                 current_time,
                "fields":
                {
                    "恢复内容":              records,
                    "报警解除时间":          datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
                },
                "tags":
                {
                    "行车方向":          _line_,
                    "站点":              _station_
                }   
            }
        ]
        self.DBclient.write_points(alert_settled_data)
        
    def DataBase_alert_time_consuming_send(self, DBname, time_consuming, current_time):
        self.DBclient.switch_database(DBname)
        alert_time_consuming = \
        [
            {
                "measurement":                  DBname,
                "time":                         current_time,
                "fields":
                {
                    "总耗时":                    time_consuming
                }
            }
        ]
        self.DBclient.write_points(alert_time_consuming)


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
           
    ##################################################################################################################   
    
    def main(self, clients, cycle, continous, network_connect_flag, base_time):
        Device_IP, Device_Port = clients[1], clients[2]
        IP_num = int(Device_IP.split('.')[-1])

        # IP地址和端口号
        fix_source_address = ('0.0.0.0', 8888)
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
                    start_time = time.time()
                    alarm_time = datetime.now()
                    if base_time:
                        alarm_time_UTC = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
                    count_num += 1

                    ##################################################################################################
                
                    if TCP_connect_flag == False:   
                        print("device: {} disconnect".format(clients[0]))  
                        TCP_connect_flag = self.TCPclient.connect()
                        # time.sleep(1)
                        # if TCP_connect_flag == False:
                        #     continue
                        count_num = 0 
                        alarm_time_UTC  = datetime.utcnow()
                        self.time_count_connect.append(alarm_time_UTC)
                        connection_status = "网络连接中断"
                        DBname_network = self.database_info['DBname-network']
                        self.time_count_connect.append(alarm_time_UTC)
                        self.DataBase_network_send(DBname_network, clients[1], connection_status, self.time_count_connect[0])
                        T_cls = self.display_clients()['terminal']
                        self.main_thread(T_cls, 5, cycle, continous, base_time)             
                   
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
                        data_item = \
                        {
                            # 表头
                            'head':  "0x" + str(hex(read_result[0])[2:]) + str(hex(read_result[1])[2:]),
                            # 日期时间校正
                            'Y':      self.ff(read_result[2], 'Y'),
                            'M':      self.ff(read_result[2], 'M'),
                            'D':      self.ff(read_result[3], 'D'),
                            'H':      self.ff(read_result[3], 'H'),
                            'Q':      self.ff(read_result[4], 'Q'),
                            'S':      self.ff(read_result[4], 'S'),
                            'ms':     self.ff(read_result[5], 'ms'),
                            # 各通道数据读取
                            'CH1':   '{:016b}'.format(read_result[7]) + '{:016b}'.format(read_result[6]),
                            'CH2':   '{:016b}'.format(read_result[9]) + '{:016b}'.format(read_result[8]),
                            'CH3':   '{:016b}'.format(read_result[11]) + '{:016b}'.format(read_result[10]),
                            'CH4':   '{:016b}'.format(read_result[13]) + '{:016b}'.format(read_result[12])
                        }

                        if base_time == False:
                            alarm_time_UTC = datetime(2000 + data_item['Y'], data_item['M'], data_item['D'], data_item['H'] - 8, data_item['Q'], data_item['S'])
                            alarm_time_UTC = alarm_time_UTC.strftime("%Y:%m:%d %H:%M:%S" + ".{}".format(data_item['ms']))
              
         
                    ###################################################################################################
                    
                        door_set, base_set = {}, {}
                        door_SAVE_set, base_SAVE_set = {}, {}
                        door_HISTORY_set, base_HISTORY_set = {}, {}
                        new_door_HISTORY_set, new_HISTORY_set = {}, {}
    
                    
                        self.LINE = {"上行": 'S', "下行": 'X'}
                        with open(file=self.setting_addr, encoding='utf-8') as cfg:
                            dict_cfg = json.load(cfg) 
                        station_info = dict_cfg['TerminalClient']['T1']['station']
                        self.StationName = list(station_info.keys())
                        for station in self.StationName:
                            self.line = list(station_info[station].keys())
                            for line in self.line:
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
                                    collect = self.data_connect(data_item, RELAY, 'CH' + str(ch))
                                    collect = [int(item) for item in collect]
                                    for i in self.relay_values:
                                        matching_key = next(k for k, v in RELAY.items() if v == i)
                                        door_set[i] = collect[int(self.relay_values.index(i))]
                                        door_HISTORY_set[i] = collect[int(self.relay_values.index(i))]
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
                                    door_HISTORY_set["alert"] = alert[0]
                                    new_door_HISTORY_set["alert"] = alert[0]
                                    self.alert_event.set()
                                    self.DataBase_alert_record_send(self.database_info['DBname-alert_record'], "异常", station, line, alarm_time_UTC)
                                
                                    
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
                                            self.alarm_record = "机电电源异常"
                                            #self.DataBase_alert_record_send(self.database_info['DBname-alert_record'], state[0], station, line, alarm_time_UTC)
                                        
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
                                            #self.EM_S_alert_flag = True
                                        
                                        
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
                                            self.DataBase_alert_record_send(self.database_info['DBname-alert_record'], state[0], station, line, alarm_time_UTC)
                                
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
                                            self.DataBase_alert_settled_send(self.database_info['DBname-alert_settled'], alert[0], station, line, alarm_time_UTC)

                                        if len(sequence_ls) == len(collect_ls) and (all(sequence_ls == collect_ls for sequence_ls, collect_ls in zip(sequence_ls, collect_ls)) and (LABEL == 0)):
                                            door_set["Note"] = STATE
                                            self.door_nop.append(door_set['Note'])
                                            alert = ["异常", "Warning"]
                                            alert_mark = LABEL
                                            door_set["AlertMark"] = alert_mark
                                            door_SAVE_set["AlertMark"] = alert_mark
                                            door_SAVE_set["Note"] = 'warning'
                                            self.alert_event.set()  
                                            self.DataBase_alert_settled_send(self.database_info['DBname-alert_settled'], alert[0], station, line, alarm_time_UTC)
                                                                
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
                                            door_HISTORY_set["alert"] = alert[0]
                                            new_door_HISTORY_set["alert"] = alert[0]
                                            self.alert_event.set()
                                            self.DataBase_alert_settled_send(self.database_info['DBname-alert_settled'], alert[0], station, line, alarm_time_UTC)
    
                                            
                                elif logic_num == 0:
                                    state = ["正常", 'Normal']
                                    door_set["Note"] = state[0]
                                    door_SAVE_set["Note"] = state[1]
                                    alert = ["正常", 'Normal']
                                    alert_mark = 1
                                    door_set["AlertMark"] = alert_mark
                                    door_SAVE_set["AlertMark"] = alert_mark
                                    door_set["alert"] = alert[0] 
                                    door_HISTORY_set["alert"] = alert[0]
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
                                        self.DataBase_alert_settled_send(self.database_info['DBname-alert_settled'], "正常", station, line, alarm_time_UTC)

                                    
                    ##################################################################################################
                    
                    
                                # if door_set["Note"] == "正常":
                                #     if self.normal_flag:
                                #         # self.normal_flag = True
                                #         # self.warning_flag = True
                                #         # self.undefine = True
                                #         # self.CanNotJudge = True
                                #         # self.EM_alert_flag = True
                                #         # self.S_alert_flag = True
                                #         # self.EM_S_alert_flag = True
                                #         self.DataBase_alert_settled_send(self.database_info['DBname-alert_settled'], self.alarm_record, station, line, alarm_time_UTC)
                                    
                                # if door_set["Note"] == "异常":
                                #     if self.warning_flag:
                                #         # self.normal_flag = True
                                #         # self.warning_flag = False
                                #         # self.undefine = True
                                #         # self.CanNotJudge = True
                                #         # self.EM_alert_flag = True
                                #         # self.S_alert_flag = True
                                #         # self.EM_S_alert_flag = True
                                #         self.alarm_record = "异常"
                                #         self.DataBase_alert_record_send(self.database_info['DBname-alert_record'], "异常", station, line, alarm_time_UTC)
                                    
                                # if door_set["Note"] == "状态无定义":
                                #     if self.undefine:
                                #         # self.normal_flag = True
                                #         # self.warning_flag = True
                                #         # self.undefine = True
                                #         # self.CanNotJudge = True
                                #         # self.EM_alert_flag = True
                                #         # self.S_alert_flag = True
                                #         # self.EM_S_alert_flag = True
                                #         self.DataBase_alert_settled_send(self.database_info['DBname-alert_settled'], self.alarm_record, station, line, alarm_time_UTC)
                                    
                                # if door_set["Note"] == "无法判断":
                                #      if self.CanNotJudge:
                                #         # self.normal_flag = True
                                #         # self.warning_flag = True
                                #         # self.undefine = True
                                #         # self.CanNotJudge = False
                                #         # self.EM_alert_flag = True
                                #         # self.S_alert_flag = True
                                #         # self.EM_S_alert_flag = True
                                #         self.alarm_record = "无法判断"
                                #         self.DataBase_alert_record_send(self.database_info['DBname-alert_record'], "异常", station, line, alarm_time_UTC)
                                    
                                # if door_set["Note"] == "机电电源异常":
                                #    if self.EM_alert_flag:
                                #         # self.normal_flag = True
                                #         # self.warning_flag = True
                                #         # self.undefine = True
                                #         # self.CanNotJudge = True
                                #         # self.EM_alert_flag = False
                                #         # self.S_alert_flag = True
                                #         # self.EM_S_alert_flag = True
                                #         self.alarm_record = "机电电源异常"
                                #         self.DataBase_alert_record_send(self.database_info['DBname-alert_record'], "机电电源异常", station, line, alarm_time_UTC)
                                    
                                # if door_set["Note"] == "信号电源异常":
                                #     if self.S_alert_flag:
                                #         # self.normal_flag = True
                                #         # self.warning_flag = True
                                #         # self.undefine = True
                                #         # self.CanNotJudge = True
                                #         # self.EM_alert_flag = True
                                #         # self.S_alert_flag = False
                                #         # self.EM_S_alert_flag = True
                                #         self.alarm_record = "信号电源异常"
                                #         self.DataBase_alert_record_send(self.database_info['DBname-alert_record'], "信号电源异常", station, line, alarm_time_UTC)
                                
                                # if door_set["Note"] == "机电/信号电源异常":
                                #     if self.EM_S_alert_flag:
                                #         # self.normal_flag = True
                                #         # self.warning_flag = True
                                #         # self.undefine = True
                                #         # self.CanNotJudge = True
                                #         # self.EM_alert_flag = True
                                #         # self.S_alert_flag = True
                                #         # self.EM_S_alert_flag = False
                                #         self.alarm_record = "机电/信号电源异常"
                                #         self.DataBase_alert_record_send(self.database_info['DBname-alert_record'], "机电/信号电源异常", station, line, alarm_time_UTC)
                                        

                                # 整理成统一形式
                                door_set["StationName"] = station                                                       
                                door_set["deviceIP"] = Device_IP  
                                base_set["measurement"] = self.database_info['DBname-station']                
                                base_set["tags"] = {"line": line, "Channel": channel}            
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
                                base_HISTORY_set["fields"] = door_HISTORY_set
                                
                                # Getting effective data to database
                                monitor_data = [base_set]
                                save_data = [base_SAVE_set]
                                history_data = [base_HISTORY_set]
                                #print(monitor_data)

                                end_time = time.time()
                                collect_time = end_time - start_time
                                #print("采集运行时间:", collect_time) 

                                time.sleep(cycle)
                                if (collect_time <= 0.2):      
                                    # 送入“station”数据库
                                    self.DBclient.switch_database(self.database_info['DBname-station']) 
                                    self.DBclient.write_points(monitor_data)
                                    self.DBclient.switch_database(self.database_info['DBname-station_SAVE']) 
                                    self.DBclient.write_points(save_data)
                                    self.DBclient.switch_database(self.database_info['DBname-station_HISTORY'])
                                    self.DBclient.write_points(history_data) 

                                    # print('                          "采集运行时间:"    {}'.format(collect_time))
                            
                                # 计算过车信息
                                # notes_list = self.door_nop
                                # if len(notes_list) == 3:
                                #     notes_list.pop(0)
                                #     if all(x == notes_list[0] for x in notes_list) and continous is False:
                                #         pass
                                #     else: 
                                #         door_list = self.door_nop
                                #         if len(door_list) == 3:
                                #             door_list.pop(0)
                                #         if door_list[0] == '正常状态' and door_list[1] == '开门':
                                #             train_arriving_time = timeit.default_timer() 
                                #         if door_list[0] == '关门' and door_list[1] == '门已关':
                                #             # train dwell time or train pass frequency in the station ever 10 minutes
                                #             train_num += 1
                                #             train_departing_time = timeit.default_timer()
                                #             dwell_time = train_departing_time - train_arriving_time
                                #         print("第{}辆车，停靠用时{}秒".format(train_num, dwell_time))
                                #         if door_list[0] == '门已关' and door_list[1] == '正常状态':   
                                #             print("\n---------------------------------------屏蔽门正常动作--------------------------------------------") 
                                #         # 送入"train"数据库
                                #         DBname_train = self.database_info['DBname-train']
                                #         self.DataBase_train_send(DBname_train, Device_IP, channel, line, train_num, dwell_time, pass_frequency) 
        
                except:
                    "终端{}连接异常".format(clients[0])
                    print('reconnecting...')
                    if self.TCPclient is not None:
                        self.TCPclient.close()
                    time.sleep(30)  


                # 获取时间          
                # print("当前时间： \n", end_time)
                elapsed_time = end_time - start_time 
                now = datetime.now()  
                current_minute = now.minute
                terminal_time = now.strftime("%H:%M:%S")
                # terminal_date = now.strftime('%Y-%m-%d %H:%M:%S')
                local_time = time.localtime(time.time())  
                with open(file=self.setting_addr, encoding='utf-8') as cfg:
                    dict_cfg = json.load(cfg) 
                export_time = dict_cfg['InitialConfig']['import_time']        
                end_time = time.time()
                # separate_time = "00:00:00"

                # 获取昨天和前天和大后天的日期
                yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d')
                day_before_yesterday = (datetime.now() - timedelta(2)).strftime('%Y-%m-%d')
                Two_days_later = (self.now_datetime + timedelta(2)).strftime('%Y-%m-%d 01:00:00')

                # 时间戳转换
                start_time_info = datetime.fromtimestamp(time_info).strftime('%Y-%m-%d') 
                end_time_info = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d') 
                
                end_time_datetime = datetime.fromtimestamp(end_time)
                end_time_minus_one_day = end_time_datetime - timedelta(days=1) 
                end_time_info_minus_one_day = end_time_minus_one_day.strftime('%Y-%m-%d')

                if (current_minute == 0):
                    # 清空显示界面缓存
                    self.DBclient.drop_database(self.database_info['DBname-station']) 
                    self.DBclient.create_database(self.database_info['DBname-station'])

                if (terminal_time == export_time):              
                    # 备份历史记录数据库,分离出一个新表
                    self.source_measurement = self.database_info['DBname-station_HISTORY']
                    new_measurement = self.source_measurement + "_" + now.strftime('%Y%m%d') 
                    new_HISTORY_set["measurement"] = new_measurement
                    new_HISTORY_set["tags"] = {"line": line, "Channel": channel, "StationName": station, 'alert': alert[0]}
                    new_HISTORY_set["time"] = alarm_time_UTC
                    del door_HISTORY_set['alert'] 
                    new_HISTORY_set["fields"] = door_HISTORY_set
                    new_history_data = [new_HISTORY_set]
                    self.DBclient.switch_database(self.source_measurement)
                    self.DBclient.write_points(new_history_data) 
                    query_separate = f"SELECT * FROM {self.source_measurement} WHERE time > now() - '{now.strftime('%Y-%m-%d')}T16:00:00Z'"
                    print(query_separate)
                    result = self.DBclient.query(query_separate)  
                    points = list(result.get_points())  
                    # 获取新point
                    i = 0
                    new_points = []  
                    for point in points: 
                        fields = {}
                        for relay in self.relay_values:
                            fields[relay] = point[relay]
                        # 假设 point 是一个字典，包含了 time, tags, 和 fields  
                        new_point = {  
                            "measurement": new_measurement,  
                            "tags": {"line": point['line'], "Channel": point['Channel'], "StationName": point['StationName'], "alert": point['alert']},
                            "fields": fields,
                            "time": point['time']
                        }
                        new_points.append(new_point)  
                        i += 1
                        if(i%10000 == 0):
                            # 批量写入新measurement  
                            self.DBclient.write_points(new_points)
                            new_points = []  
                    n += 1
                    if n%2 == 0:
                        # 每两天清除当天表历史记录中数据
                        query_delete = f"DELETE FROM {self.source_measurement}"
                        self.DBclient.query(query_delete) 
                        n = 0

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
                
              
    def main_thread(self, T_cls, T, cycle, continous, base_time):  
        connect_flag = True
        sk = None
        time.sleep(T)   
        if connect_flag:
            for cls in T_cls.keys():
                clients = [cls, T_cls[cls]['host'], T_cls[cls]['port']]
                # starting thread to collect data
                network_connect_flag = True
                terminal_thread = threading.Thread(target=self.main, kwargs={'clients': clients, 'cycle': cycle, 'continous': continous, 'network_connect_flag': network_connect_flag, 'base_time': base_time})
                terminal_thread.start()
                
   
    def run(self, cycle, continous, base_time, save_all):    
        # conncting to the Influxdb 
        self.DataBase_connect(self.database_info['host'], self.database_info['port'], self.database_info['username'], self.database_info['password'], save_all)
        # ModbusTCP connecting and create threads in listenning network loop one by one
        T_cls = self.display_clients()['terminal']
        self.main_thread(T_cls, 3, cycle, continous, base_time)
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
