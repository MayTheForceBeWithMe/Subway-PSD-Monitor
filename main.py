from pymodbus.client import ModbusTcpClient
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb import InfluxDBClient
from collections import OrderedDict 
from pydub import AudioSegment  
from pydub.playback import play
from datetime import datetime
from telnetlib import Telnet
import threading
import queue  
import socket
import psutil  
import webbrowser
import subprocess
import time
import timeit
import json
import os



class PSD_Monitoring(object):
    
    def __init__(self):
        self.setting_addr = "D:/HMI_demo/2023project/subway/setting.json"
        self.data_sender = [] 
        self.database_info = \
        {
            "host":                 self.display_clients()['database']['host'],
            "port":                 self.display_clients()['database']['port'],
            "username":             self.display_clients()['database']['username'],
            "password":             self.display_clients()['database']['password'],
            'DBname-station':       self.display_clients()['database']['DBname-station'],
            'DBname-train':         self.display_clients()['database']['DBname-train'],
            "DBname-network":       self.display_clients()['database']['DBname-network'],
            "DBname-transimssion":  self.display_clients()['database']['DBname-transimssion']
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
        self.q = queue.Queue() 
        self.time_count_connect = []
        self.relay_buff = []
        self.relay_sort_ls = []
    
    
    
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
            
    
    
    def data_connect(self, group, data):
        # 输入端序号和序列位一一对应
        group_key = group['继电器序列'].keys()
        group_key = list(group_key) 
        for i in group_key:
            try: 
                group['继电器序列'][i] = data[int(group['继电器序列'][i])]
            except:
                continue
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
            DBhost = dict_cfg['DatabaseClient']['host']
            DBport = dict_cfg['DatabaseClient']['port']
            DBusername = dict_cfg['DatabaseClient']['username']
            DBpassword = dict_cfg['DatabaseClient']['password']
            DBname_station = dict_cfg['DatabaseClient']['DBname'][0]
            DBname_train = dict_cfg['DatabaseClient']['DBname'][1]
            DBname_network = dict_cfg['DatabaseClient']['DBname'][2]
            DBname_transimssion = dict_cfg['DatabaseClient']['DBname'][3]
            dict_info = {
                'terminal': 
                    T_dict,
                'database':{
                    'host': DBhost,
                    'port': DBport,
                    'username': DBusername,
                    'password': DBpassword,
                    'DBname-station': DBname_station,
                    'DBname-train': DBname_train,
                    'DBname-network': DBname_network,
                    'DBname-transimssion': DBname_transimssion
                }
            }
        return dict_info
    
            
            
    def GetTerminalAndChannel(self, get_terminal, get_channel):
        with open(file=self.setting_addr, encoding='utf-8') as cfg:
            # getting config
            dict_cfg = json.load(cfg)
            T_total = dict_cfg['TerminalNum']
            T_num = int(get_terminal[1])
            T_str = get_channel[0]
            CH_num = int(get_channel[2])
            CH_str = get_channel[0:1]
            CH_result = {}
            J = []
            if (T_num <= T_total and T_num > 0) and (CH_num < 5 and CH_num > 0) or (T_str =='T' and CH_str == 'CH'):
                CH = dict_cfg['TerminalClient'][get_terminal]['channels'][get_channel]
                IP = dict_cfg['TerminalClient'][get_terminal]['host']
                CH = sorted(CH.items(),key=lambda x:x[0])
                if len(CH) == 0:
                    pass
                # 获取键值
                CH = {k: v for k, v in CH} 
                #print(CH)
                #变成列表形式
                for key in CH.keys():
                    try:
                        CH_k = CH[key]
                        if len(CH_k[1]) == 0:
                            continue
                        # 上下行
                        CH_k = [CH_k]
                        for item in CH_k:   
                            key_ = item[0]  
                            value_ = item[1]  
                            if key_ == 'up':          
                                if value_[2] == 'TWJ':   
                                    item[1][2] = 'TWJ'  
                                value_.insert(2, 'up')
                            elif key_ == 'down':        
                                if value_[2] == 'TWJ':     
                                    item[1][2] = 'TWJ'   
                                value_.insert(2, 'down')
                        CH_k = CH_k[0]
                        CH_k[1].append(key)
                        J.append(CH_k[1])    
                    except:
                        pass          
                # 变为字典形式
                for item in J:   
                    key = item[0]  
                    station = item[1]
                    line = item[2]
                    value = item[3]   
                    port = item[4] 
                    J_value = [station, line, value, port]
                    if key in CH_result:  
                        CH_result[key].append(J_value)  
                    else:  
                        CH_result[key] = [J_value]  
                
                CH = {'IP': IP, get_channel: CH_result}
                return CH
            else:
                return None
        
    
    
    def RelayGroup(self, Tx, CHx, Num):
        # 准备挂载
        try:
            ###### Channel #####
            CH = self.GetTerminalAndChannel(Tx, CHx)
            CH = CH[CHx]
            ###### Number ######
            CH_ls = CH[Num]
            StationName = CH_ls[0][0]
            LineDirection = CH_ls[0][1]
            ###### RelayGroup and Node ######
            RelayGroup = {k[2] : k[3] for k in CH_ls}  
            R = RelayGroup.keys()
            RelayGroup = {k: RelayGroup[k] for k in R}
            RelayGroup = {'站点': StationName, '行车方向': LineDirection, '所用通道': CHx, '组号': Num, '继电器序列': RelayGroup}
            return RelayGroup
        except:
            RelayGroup = {'站点': None, '行车方向': None, '所用通道': None, '组号': None, '继电器序列': None}
            return RelayGroup
                
     
                              
    def Alarm_Audio(self):  
        with open(file=self.setting_addr, encoding='utf-8') as cfg:
            dict_cfg = json.load(cfg)
            audio_file = dict_cfg["AudioFile"]
            # Playing alarm audio
            audio = AudioSegment.from_file(audio_file, format="mp3")  
            play(audio)
        pass
           
           
           
    def upload(self, change, continous):
        if continous == False:
            self.notes_nop.append(change)
            notes_list = self.notes_nop
            if len(notes_list) == 3:
                notes_list.pop(0)
                if all(x == notes_list[0] for x in notes_list):
                    return False
                else:  
                    return True 
        else:
            return True
            

                    
    def DataBase_connect(self, HOST, PORT, USER, PASSWORD):
        try:
            # connect to database 
            self.DBclient = InfluxDBClient(HOST, PORT, USER, PASSWORD)
            # clearing database and then creating a new database again
            self.DBclient.drop_database(self.database_info['DBname-station'])   
            self.DBclient.create_database(self.database_info['DBname-station'])
            self.DBclient.drop_database(self.database_info['DBname-train'])   
            self.DBclient.create_database(self.database_info['DBname-train'])
            self.DBclient.drop_database(self.database_info['DBname-network'])   
            self.DBclient.create_database(self.database_info['DBname-network'])
            self.DBclient.drop_database(self.database_info['DBname-transimssion'])   
            self.DBclient.create_database(self.database_info['DBname-transimssion'])
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
        
    def DataBase_transimaission_send(self, DBname, deviceIP, MegaByte, current_time):
        self.DBclient.switch_database(DBname)
        transimaission_data = \
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
                    "MegaByte":            MegaByte
                }
            }
        ]
        self.DBclient.write_points(transimaission_data)
    
     
    
    def main(self, clients, cycle, continous, print_info, network_connect_flag, base_time):
        TCPclient = ModbusTcpClient(clients[1], clients[2])
        change_flag = False
        train_num = 0
        dwell_time = 0
        pass_frequency = 0
        count_num = 0
        beginning_time = timeit.default_timer() 
        
        while True:  
            try:
                alarm_time = datetime.now()
                if base_time:
                    alarm_time_UTC = datetime.utcnow()
                count_num += 1

                ##################################################################################################
                
                DBname_transimssion = self.database_info['DBname-transimssion']
                io = psutil.net_io_counters() 
                bytes_received = io.bytes_recv
            
                ##################################################################################################
                
                if not TCPclient.connect():   
                    print("设备：{} 无法连接".format(clients[0]))  
                    TCPclient.close() 
                    count_num = 0 
                    MegaByte = 0  
                    alarm_time_UTC  = datetime.utcnow()
                    self.time_count_connect.append(alarm_time_UTC)
                    connection_status = "网络连接中断"
                    DBname_network = self.database_info['DBname-network']
                    self.time_count_connect.append(alarm_time_UTC)
                    self.DataBase_network_send(DBname_network, clients[1], connection_status, self.time_count_connect[0])
                    T_cls = self.display_clients()['terminal']
                    self.listen_network(T_cls, 5, cycle, continous, base_time)             
                else:
                    MegaByte = bytes_received / (1024 * 1024)
                alarm_time_UTC  = datetime.utcnow()
                self.DataBase_transimaission_send(DBname_transimssion, clients[1], MegaByte, alarm_time_UTC)
                if count_num == 1:
                    network_connect_flag = True
                    
                ###################################################################################################
                
                # reading Modbus
                read_result = TCPclient.read_holding_registers(0, 100)  

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
                        
                    # data connect
                    for i in range(1, 4):
                        for j in range(1, 20):
                            CH = 'CH' + str(i)                                   # 采集通道
                            for k in range(20):                                  # 组号
                                RelayItem = self.RelayGroup(clients[0], CH, j)   # 获取目标端口
                                Data = data_item[CH][::-1]                       # 将读取的32位二进制数据序列翻转
                                with open(file=self.setting_addr, encoding='utf-8') as cfg:
                                    dict_cfg = json.load(cfg) 
                                port_info = dict_cfg["TerminalClient"]['T1']['channels'][CH]
                                port_num = list(port_info.keys())
                                # 若获取到的序列全为None则全部忽略
                                if RelayItem['站点'] is None and RelayItem['行车方向'] is None and RelayItem['所用通道'] is None and RelayItem['组号'] is None and RelayItem['继电器序列'] is None:
                                    continue
                                # 将获取到的端口对应到数据位
                                if k != RelayItem['组号']:
                                    pass
                                self.data_connect(RelayItem, Data) 
                                R = RelayItem['继电器序列']
                                R_keys = list(R.keys())
                                Relay_key = list(R.keys()) 
                                Relay_value = list(R.values())
                                Relay_value = [int(val) for val in Relay_value] 
                                device_IP = clients[1]
                                Channel = CH
                                StationName = RelayItem['站点']
                                GroupNum = RelayItem['组号']
                                DBname_station = self.database_info['DBname-station']
                                DBname_train = self.database_info['DBname-train']
                                Line = RelayItem['行车方向']
                                door_set, base_set = {}, {}
                                for key in Relay_key:    
                                    if key in Relay_key: 
                                        door_set[key] = int(R[key])
                                # data alarm processing  
                                action_logic = dict_cfg['ActionLogic']
                                action_logic_num = dict_cfg['ActionLogic'][StationName]
                                self.collect_nop.append(Relay_value)
                                collect_list = self.collect_nop
                                if len(collect_list) == 2:
                                    # getting data from previous status and current status
                                    self.previous_data =  self.collect_nop[0]
                                    self.current_data = self.collect_nop[1]
                                    self.previous_data = list(map(int, self.collect_nop[0]))
                                    self.current_data = list(map(int, self.collect_nop[1]))
                                    collect_list.pop(0)
                                    collect = collect_list[0]

                                    # 进入逻辑判断
                                    for i in range(len(action_logic_num)):
                                        relay, sequence = action_logic[StationName][str(i)]["relay"], action_logic[StationName][str(i)]["sequence"]
                                        state, label = action_logic[StationName][str(i)]['state'], action_logic[StationName][str(i)]['label'] 
                                        sequence, relay = OrderedDict(sorted(sequence.items())), OrderedDict(sorted(relay.items()))
                                        sequence, relay = list(sequence.values()), list(relay.values())
                                        label = int(label)
                                         # 判断继电器序列中有无机电电源端口和信号电源端口   
                                        power_label = ["LZLF", "KZKF"]
                                        EM_power, S_power = int(collect[relay.index(power_label[0])]), int(collect[relay.index(power_label[1])])
                                        if ((power_label[0] in R_keys) or (power_label[1] in R_keys)) and ((EM_power != 1) or (S_power != 1)):
                                            alert = "abnormal"
                                            alert_mark = 0
                                            door_set["AlertMark"] = alert_mark
                                            # 判断机电电源和信号电源是否异常
                                            if (EM_power != 1) and (S_power == 1):
                                                state = "EM-power-abnormal"
                                                door_set["Note"] = state
                                            if (EM_power == 1) and (S_power != 1):
                                                state = "S-power-abnormal"
                                                door_set["Note"] = state              
                                            if (EM_power != 1) and (S_power != 1):
                                                state = "EM/S-power-abnormal"
                                                door_set["Note"] = state
                                        # 判断动作状态
                                        if len(sequence) == len(collect) and (all(sequence == collect for sequence, collect in zip(sequence, collect)) and (label == 1)):
                                            door_set["Note"] = state
                                            alert = "normal"
                                            alert_mark = label 
                                            door_set["AlertMark"] = alert_mark
                                        if len(sequence) == len(collect) and (all(sequence == collect for sequence, collect in zip(sequence, collect)) and (label == 0)):
                                            door_set["Note"] = state
                                            alert = "abnormal"
                                            alert_mark = label
                                            door_set["AlertMark"] = alert_mark
                
                ##################################################################################################
                                        # 整理成统一形式
                                        door_set["StationName"] = StationName
                                        door_set["Channel"] = Channel
                                        door_set["deviceIP"] = device_IP
                                        base_set["measurement"] = DBname_station
                                        base_set["tags"] = {"alert": alert, "line": Line}
                                        base_set["time"] = alarm_time_UTC
                                        base_set["fields"] = door_set
                                        # Getting effective data to database
                                        monitor_data = [base_set]
                                        # 送入“station”数据库
                                        self.DBclient.switch_database(DBname_station) 
                                        self.DBclient.write_points(monitor_data)
                                       # print(monitor_data)
                                        if print_info:
                                            print("\n站点：{} - {}\t报警时间：{}\t状态：{}\t采集卡终端IP：{}\t编号：{}-{}\t上报数据：{}".format(StationName, Line, alarm_time, alert, device_IP, Channel, GroupNum ,door_set))
                                
            except:
                #pass
                #"终端{}连接异常".format(clients[0])
                print('正在连接网络')
                if TCPclient is not None:
                    TCPclient.close()
                
                ##################################################################################################                
                
              
    def listen_network(self, T_cls, T, cycle, continous, print_info, base_time):  
        connect_flag = True
        sk = None
        while True:
            time.sleep(T)
            try:       
                if connect_flag:
                    for cls in T_cls.keys():
                        clients = [cls, T_cls[cls]['host'], T_cls[cls]['port']]
                        # starting thread to collect data
                        network_connect_flag = True
                        terminal_thread = threading.Thread(target=self.main, kwargs={'clients': clients, 'cycle': cycle, 'continous': continous, 'print_info': print_info, 'network_connect_flag': network_connect_flag, 'base_time': base_time})
                        terminal_thread.start()
                        time.time(1)
                else:
                    # created socket object 
                    sk = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  
                    sk.connect((T_cls[cls]['host'], T_cls[cls]['port'])) 
                    print('网络连接成功')  
                    
            except socket.error as e:  
                print('网络不通，错误信息：', e)  
            finally:  
                # 如果不为空则close
                if sk is not None:
                    sk.close()
                

    def run(self, cycle, continous, print_info, base_time):    
        # conncting to the Influxdb 
        self.DataBase_connect(self.database_info['host'], self.database_info['port'], self.database_info['username'], self.database_info['password'])
        # ModbusTCP connecting and create threads in listenning network loop one by one
        print("starting thread")
        T_cls = self.display_clients()['terminal']
        self.listen_network(T_cls, 5, cycle, continous, print_info, base_time)

            
                
if __name__ == "__main__":
    # starting up Influxdb 
    os.chdir('D:\\InfluxDB\\influxdb-1.7.7-1')
    os.popen('influxd.exe -config influxdb.conf') 
    os.popen('influx.exe') 
    time.sleep(2)
    # starting up Grafana
    os.chdir('D:\\grafana-enterprise-10.2.0.windows-amd64\\grafana-10.2.0\\bin')
    os.popen('grafana-server.exe') 
    time.sleep(2)
    # open to the Web
    url = 'http://127.0.0.1/srp-frame-example-pbm/pbm/index.html'
    chrome_path = 'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe' 
    subprocess.Popen([chrome_path, url])
    # starting up terminal
    terminal = PSD_Monitoring()
    terminal.run(cycle=200, continous=False, print_info=False, base_time=True)
