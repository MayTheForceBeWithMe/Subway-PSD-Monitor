from pymodbus.client import ModbusTcpClient
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb import InfluxDBClient
from pydub import AudioSegment  
from pydub.playback import play
from datetime import datetime  
import threading
import time
import timeit
import json



class PSD_Monitoring(object):
    
    def __init__(self):
        self.data_sender = [] 
        self.database_info = \
        {
            "host":       self.display_clients()['database']['host'],
            "port":       self.display_clients()['database']['port'],
            "username":   self.display_clients()['database']['username'],
            "password":   self.display_clients()['database']['password'],
            'DBname':     self.display_clients()['database']['DBname']
        }
        self.station_client = {}
        self.server_client = {}
        self.collect_err = \
        {
            "TWJ": None, 
            "KMJ": None, 
            "GMJ": None, 
            "MGJ": None, 
            "QCJ": None, 
            "LZLF": None, 
            "KZKF": None, 
            "Note": None, 
            "Channel": None, 
            "Site": None
        }
        self.collect_nop = []
        self.previous_data = []
        self.current_data = []
        self.notes_nop = []
        self.door_nop = []
        self.collect_notes = None
        self.current_note = None
    
    
    def ff(self, result, time):
        result = hex(result)[2:]
        l = len(result)
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
        else:
            if time == 'ms':
                return int(str(result), base=16)
            
    
    def data_connect(self, RG, data):
        RG_key = RG['继电器序列'].keys()
        RG_key = list(RG_key) 
        for i in RG_key:
            try: 
                RG['继电器序列'][i] =  data[int(RG['继电器序列'][i])]
            except:
                continue
        return RG
    
    
    def display_clients(self):
        with open(file="subway/setting.json", encoding='utf-8') as cfg:
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
            DBname = dict_cfg['DatabaseClient']['DBname'][0]
            dict_info = {
                'terminal': 
                    T_dict,
                'database':{
                    'host': DBhost,
                    'port': DBport,
                    'username': DBusername,
                    'password': DBpassword,
                    'DBname': DBname
                }
            }
        return dict_info
    
            
    def GetTerminalAndChannel(self, get_terminal, get_channel):
        with open(file="subway/setting.json", encoding='utf-8') as cfg:
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
                        #print(CH_k)
                        # 上下行
                        CH_k = [CH_k]
                        for item in CH_k:   
                            key_ = item[0]  
                            value_ = item[1]  
                            if key_ == '上行':          
                                if value_[2] == 'TWJ':   
                                    item[1][2] = 'TWJ'  
                                value_.insert(2, '上行')
                            elif key_ == '下行':        
                                if value_[2] == 'TWJ':     
                                    item[1][2] = 'TWJ'   
                                value_.insert(2, '下行')
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
        # 查看设备挂载信息
        RelayGroup0 = {'TWJ': None, 'KMJ': None, 'GMJ': None, 'MGJ': None, 'QCJ': None, 'LZLF': None, 'KZKF': None}
        try:
            ###### Channel #####
            CH = self.GetTerminalAndChannel(Tx, CHx)
            CH = CH[CHx]
            ###### Number ######
            CH_ls = CH[Num]
            StationName = CH_ls[0][0]
            LineDirection = CH_ls[0][1]
            ###### RelayGroup and Node ######
            RelayGroup  = {k[2] : k[3] for k in CH_ls}  
            R = RelayGroup.keys()
            sorted_R = sorted(R, key=lambda x: ['TWJ', 'KMJ', 'GMJ', 'MGJ', 'QCJ', 'LZLF', 'KZKF'].index(x)) 
            RelayGroup = {k: RelayGroup[k] for k in sorted_R}
            if len(RelayGroup) < 7 and len(RelayGroup) > 0:
                RelayGroup0.update(RelayGroup)
                RelayGroup = RelayGroup0
            elif len(RelayGroup) == 7:
                pass
            else:
                RelayGroup = None
            RelayGroup = {'站点': StationName, '行车方向': LineDirection, '所用通道': CHx, '编号': Num, '继电器序列': RelayGroup}
            return RelayGroup
        except:
            RelayGroup = {'站点': None, '行车方向': None, '所用通道': None, '编号': None, '继电器序列': None}
            return RelayGroup
                
                              
    def Alarm_Audio(self):  
        with open("subway/setting.json", encoding='utf-8') as cfg:
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
            
                
    def alarm_notes(self, collect):
        # data alarm processing
        self.collect_nop.append([collect[0], collect[1], collect[2], collect[3], collect[4], collect[5], collect[6]])
        collect_list = self.collect_nop
        if len(collect_list) == 3:
            # getting data from previous status and current status
            self.previous_data =  self.collect_nop[0]
            self.current_data = self.collect_nop[1]
            collect_list.pop(0)
            # the train has not arrived
            if self.previous_data ==  [0, 0, 1, 1, 0, 1, 1] and self.current_data == [0, 0, 1, 1, 0, 1, 1]:
                return '无列车停靠'
            # response signal from the action of the PSD when the train arriving 
            elif (self.previous_data == [0, 0, 1, 1, 0, 1, 1] and self.current_data == [1, 0, 1, 0, 0, 1, 1]) or \
               (self.previous_data == [1, 0, 1, 1, 0, 1, 1] and self.current_data == [1, 0, 1, 1, 0, 1, 1]):
                return '列车停稳'
            elif (self.previous_data == [1, 0, 1, 0, 0, 1, 1] and self.current_data == [1, 1, 0, 0, 0, 1, 1]) or \
               (self.previous_data == [1, 1, 0, 0, 0, 1, 1] and self.current_data == [1, 1, 0, 0, 0, 1, 1]):
                return '开门'
            elif (self.previous_data == [1, 1, 0, 0, 0, 1, 1] and self.current_data == [1, 0, 1, 0, 0, 1, 1]) or \
               (self.previous_data == [1, 0, 1, 0, 0, 1, 1] and self.current_data == [1, 0, 1, 0, 0, 1, 1]):
                return '关门'
            elif (self.previous_data == [1, 0, 1, 0, 0, 1, 1] and self.current_data == [1, 0, 1, 1, 0, 1, 1]) or \
               (self.previous_data == [1, 0, 1, 1, 0, 1, 1] and self.current_data == [1, 0, 1, 1, 0, 1, 1]):
                return '门关好'
            elif (self.previous_data == [1, 0, 1, 1, 0, 1, 1] and self.current_data == [0, 0, 1, 1, 0, 1, 1]) or \
               (self.previous_data == [0, 0, 1, 1, 0, 1, 1] and self.current_data == [0, 0, 1, 1, 0, 1, 1]):
                return '列车发车'
            #  abnormal alert
            elif (self.previous_data[:4] == [0, 0, 0, 0] and self.current_data[:4] == [0, 0, 0, 0]) or \
               (self.previous_data[:4] == [1, 1, 1, 1] and self.current_data[:4] == [1, 1, 1, 1]):
                return '输入异常'
            elif (self.previous_data == [0, 0, 1, 1, 0, 1, 1] and self.current_data == [1, 1, 0, 1, 0, 1, 1]) or \
               (self.previous_data == [1, 1, 0, 1, 0, 1, 1] and self.current_data == [1, 1, 0, 1, 0, 1, 1]):
                self.Alarm_Audio()
                return '门关好异常落下'
            elif (self.previous_data[5] == 1 and self.current_data[5] == 0) or \
               (self.previous_data[5] == 0 and self.current_data[5] == 0):
                self.Alarm_Audio()
                return '机电电源异常'
            elif  (self.previous_data[6] == 1 and self.current_data[6] == 0) or \
               (self.previous_data[6] == 0 and self.current_data[6] == 0):
                self.Alarm_Audio()
                return '信号电源异常'
            else:
                return "数据异常"
        else:
            return None
        
                    
    def DataBase_connect(self, HOST, PORT, USER, PASSWORD):
        try:
            # connect to database 
            self.DBclient = InfluxDBClient(HOST, PORT, USER, PASSWORD)
            # clearing database and then creating a new database again
            self.DBclient.drop_database(self.database_info['DBname'])   
            self.DBclient.create_database(self.database_info['DBname'])
            print('database connected')    
        except:
            print('database connect fail')           
   
            
                    
    def DataBase_door_send(self,DBname, deviveIP, channel, line, door, alert, current_time):
        # send data to data base
        self.DBclient.switch_database(DBname) 
        monitor_data = \
        [
            {
                "measurement": DBname,
                "tags": 
                {
                    "DeviceIP": deviveIP,
                    "Line": line,
                    "alert": alert
                },
                "time": current_time,
                "fields": 
                {
                    "TWJ":      door['TWJ'],
                    "KMJ":      door['KMJ'],
                    "GMJ":      door['GMJ'],
                    "MGJ":      door['MGJ'],
                    "QCJ":      door['QCJ'],
                    "LZLF":     door['LZLF'],
                    "KZKF":     door['KZKF'],
                    "Note":     door['Note'],
                    "Channel":  door['Channel'],
                    "Station":  door['Station'],
                }
            }
        ]
        self.DBclient.write_points(monitor_data)
        
    
    
    def main(self, clients, cycle, continous):
        TCPclient = ModbusTcpClient(clients[1], clients[2])
        change_flag = False
        train_num = 0 - 1
        dwell_time = 0
        pass_frequency = 0
        beginning_time = timeit.default_timer() 
        while True:
            try:     
                if not TCPclient.connect():   
                    print("设备：{} 无法连接".format(clients[0]))   
                
                # reading Modbus
                read_result = TCPclient.read_holding_registers(0, 100)  

                # running time counter
                count_time = timeit.default_timer() 
                total_time = count_time - beginning_time
                total_time = int(total_time)
    
                # every data are not erro or void, then display
                if not read_result.isError():   
                    # geting data that starts with modbus bit 1 from the registers
                    read_result = read_result.registers
                    # 读到的所有寄存器数据
                    data_item = \
                    {
                        'head':  "0x" + str(hex(read_result[0])[2:]) + str(hex(read_result[1])[2:]),
                        # 日期时间
                        'Y':     self.ff(read_result[2], 'Y'),
                        'M':     self.ff(read_result[2], 'M'),
                        'D':     self.ff(read_result[3], 'D'),
                        'H':     self.ff(read_result[3], 'H'),
                        'Q':     self.ff(read_result[4], 'Q'),
                        'S':     self.ff(read_result[4], 'S'),
                        'ms':    self.ff(read_result[5], 'ms'),
                        # 各通道数据
                        'CH1':   '{:016b}'.format(read_result[7]) + '{:016b}'.format(read_result[6]),
                        'CH2':   '{:016b}'.format(read_result[9]) + '{:016b}'.format(read_result[8]),
                        'CH3':   '{:016b}'.format(read_result[11]) + '{:016b}'.format(read_result[10]),
                        'CH4':   '{:016b}'.format(read_result[13]) + '{:016b}'.format(read_result[12])
                    }
                    
                    # data connect
                    for i in range(1, 4):
                        for j in range(1, 20):
                                CH = 'CH' + str(i)
                                RelayItem = self.RelayGroup(clients[0], CH, j)
                                Data = data_item[CH][::-1]
                                if RelayItem['站点'] is None and RelayItem['行车方向'] is None and RelayItem['所用通道'] is None and RelayItem['编号'] is None and RelayItem['继电器序列'] is None:
                                    continue
                                RelayItem = self.data_connect(RelayItem, Data)         
                                Relay = RelayItem['继电器序列']
                                Relay_list = [Relay["TWJ"], Relay["KMJ"], Relay["GMJ"], Relay["MGJ"], Relay["QCJ"], Relay["LZLF"], Relay["KZKF"]]
                                # gertting notes and adding it to data set
                                Note = self.alarm_notes(Relay_list)
                                door_set = \
                                {
                                    "TWJ":      Relay["TWJ"], 
                                    "KMJ":      Relay["KMJ"], 
                                    "GMJ":      Relay["GMJ"],
                                    "MGJ":      Relay["MGJ"],
                                    "QCJ":      Relay["QCJ"],
                                    "LZLF":     Relay["LZLF"], 
                                    "KZKF":     Relay["KZKF"],
                                    "Note":     Note,
                                    "Channel":  RelayItem['所用通道'],
                                    "Station":  RelayItem['站点'],
                                }
                
                                # getting effective data
                                device_IP = clients[1]
                                Channel = CH
                                StationName = RelayItem['站点']
                                GroupNum = RelayItem['编号']
                                DBname = self.database_info['DBname']
                                Line = RelayItem['行车方向']
                                alarm_time = datetime.now()
                                alarm_time_UTC = datetime.utcnow()
                                alert = None
                                if Note is not None and Note != "数据异常": 
                                    try:
                                        # alarm working only for data changes and then writing to database
                                        if change_flag:
                                            if self.upload(door_set['Notes'], continous):
                                                print("\n站点：{} - {}\t报警时间：{}\t采集卡终端IP：{}\t编号：{}-{}\t上报数据：{}".format(StationName, Line, alarm_time, device_IP, Channel, GroupNum ,door_set))
                                                self.door_nop.append(door_set['Notes'])
                                                door_list = self.door_nop
                                                if len(door_list) == 3:
                                                    door_list.pop(0)
                                                if door_list[0] == '无列车停靠' and door_list[1] == '列车停稳':
                                                    train_arriving_time = timeit.default_timer() 
                                                if door_list[0] == '列车停稳' and door_list[1] == '开门':
                                                    # train dwell time or train pass frequency in the station ever 10 minutes
                                                    train_num += 1
                                                    train_departing_time = timeit.default_timer()
                                                    dwell_time = train_departing_time - train_arriving_time
                                                if door_list[0] == '门关好' and door_list[1] == '列车发车':
                                                    print("\n----------------------------------------下一辆列车-----------------------------------------------")
                                                    print("\n---------------------------------------屏蔽门正常动作--------------------------------------------")
                                                if door_list[1] == '机电电源异常' or door_list[1] == '信号电源异常' or door_list[1] == '门关好异常落下':
                                                    alert = '异常'  
                                                else:
                                                    alert = '无异常' 
                                                self.DataBase_door_send(DBname, device_IP, Channel, Line, door_set, alert, alarm_time_UTC)
                                        else:
                                            # getting first data for the first time
                                            self.DataBase_door_send(DBname, device_IP, Channel, Line, door_set, alert, alarm_time_UTC)
                                            change_flag = True
                                    except:          
                                        pass
                                elif Note == "数据异常":
                                    print("\n站点：{} - {}\t报警时间：{}\t采集卡终端IP：{}\t编号：{}-{}\t上报数据：{}".format(StationName, Line, alarm_time, device_IP, Channel, GroupNum ,door_set))
                                    self.DataBase_door_send(DBname, device_IP, Channel, Line, door_set, alert, alarm_time_UTC)
                                else:
                                    pass
                                    
                    time.sleep(cycle/1000)
                else:
                    print("终端{}数据接收错误".format(clients[0]))      
            except:
                note = "终端{}连接异常".format(clients[0])
                self.collect_err['Note'] = Note
                self.Alarm_Audio()
                TCPclient.close() 
        TCPclient.close()        
        print("连接已中断")
        

    def run(self, cycle, continous):
        # conncting to the Influxdb 
        self.DataBase_connect(self.database_info['host'], self.database_info['port'], self.database_info['username'], self.database_info['password'])
        # ModbusTCP connecting and create threads one by one
        print("starting thread")
        T_cls = self.display_clients()['terminal']
        for cls in T_cls.keys():
            clients = [cls, T_cls[cls]['host'], T_cls[cls]['port']]
            # starting thread to collect data
            threading.Thread(target=self.main, kwargs={'clients': clients, 'cycle': cycle, 'continous': continous}).start()


if __name__ == "__main__":
    terminal = PSD_Monitoring()
    terminal.run(cycle=200, continous=False)
