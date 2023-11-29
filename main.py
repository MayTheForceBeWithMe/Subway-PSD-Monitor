from pymodbus.client import ModbusTcpClient
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb import InfluxDBClient
from datetime import datetime  
import threading
import time
import json


class PSD_Monitoring(object):
    
    def __init__(self, StationName, StationLine, StationMark):
        self.device_num = []
        self.ModbusClient_ip = []
        self.ModbusClient_port = []
        self.data_sender = [] 
        self.database_info = {'host': 'localhost', 'port': 8086, 'username': 'WangShiji', 'password': 'cdf5cac0bcaa', 'DBname': StationName}
        self.station_info = {"name": StationName, "line": StationLine, "mark": StationMark}
        self.station_client = {}
        self.server_client = {}
        self.collect_err = {"STWJ": None, "KMJ": None, "GMJ": None, "MGJ": None, "QCJ": None, "LZLF": None, "KZKF": None, "Notes": None}
        self.collect_nop = []
        self.previous_data = []
        self.current_data = []
        self.notes_nop = []
        self.collect_notes = None
        self.current_note = None

    
    def add_devices(self, NUM, IP, PORT):
        self.device_num.append(NUM)
        self.ModbusClient_ip.append(IP)
        self.ModbusClient_port.append(PORT)
        
        
    def display_devices(self):
        for i in range(len(self.device_num)):
            print("设备{}\tIP地址：{}\t端口号：{}".format(self.device_num[i], self.ModbusClient_ip[i], self.ModbusClient_port[i])) 
        
        
    def read_config_file(self, file_name):
        with open(str(file_name), encoding='utf-8') as cfg:
            dict_cfg = json.load(cfg)
            # getting config
            self.station_client['host'] = dict_cfg['client']['host']
            self.station_client['port'] = dict_cfg['client']['port']
            self.station_info['client'] = self.station_client
            self.add_devices(0, self.station_client['host'], self.station_client['port'])
            # server
            try:
                server_num = dict_cfg['server']
                for i in range(server_num):
                    n = i + 1
                    server = 'server' + str(n)
                    self.server_client['host'] = dict_cfg[server]['host']
                    self.server_client['port'] = dict_cfg[server]['port']
                    self.add_devices(n, self.server_client['host'], self.server_client['port'])
            except:
                pass
           
  
    def receive_data(self, devices, cycle, real_time):
        print("\n正在连接")
        TCPclient = ModbusTcpClient(devices[1],  devices[2])
        change_flag = False
        while True:
            try:     
                if not TCPclient.connect():   
                    print("设备{}无法连接".format(devices[0]))   
                    break
                
                read_result = TCPclient.read_holding_registers(0, 10)
                # every data are not erro or void, then display
                if not read_result.isError():   
                    # geting data that starts with modbus bit 1 from the registers
                    read_result = bin(read_result.registers[0])[2:]
                    # gertting notes and adding it to data set
                    note = self.alarm_notes(read_result)
                    data_set = {"STWJ": read_result[6], "KMJ": read_result[5], "GMJ": read_result[4], "MGJ": read_result[3], "QCJ": read_result[2], "LZLF": read_result[1], "KZKF": read_result[0], "Notes": note}
                    # getting effective data
                    if note is not None:    
                        try:
                            device_IP = devices[1]
                            alarm_time = datetime.now()
                            alarm_time_UTC = datetime.utcnow()
                            # alarm working only for data changes and then writing to database
                            if change_flag:
                                if self.upload(data_set['Notes'], real_time):
                                    self.DataBase_send(device_IP, alarm_time_UTC, data_set) 
                                    print("\n站名：{} - {}\t编号：{}\t报警时间：{}\t设备IP：{}\t上报数据：{}".format(self.station_info['name'], self.station_info['line'], self.station_info['mark'], alarm_time, device_IP, data_set))
                            else:
                                # getting first data for the first time
                                self.DataBase_send(device_IP, alarm_time_UTC, data_set)
                                change_flag = True
                        except:
                            pass
                    else:
                        pass
                    time.sleep(cycle/1000)
                else:   
                    print("设备{}数据接收错误".format(devices[0]))
                    break
                   
            except:
                note = "设备{}连接异常".format(devices[0])
                self.collect_err['Notes'] = note
                print(self.collect_err)
                TCPclient.close() 
        TCPclient.close()        
        print("连接已中断")
        
        
    def upload(self, change, real_time):
        if real_time == False:
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
        self.collect_nop.append([collect[6], collect[5], collect[4], collect[3], collect[2], collect[1], collect[0]])
        if len(self.collect_nop) == 2:
            # getting data from previous status and current status
            self.previous_data =  list(map(int, self.collect_nop[0]))
            self.current_data = list(map(int, self.collect_nop[1]))
            self.collect_nop.clear()
            # the train has not arrived
            if self.previous_data ==  [0, 0, 1, 1, 0, 1, 1] and self.current_data == [0, 0, 1, 1, 0, 1, 1]:
                return '无列车停靠'
            # response signal from the action of the PSD when the train arriving 
            if (self.previous_data == [0, 0, 1, 1, 0, 1, 1] and self.current_data == [1, 0, 1, 0, 0, 1, 1]) or \
               (self.previous_data == [1, 0, 1, 0, 0, 1, 1] and self.current_data == [1, 0, 1, 0, 0, 1, 1]):
                return '列车停稳'
            if (self.previous_data == [1, 0, 1, 0, 0, 1, 1] and self.current_data == [1, 1, 0, 0, 0, 1, 1]) or \
               (self.previous_data == [1, 1, 0, 0, 0, 1, 1] and self.current_data == [1, 1, 0, 0, 0, 1, 1]):
                return '开门'
            if (self.previous_data == [1, 0, 1, 0, 0, 1, 1] and self.current_data == [1, 0, 1, 0, 0, 1, 1]) or \
               (self.previous_data == [1, 0, 1, 0, 0, 1, 1] and self.current_data == [1, 0, 1, 0, 0, 1, 1]):
                return '关门'
            if (self.previous_data == [1, 0, 1, 0, 0, 1, 1] and self.current_data == [1, 0, 1, 1, 0, 1, 1]) or \
               (self.previous_data == [1, 0, 1, 1, 0, 1, 1] and self.current_data == [1, 0, 1, 1, 0, 1, 1]):
                return '门关好'
            if (self.previous_data == [1, 0, 1, 1, 0, 1, 1] and self.current_data == [0, 0, 1, 1, 0, 1, 1]):
                return '列车发车'
            #  response signal about abnormal alarm
            if (self.previous_data == [0, 0, 0, 0, 0, 0, 0] and self.current_data == [0, 0, 0, 0, 0, 0, 0]) or \
               (self.previous_data == [1, 1, 1, 1, 1, 1, 1] and self.current_data == [1, 1, 1, 1, 1, 1, 1]):
                return '输入异常'
            if (self.previous_data == [0, 0, 1, 1, 0, 1, 1] and self.current_data == [1, 1, 0, 1, 0, 1, 1]) or \
               (self.previous_data == [1, 1, 0, 1, 0, 1, 1] and self.current_data == [1, 1, 0, 1, 0, 1, 1]):
                return '门关好异常落下'
            if (self.previous_data[5] == 1 and self.current_data[5] == 0) or \
               (self.previous_data[5] == 0 and self.current_data[5] == 0):
                return '机电电源异常'
            if (self.previous_data[6] == 1 and self.current_data[6] == 0) or \
               (self.previous_data[6] == 0 and self.current_data[6] == 0):
                return '信号电源异常'
        else:
            return None
        
                    
    def DataBase_connect(self, HOST, PORT, USER, PASSWORD):
        try:
            # connect to database (this is a test)
            self.DBclient = InfluxDBClient(HOST, PORT, USER, PASSWORD)
            # clearing database and then creating a new database again
            self.DBclient.drop_database(self.database_info['DBname'])   
            self.DBclient.create_database(self.database_info['DBname'])   
            self.DBclient.switch_database(self.database_info['DBname'])     
        except:     
            print('\nInfluxdb未连接')
            
            
    def DataBase_send(self, DeviceIP, current_time, fields):
        # send data to data base
        monitor_data = \
        [
            {
                "measurement": self.station_info['name'],
                "tags": 
                {
                    "DeviceIP": DeviceIP,
                    "Line": self.station_info['line'],
                },
                "time": current_time,
                "fields": 
                {
                    "STWJ": fields['STWJ'],
                    "KMJ": fields['KMJ'],
                    "GMJ": fields['GMJ'],
                    "MGJ": fields['MGJ'],
                    "QCJ": fields['QCJ'],
                    "LZLF": fields['LZLF'],
                    "KZKF": fields['KZKF'],
                    "Notes": fields['Notes'],
                }
            }
        ]
        self.DBclient.write_points(monitor_data)

 
    def run(self, cycle, real_time):
        # conncting to the Influxdb 
        self.DataBase_connect(self.database_info['host'], self.database_info['port'], self.database_info['username'], self.database_info['password'])
        # ModbusTCP connecting and create device threads one by one
        for i in range(len(self.device_num)):
            device = [self.device_num[i], self.ModbusClient_ip[i], self.ModbusClient_port[i]]
            # starting thread to collect data
            threading.Thread(target=self.receive_data, kwargs={'devices': device, 'cycle': cycle, 'real_time': real_time}).start()

            
if __name__ == "__main__":
    Station = PSD_Monitoring('马泉营', '上行', 'S4')
    Station.read_config_file('subway/setting.json')
    Station.display_devices()
    Station.run(cycle=200, real_time=True)
