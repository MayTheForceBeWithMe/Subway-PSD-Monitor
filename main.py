from pymodbus.client import ModbusTcpClient
from influxdb_client import InfluxDBClient, Point, WritePrecision
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
        self.nop_time = datetime.now()
        self.database_info = {'host': 'localhost', 'port': 8086, 'username': 'WangShiji', 'password': 'cdf5cac0bcaa', 'DBname': StationName}
        self.station_info = {"name": StationName, "line": StationLine, "mark": StationMark}
        self.station_client = {}
        self.server_client = {}
        self.collect_err = {"STWJ": None, "KMJ": None, "GMJ": None, "MGJ": None, "QCJ": None, "LZ-LF": None, "KZ-KF": None, "Notes": None}
        self.collect_nop = []
        self.collect_nop0 = []
        self.collect_nop_front = []
        self.collect_nop_now = []
        

    def add_devices(self, NUM, IP, PORT):
        self.device_num.append(NUM)
        self.ModbusClient_ip.append(IP)
        self.ModbusClient_port.append(PORT)
        
    def read_config_file(self):
        with open(str('subway/setting.json'), encoding='utf-8') as cfg:
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
           
  
    def receive_data(self, devices, T):
        print("\n正在连接")
        client = ModbusTcpClient(devices[1],  devices[2])
        while True:
            try:     
                if not client.connect():   
                    print("设备{}无法连接".format(devices[0]))   
                    break
                
                read_result = client.read_holding_registers(0, 10)
                if not read_result.isError():   
                    read_result = bin(read_result.registers[0])[2:]
                    notes = self.alarm_judgment(read_result)
                    if notes is not None:
                        collect_set = {"STWJ": read_result[6], "KMJ": read_result[5], "GMJ": read_result[4], "MGJ": read_result[3], "QCJ": read_result[2], "LZ-LF": read_result[1], "KZ-KF": read_result[0], "Notes": notes}
                        # Writing to data base
                        #self.DataBase_send(devices[1],self.nop_time, collect_set)
                        #print("设备{}中读取到寄存器的值：{}".format(devices[0], read_result))  
                        print(collect_set)
                    else:
                        pass
                    time.sleep(T)
                else:   
                    print("设备{}数据接收错误".format(devices[0]))
                    break
                        
            except:
                notes = "设备{}连接异常".format(devices[0])
                self.collect_err['Notes'] = notes
                print(self.collect_err)
                client.close() 
        client.close()        
        print("连接已中断")
        
        
    def alarm_judgment(self, signal):
        # data alarm processing
        self.collect_nop = [signal[6], signal[5], signal[4], signal[3], signal[2], signal[1], signal[0]]
        self.collect_nop0.append(self.collect_nop)
        if len(self.collect_nop0) == 2:
            same = all(a == b for a, b in zip(self.collect_nop0[0], self.collect_nop0[1]))
            for a, b in zip(self.collect_nop0[0], self.collect_nop0[1]):
                self.collect_nop_front.append(a)
                self.collect_nop_now.append(b)
             # train arriving

            # alarm
        if same == True:
            return '无列车停靠'
               
        else:
            return None

                     
    def DataBase_connect(self, HOST, PORT, USER, PASSWORD):
        # connect to data base (this is a test)
        self.DBclient = InfluxDBClient(HOST, PORT, USER, PASSWORD)
        # clearing data base
        self.DBclient.drop_database(self.database_info['DBname'])  
        # and then creating   
        self.DBclient.create_database(self.database_info['DBname'])   
        self.DBclient.switch_database(self.database_info['DBname'])     
                          
        
    def DataBase_send(self, DeviceIP, current_time, DATA):
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
                    "STWJ": DATA['STWJ'],
                    "KMJ": DATA['KMJ'],
                    "GMJ": DATA['GMJ'],
                    "MGJ": DATA['MGJ'],
                    "QCJ": DATA['QCJ'],
                    "LZ-LF": DATA['LZ-LF'],
                    "KZ-KF": DATA['KZ-KF'],
                    "Notes": DATA['Notes'],
                }
            }
        ]
        #print(monitor_data)
        self.DBclient.write_points(monitor_data)
        self.DBclient.close()
        
        
    def run(self):
        ''' main '''
        # reading devices config from Json
        self.read_config_file()
        # checking devices
        for i in range(len(self.device_num)):
            print("设备{}\tIP地址：{}\t端口号：{}".format(self.device_num[i], self.ModbusClient_ip[i], self.ModbusClient_port[i]))  
            
        # Conncting to the Influxdb 
        #self.DataBase_connect(self.database_info['host'], self.database_info['port'], self.database_info['username'], self.database_info['password'])
        
        # ModbusTCP connecting and create device threads one by one
        for i in range(len(self.device_num)):
            device = [self.device_num[i], self.ModbusClient_ip[i], self.ModbusClient_port[i]]
            # starting thread to collect data
            threading.Thread(target=self.receive_data, kwargs={'devices': device, 'T': 0.25}).start()

            
if __name__ == "__main__":
    Station = PSD_Monitoring('马泉营', '上行', 'S4')
    Station.run()
