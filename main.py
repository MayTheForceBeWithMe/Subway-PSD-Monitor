from pymodbus.client import ModbusTcpClient
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb import InfluxDBClient
from datetime import datetime  
import threading
import time
import json


class PSD_Monitoring(object):
    
    def __init__(self, StationName, StationLine, StationNum):
        self.device_num = []
        self.ModbusClient_ip = []
        self.ModbusClient_port = []
        self.current_time = datetime.now()  
        self.station_info = {"name": StationName, "line": StationLine, "num": StationNum}
        self.collect_err = {"STWJ": None, "KMJ": None, "GMJ": None, "MGJ": None, "QCJ": None, "LZ-LF": None, "KZ-KF": None, "Notes": ''}

    def add_devices(self, NUM, IP, PORT):
        self.device_num.append(NUM)
        self.ModbusClient_ip.append(IP)
        self.ModbusClient_port.append(PORT)
        
    def read_config_file(self):
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
                    notes = self.status_judgment(read_result)
                    collect_set = {"STWJ": read_result[6], "KMJ": read_result[5], "GMJ": read_result[4], "MGJ": read_result[3], "QCJ": read_result[2], "LZ-LF": read_result[1], "KZ-KF": read_result[0], "Notes": notes}
                    time.sleep(T)
                    #self.DataBase_send(devices[1], collect_set)
                    
                    #print("设备{}中读取到寄存器的值：{}".format(devices[0], read_result))  
                    print(collect_set)
                    
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
        # 信号电源 | 机电电源 | 互锁解除 | 门关好 | 关门 | 开门 | 车停稳
        pass

              
    def run(self):
        # checking devices
        for i in range(len(self.device_num)):
            print("设备{}\tIP地址：{}\t端口号：{}".format(self.device_num[i], self.ModbusClient_ip[i], self.ModbusClient_port[i]))
    
        # TCP connect
        for i in range(len(self.device_num)):
            device = [self.device_num[i], self.ModbusClient_ip[i], self.ModbusClient_port[i]]
            threading.Thread(target=self.receive_data(device, 0.25)).start()
                
                
    def DataBase_connect(self, HOST, PORT, USER, PASSWORD, DBNAME):
        # connect to data base 
        self.DBclient = InfluxDBClient(HOST, PORT, USER, PASSWORD)
        self.DBclient.switch_database(DBNAME)
        
    def DataBase_send(self, DeviceIP, DATA):
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
                "time": self.current_time,
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
        self.DBclient.write_points(monitor_data)

            
if __name__ == "__main__":
    Station = PSD_Monitoring('马泉营', '上行', 'S4')
    Station.add_devices(1, '192.168.1.250', 502)
    Station.add_devices(2, '192.168.1.251', 502)
    Station.run()

        
        
        