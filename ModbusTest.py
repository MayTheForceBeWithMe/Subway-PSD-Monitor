from pymodbus.client import ModbusTcpClient
import threading
import QT_test


class Modbus_MainTest(object):
    
    # 主测试
    def __init__(self, IP, PORT):
        self.err = 0
        self.IP = IP
        self.PORT = PORT
        
    def receive_data(self):
        # TCP连接
        self.client = ModbusTcpClient(self.IP, self.PORT)
        self.client.connect()
        # 接收数据
        try:
            while True:
                read_result = self.client.read_holding_registers(0, 10)
                if read_result.isError():
                    print("数据接收错误")
                    break
                else:
                    read_result = bin(read_result.registers[0])[2:]
                    
                    STWJ = read_result[6]
                    KWJ =  read_result[5]
                    GMJ =  read_result[4]
                    MGJ =  read_result[3]
                    QCJ =  read_result[2]
                    LZLF = read_result[1]
                    KZKL = read_result[0]
                    #ui.GetData(read_result)
                    print("读取到寄存器中的值：", read_result)
                    print("STWJ:{}\tKWJ:{}\tGMJ:{}\tMGJ:{}\tQCJ:{}\tLZ-LF:{}\t KZ-KL:{}".format(STWJ, KWJ, GMJ, MGJ, QCJ, LZLF, KZKL))
                
        except:
            print("连接错误")
            self.client.close()
            self.err = -1
            return self.err
            
        print("连接已中断")
        self.client.close()
        
    def data_recv_thread(self):
        # 读数据的线程
        ReadData_thread = threading.Thread(args=self.receive_data, daemon=True)
        ReadData_thread.start()
        
if __name__ == "__main__":
    ModbusTest = Modbus_MainTest("192.168.1.250", 502)
    ModbusTest.receive_data()