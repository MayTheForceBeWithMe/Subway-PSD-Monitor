from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb import InfluxDBClient
from datetime import datetime  


MyCount = \
{
   "host": 'localhost',
   "port":  8086,
   "username": 'WangShiji',
   "password": 'cdf5cac0bcaa',    
}
  

DBclient = InfluxDBClient(MyCount['host'], MyCount['port'], MyCount['username'], MyCount['password'])
DBclient.switch_database('地铁屏蔽门监测')
current_time = datetime.now()  

monitor_data = [
    {
        "measurement": "站名",
        "tags": 
        {
            "设备IP": "192.168.1.250",
            "位置": "上行",
            "状态": "无异常"
        },
        "time": current_time,
        "fields": 
        {
            "车停稳": 1,
            "开门": 0,
            "关门": 1,
            "门关好": 1,
            "互锁解除": 0,
            "机电电源": 1,
            "信号电源": 1,
            "备注": "列车停稳"
        }
    }
]

DBclient.write_points(monitor_data)


