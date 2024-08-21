import sys
import os
import json
import time
import easygui 
import pandas as pd 
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import matplotlib.dates as mdates
import subprocess
import psutil
import atexit 
import msvcrt 
from datetime import datetime, timedelta 
from PyQt5.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QWidget, QLineEdit, QPushButton, QDateTimeEdit, QHBoxLayout, QTableWidget, QTableWidgetItem, QMessageBox, QComboBox, QFileDialog, QLabel
from PyQt5.QtCore import QDateTime, Qt
from PyQt5.QtWidgets import *
from PyQt5.QtGui import *
from PyQt5.QtChart import QChart, QChartView, QLineSeries, QValueAxis, QDateTimeAxis
from PyQt5.QtGui import QPainter
from influxdb import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from openpyxl import Workbook




def CheckIsInTimeRange(x,st,end):
    t = pd.Timestamp(x)
    if st<=t and end>=t:
        return True
    else:
        return False

class HistoryBrowser(QMainWindow):
    def __init__(self):
        self.plot_title = "" 
        self.time_buf = []
        self.date_buf = []
        self.relay_buf = []
        self.data_row_col_buf = []
        self.row_buf = []
        self.date_buf = []
        self.point_buf = []
        self.excel_data_buf = []
        self.data_split_buf = 0
        self.check_state = True
        self.i = False
        self.q = False
        self.import_file = True
        self.import_plot = False
        self.quary_enable = False
        self.import_enable = False
        self.databases_plot = False
        self.inside_plot = False
        self.merge_enable = False
        self.crossing_multiple_days = False
        self.quary_null = False
        self.page_turn = False
        self.page_last = True
        self.page_next = True
        self.pages_num = 0
        self.pages_max = 0


        self.setting_addr = "D:\\PSDmonitor\\profile\\SettingMenu.json"
        self.export_path = 'D:\\PSDmonitor\\InfluxDB\\export_data'
        self.folder_path = 'D:\\PSDmonitor\\InfluxDB\\influxdb-1.7.7-1'
        self.influxdb_conf = 'influxd.exe -config influxdb.conf'
        self.influx_exe = '.\\influx.exe'
        
        # 打开JSON
        with open(file=self.setting_addr, encoding='utf-8') as cfg:
            self.dict_cfg = json.load(cfg) 
        self.host = self.dict_cfg["DatabaseClient"]['host']
        self.database = self.dict_cfg["DatabaseClient"]['DBname'][2]
        self.port = self.dict_cfg["DatabaseClient"]['port']
        self.username = self.dict_cfg["DatabaseClient"]['username']
        self.password = self.dict_cfg["DatabaseClient"]['password']
        self.url =  self.host + ":" + str(self.port)
        
        # 获取站点
        self.station = self.dict_cfg["TerminalClient"]['T1']["station"]
        self.station = list(self.station.keys())
        
        super().__init__()
        self.setWindowTitle("地铁屏蔽门监测数据历史记录查询助手")
        self.setWindowIcon(QIcon('favor.ico'))
        self.setGeometry(0, 0, 1200, 800)
        self.showMaximized() 

        # 检查是否已开启数据库
        self.kill_service_by_name('influxd.exe')

        # 启动数据库
        os.chdir(self.folder_path)
        os.popen(self.influxdb_conf) 

        # 连接数据库
        self.client = InfluxDBClient(host=self.host, port=self.port, database=self.database, username=self.username, password=self.password)
        self.database = str(self.database)
        time.sleep(5)
        
        # 启动界面
        self.initUI()
        


    def initUI(self):
        self.site_select = QComboBox()
        self.site_select.addItems(self.station)

        self.direction_select = QComboBox()
        self.direction_select.addItems(["上行", "下行"])

        self.data_select = QComboBox()
        self.data_select.addItems(["数据库", "外部文件"])
        

        self.items_select = QComboBox()
        self.items_select.addItems(["1000", "3000", "5000", "10000", "12000", "15000"])
        
        self.relay = self.dict_cfg["TerminalClient"]['T1']["station"][self.site_select.currentText()][self.direction_select.currentText()]['relay']
        self.name  = self.dict_cfg["TerminalClient"]['T1']["station"][self.site_select.currentText()][self.direction_select.currentText()]['name']
        self.channel = self.dict_cfg["TerminalClient"]['T1']["station"][self.site_select.currentText()][self.direction_select.currentText()]['channel']
        self.relay_mark = list(self.relay.keys())  
        self.relay_mark_name = list(self.relay.values())  
        self.relay_name = list(self.relay.values())
        self.realy_channel = list(self.channel.values())
        self.station = self.dict_cfg["TerminalClient"]['T1']["station"]
        self.relay_chinese_name = list(self.name.values())
        self.table = QTableWidget()
        
        self.chart = QChart()
        self.chart_view = QChartView(self.chart)
        self.chart_view.setRenderHint(QPainter.Antialiasing)

        self.from_date = QDateTimeEdit()
        self.from_date.setCalendarPopup(True)
        self.from_date.setDisplayFormat("yyyy/MM/dd HH:mm:ss")
        self.from_date.setDateTime(QDateTime.currentDateTime().addDays(-1))  # 默认开始时间为当前时间前一天
     
        

        self.to_date = QDateTimeEdit()
        self.to_date.setCalendarPopup(True)
        self.to_date.setDisplayFormat("yyyy/MM/dd HH:mm:ss")
        self.to_date.setDateTime(QDateTime.currentDateTime())  # 默认结束时间为当前时间


        self.set_query_check = QCheckBox("设定24H查询范围")
        self.set_query_check.stateChanged.connect(self.query_check)
        self.set_query_check.toggle()

        self.query_button = QPushButton("查询")
        self.query_button.clicked.connect(self.query_data)
        
        self.plot_button = QPushButton("波形图")
        self.plot_button.clicked.connect(self.plot_data)

        self.import_button = QPushButton("导入")
        self.import_button.clicked.connect(self.import_data)

        self.export_button = QPushButton("导出")
        self.export_button.clicked.connect(self.export_data)
        
        self.clear_button = QPushButton("清除数据")
        self.clear_button.clicked.connect(self.clear_data)
        
        self.next_page_button = QPushButton("下一页")
        self.next_page_button.clicked.connect(self.next_page)

        self.last_page_button = QPushButton("上一页")
        self.last_page_button.clicked.connect(self.last_page)

        self.goto_page_button = QPushButton("跳转")
        self.goto_page_button.clicked.connect(self.goto_page)

        self.goto_input = QLineEdit(self)
        self.goto_input.installEventFilter(self)

        # 添加描述标签并右对齐
        site_label = QLabel("站点：")
        site_label.setAlignment(Qt.AlignmentFlag.AlignLeft|Qt.AlignmentFlag.AlignVCenter)
        direction_label = QLabel("行车方向：")
        # 
        direction_label.setAlignment(Qt.AlignmentFlag.AlignLeft|Qt.AlignmentFlag.AlignVCenter)
        from_date_label = QLabel("开始时间：")
        from_date_label.setAlignment(Qt.AlignmentFlag.AlignLeft|Qt.AlignmentFlag.AlignVCenter)
        to_date_label = QLabel("结束时间：")
        to_date_label.setAlignment(Qt.AlignmentFlag.AlignLeft|Qt.AlignmentFlag.AlignVCenter)
        #
        goto_label = QLabel("转到第")
        goto_label.setAlignment(Qt.AlignmentFlag.AlignLeft|Qt.AlignmentFlag.AlignVCenter)#Qt.AlignCenter | Qt.AlignVCenter
        page_label = QLabel("页")
        page_label.setAlignment(Qt.AlignmentFlag.AlignLeft|Qt.AlignmentFlag.AlignVCenter)
        every_page_label = QLabel("每页数据量")
        every_page_label.setAlignment(Qt.AlignmentFlag.AlignLeft|Qt.AlignmentFlag.AlignVCenter)
        # self.item_label = QLabel("条     ")
        # self.item_label.setAlignment(Qt.AlignmentFlag.AlignLeft)
        self.all_pages_label = QLabel("当前第{}页 共{}页".format(self.pages_max, self.pages_num), self)
        self.all_pages_label.setAlignment(Qt.AlignmentFlag.AlignLeft|Qt.AlignmentFlag.AlignVCenter)

    

        # 布局调整 第一行
        quary_layout = QHBoxLayout()    
        quary_layout.addWidget(site_label)
        quary_layout.addWidget(self.site_select)
        quary_layout.addWidget(direction_label)
        quary_layout.addWidget(self.direction_select)
        quary_layout.addWidget(from_date_label)
        quary_layout.addWidget(self.from_date)
        quary_layout.addWidget(to_date_label)
        quary_layout.addWidget(self.to_date)
        quary_layout.addWidget(every_page_label)
        quary_layout.addWidget(self.items_select)
        quary_layout.setStretch(0, 1)
        quary_layout.setStretch(1, 2)
        quary_layout.setStretch(2, 1)
        quary_layout.setStretch(3, 2)

        # 布局调整 第二行
        quary_pages = QHBoxLayout()
        quary_pages.addWidget(self.set_query_check)
        quary_pages.addWidget(self.last_page_button)
        quary_pages.addWidget(self.next_page_button)
        quary_pages.addWidget(goto_label)
        quary_pages.addWidget(self.goto_input)
        quary_pages.addWidget(page_label)
        quary_pages.addWidget(self.all_pages_label)

        quary_pages.addWidget(self.goto_page_button)
        quary_pages.addWidget(self.query_button)
        quary_pages.addWidget(self.export_button)
        quary_pages.addWidget(self.import_button)
        quary_pages.addWidget(self.plot_button)
        quary_pages.addWidget(self.clear_button)


        # quary_pages.addWidget(self.item_label)


        # button_layout = QHBoxLayout()
        # button_layout.addWidget(self.query_button)
        # button_layout.addWidget(self.plot_button)
        # button_layout.addWidget(self.import_button)
        # button_layout.addWidget(self.export_button)
        #button_layout.addWidget(self.clear_button)

        top_layout = QVBoxLayout()
        top_layout.addLayout(quary_layout)
        # top_layout.addLayout(button_layout)
        top_layout.addLayout(quary_pages)
        
        layout = QVBoxLayout()
        layout.addLayout(top_layout)
        layout.addWidget(self.table)
        # layout.addWidget(self.chart_view)

        container = QWidget()
        container.setLayout(layout)
        self.setCentralWidget(container)


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


    def table_data_change(self, data):
        grouped_data = {}  
        for item in data:  
            key = item[0]  
            if key not in grouped_data:  
                grouped_data[key] = [item[2]]  
            grouped_data[key].append(item[2] if item[1] == 0 else item[2])  
        result = []  
        for key, values in grouped_data.items():  
            result.append(values[1:]) 
        return result 
        
        
    def time_to_decimal(self, time_str):  
        hours, minutes, seconds = map(int, time_str.split(':'))  
        decimal_hours = hours + minutes / 60.0 + seconds / 3600.0  
        return decimal_hours 
    
    
    def time_change(self, time_data):
        try:
            time_record = datetime.strptime(time_data, '%Y-%m-%dT%H:%M:%S.%fZ')  
            dt_plus_8_hours = time_record + timedelta(hours=8)  
            time_record = dt_plus_8_hours.strftime("%Y-%m-%dT%H:%M:%S.%f")
            time_record = self.time_split_point(time_record, 3)
        except:
            time_record = datetime.strptime(time_data, '%Y-%m-%dT%H:%M:%SZ')  
            dt_plus_8_hours = time_record + timedelta(hours=8)  
            time_record = dt_plus_8_hours.strftime("%Y-%m-%dT%H:%M:%S")
        time_record = time_record.replace("T", " ")
        return time_record  
    
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
    

    def query_check(self, state):
        if state == Qt.Checked:  
            self.check_state = True  
        else:  
            self.check_state = False 


    def next_page(self):
        if self.page_turn == True:
            if self.page_next == True:
                self.page_last = True
                self.table.clearContents()
                self.pages_num += 1
                if self.pages_num > self.pages_max:
                    self.pages_num = self.pages_max
                    self.page_next = False
                self.display_data(self.pages_num)
                self.all_pages_label.setText("当前第{}页 共{}页".format(self.pages_num, self.pages_max))
                return self.pages_num


    def last_page(self):
        if self.page_turn == True:
            if self.page_last == True:
                self.page_next = True
                self.table.clearContents()
                self.pages_num -= 1
                if self.pages_num <= 0:
                    self.pages_num = 1
                    self.page_last == False
                self.display_data(self.pages_num)
                self.all_pages_label.setText("当前第{}页 共{}页".format(self.pages_num, self.pages_max))
                return self.pages_num
        

    def goto_page(self):
        if self.page_turn == True:
            try:
                self.page_next = True
                self.page_last = True
                self.table.clearContents()
                self.pages_num = eval(self.goto_input.text())
                if isinstance(self.pages_num, int):
                    if self.pages_num <= 0:
                        self.pages_num = 1
                        QMessageBox.critical(self, '输入错误', "请输入正确页码", QMessageBox.Ok) 
                    elif self.pages_num > self.pages_max:
                        self.pages_num = self.pages_max
                        QMessageBox.warning(self, '警告', "页数超过最大页数", QMessageBox.Ok)
                    self.display_data(self.pages_num)
                    self.all_pages_label.setText("当前第{}页 共{}页".format(self.pages_num, self.pages_max))
                    return self.pages_num
            except:
                QMessageBox.critical(self, '输入错误', "请输入正确页码", QMessageBox.Ok) 
    


    def read_result_data(self, result_data):
        row = 0  
        for point in result_data:  
            time_record = self.time_change(point['time'])
            self.table.insertRow(row)  
            self.data_row_col_buf.append([row, 0, str(time_record)])
            columm = 1  
            for mark in self.relay_mark_name:   
                self.data_row_col_buf.append([row, columm, str(point[mark])])
                columm += 1    
            row += 1
        return self.data_row_col_buf



    def merge_measurement(self, result_point):
        # 整合所有point
        all_point_buf = []
        # first point
        first_point = result_point[0]
        first_point_buf = self.read_result_data(first_point)
        for fp in first_point_buf:
            all_point_buf.append(fp)
        # middle point
        middle_point = [mp for mp in result_point[1:-1]]
        middle_point_buf_ls = [mp for mp in self.read_result_data(middle_point)]
        for middle_point_buf in middle_point_buf_ls:
            for mp in middle_point_buf:
                all_point_buf.append(mp) 
        # last point
        last_point = result_point[len(result_point)-1]
        last_point_buf = self.read_result_data(last_point)
        for lp in last_point_buf:
            all_point_buf.append(lp)  
        self.merge_enable = False
        return all_point_buf
    
        

    def from_Excel(self, result_data):
        # 创建DataFrame列名到表头的映射
        rm1 = self.data_head.copy()
        rm1.insert(0, "time")
        rm2 = self.data_head.copy()
        rm2.insert(0, "time")
        column_mapping = {original: mapped for original, mapped in zip(rm1, rm2)} 
        self.relay_mark_len = len(rm2)
        self.table.setColumnCount(self.relay_mark_len)
        self.table.setHorizontalHeaderLabels(rm2) 
        for row_idx, row in result_data.iterrows():
            for df_column, table_column in column_mapping.items():
                col_idx = list(column_mapping.values()).index(table_column)  
                self.data_row_col_buf.append([row_idx, col_idx, str(row[df_column])])  
        return self.data_row_col_buf
    

    def from_Database(self, result_data):  
        if self.merge_enable:
            return self.merge_measurement(result_data)
        else:
            if self.read_result_data(result_data) == []:
                self.quary_null = True
                QMessageBox.information(self, '查询完成', "未查询到数据", QMessageBox.Ok)
                return None 
            else:
                return self.read_result_data(result_data)
            

    def data_split(self, data_buf):
        start_time = time.time() 
        item_max = int(self.items_select.currentText())
        self.__data_split__ =  self.to_split(data_buf, item_max)
        self.pages_max = len(self.__data_split__)
        end_time = time.time() 
        self.processing_time = end_time - start_time
        return self.__data_split__
    

    def display_data(self, page_num):
        self.table.setUpdatesEnabled(False)
        data_split_buf = self.data_split_buf[page_num-1]
        self.data_group_buf = self.table_data_change(data_split_buf)
        # # 时间随动
        # self.import_time_start, self.import_time_end = self.data_group_buf[0][0], self.data_group_buf[-1][0]
        # self.import_time_start = datetime.strptime(self.import_time_start.split('.')[0]  , "%Y-%m-%d %H:%M:%S") 
        # self.import_time_end = datetime.strptime(self.import_time_end.split('.')[0]  , "%Y-%m-%d %H:%M:%S")  
        # self.from_date.setDateTime(self.import_time_start)
        # self.to_date.setDateTime(self.import_time_end) 
        # 清空现有的表格内容
        self.table.clearContents()
        # 设置表格的行数和列数
        self.col_num = len(self.data_head)+1
        self.num_rows = int(len(data_split_buf)/self.col_num)
        self.table.setRowCount(self.num_rows)
        start_time = time.time() 
        if (page_num > 1) and (page_num < self.pages_max):
          for row, row_data in enumerate(self.data_group_buf):  
            for column, value in enumerate(row_data):  
                if isinstance(value, str):  
                    item = QTableWidgetItem(value)  
                else: 
                    item = QTableWidgetItem(str(value))
                self.table.setItem(row, column, item)
        elif page_num == 1 :
            for data in data_split_buf:
                self.table.setItem(data[0], data[1], QTableWidgetItem(data[2])) 
                #self.table.item(data[0], data[1]).setTextAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
                self.row_buf.append(data[0])
        elif page_num == self.pages_max:
            row = 0
            for data in data_split_buf:
                self.table.setItem(self.row_buf[row], data[1], QTableWidgetItem(data[2])) 
                #self.table.item(self.row_buf[row], data[1]).setTextAlignment(Qt.AlignHCenter | Qt.AlignVCenter)
                row += 1
        end_time = time.time() 
        self.table.setUpdatesEnabled(True)
        self.data_row_col_buf.clear()
        self.display_time = end_time - start_time

    
    def to_split(self, data_buf, items_max): 
        if not data_buf:  
            return []  # 或者抛出异常  
        start_time = time.time()  
        total_data_size = len(data_buf)  
        items_per_block = items_max * (len(self.data_head) + 1)  
        actual_items_per_block = min(items_per_block, total_data_size)  
  
        # 计算实际划分块的数量  
        # 如果items_per_block能整除total_data_size，则直接计算；否则加1  
        num_blocks = total_data_size // actual_items_per_block  
        if total_data_size % actual_items_per_block > 0:  
            num_blocks += 1  
  
        # 数据划分  
        self.data_split_buf = [  
            data_buf[i:i + actual_items_per_block]  
            for i in range(0, total_data_size, actual_items_per_block)  
        ]  
  
        # 注意：这里的pages_max实际上是num_blocks，但命名可能根据上下文有所不同  
        self.pages_max = num_blocks  
        end_time = time.time()  
        self.processing_time = end_time - start_time  
        return self.data_split_buf  
        
        
###########################################################################################################################################         

    def CrossingDayAndSplicingTable(self, start_datetime_info, end_datetime_info, query_day_range):
        start_datetime = datetime.strptime(start_datetime_info, '%Y-%m-%dT%H:%M:%SZ')
        end_datetime = datetime.strptime(end_datetime_info, '%Y-%m-%dT%H:%M:%SZ')
        start_day = start_datetime.day
        end_day = end_datetime.day
        delta = end_datetime - start_datetime  
        self.days_diff = delta.days  # 这将给出整数部分的天数差 
        total_seconds = delta.total_seconds()  
        if self.days_diff > query_day_range:
            QMessageBox.warning(self, '警告', "超过{}天范围，数据量过大".format(query_day_range), QMessageBox.Ok)
        else:
            current_date = start_datetime  
            while current_date < end_datetime:  
                # 将datetime对象转换为仅包含日期的字符串，并添加到列表中  
                date_str = current_date.date().isoformat()  
                self.date_buf.append(date_str)  
                # 增加一天  
                current_date += timedelta(days=1)   
                # 包括结束时间
                if end_datetime.date() not in [datetime.strptime(d, '%Y-%m-%d').date() for d in self.date_buf]:  
                    self.date_buf.append(end_datetime.date().isoformat()) 
            start_datetime =  start_datetime.strftime('%Y-%m-%d')  
            end_datetime = end_datetime.strftime('%Y-%m-%d') 
            index_ = self.date_buf.index(end_datetime) 
            self.date_buf.pop(index_)  
            self.date_buf.append(end_datetime)  
            self.one_day_measurement = self.database

            ######################################逐个查询数据并拼接######################################

            # 查询天数判断
            if (self.days_diff >= 1) and (datetime.now().day == self.s_day) and (datetime.now().day == self.e_day):
                # 1天以上（后期优化）
                self.merge_enable = True

                self.crossing_multiple_days =True
                # 历史某天跨天24H查询
                self.date_buf = [date.replace('-', '') for date in self.date_buf] 

                # 获取所有measurement表名称
                self.measurement_buf = [self.database + "_" + date for date in self.date_buf] 
                self.measurement_buf = [x for i, x in enumerate(self.measurement_buf) if x not in self.measurement_buf[:i]] 

                # 首项measurement
                first_measurement = self.measurement_buf[0]
                first_query = f"SELECT * FROM {first_measurement} WHERE time >= '{self.from_date_str}' AND time < '{start_datetime}T15:59:59Z' AND StationName = \'{self.site}\' AND line = \'{self.direction}\'"
                first_query = self.client.query(first_query)
                first_point = first_query.get_points()

                # 尾项measurement
                last_measurement = self.measurement_buf[len(self.measurement_buf)-1]
                last_query = f"SELECT * FROM {last_measurement} WHERE time >= '{end_datetime}T16:00:00Z' AND time < '{self.to_date_str} AND StationName = \'{self.site}\' AND line = \'{self.direction}\'"
                last_query = self.client.query(last_query)
                last_point = last_query.get_points()

                # 首项point
                self.point_buf.append(first_point)

                if (len(self.measurement_buf) > 2):
                    # 中间连续的measurement
                    middle_measurement_buf = self.measurement_buf[1:-1]
                    for middle_measurement in middle_measurement_buf:
                        middle_query = f"SELECT * FROM {middle_measurement} WHERE StationName = \'{self.site}\' AND line = \'{self.direction}\'"
                        middle_query = self.client.query(middle_query)
                        middle_point = middle_query.get_points()
                        # 中间连续的point
                        self.point_buf.append(middle_point)

                 # 尾项point
                self.point_buf.append(last_point)

                self.measurement_buf.clear()
                return self.point_buf
 

            elif self.days_diff == 0:
                # 单日查询，无需拼接
                if (self.s_day == self.e_day) and (datetime.now().day != self.s_day) and (datetime.now().day != self.e_day):
                    # 历史某天
                    self.one_day_measurement = self.one_day_measurement + "_" + end_datetime.replace('-', '') 
                    self.one_day_query = f"SELECT * FROM {self.one_day_measurement} WHERE time >= '{self.from_date_str}' AND time < '{self.to_date_str}' AND StationName = \'{self.site}\' AND line = \'{self.direction}\'"
                elif (datetime.now().day == self.s_day) and (datetime.now().day == self.s_day):
                    # 当前天
                    self.one_day_query = f"SELECT * FROM {self.one_day_measurement} WHERE time >= '{self.from_date_str}' AND time < '{self.to_date_str}' AND StationName = \'{self.site}\' AND line = \'{self.direction}\'"
                else:
                    # 历史某天
                    self.one_day_measurement = self.one_day_measurement + "_" + end_datetime.replace('-', '') 
                    self.one_day_query = f"SELECT * FROM {self.one_day_measurement} WHERE time >= '{self.from_date_str}' AND time < '{self.to_date_str}' AND StationName = \'{self.site}\' AND line = \'{self.direction}\'"
                one_day_query = self.client.query(self.one_day_query)
                result_point = one_day_query.get_points()
                return result_point

            else:
                # 昨天和今天
                self.one_day_query = f"SELECT * FROM {self.one_day_measurement} WHERE time >= '{self.from_date_str}' AND time < '{self.to_date_str}' AND StationName = \'{self.site}\' AND line = \'{self.direction}\'"
                one_day_query = self.client.query(self.one_day_query)
                result_point = one_day_query.get_points()
                return result_point


                
###########################################################################################################################################         

    def remove_duplicates(self, lst):  
        seen = set()  
        new_list = []  
        for item in lst:  
            if item not in seen:  
                seen.add(item)  
                new_list.append(item)  
        return new_list 
    

    def is_chinese(self, s):  
        import re
        pattern = re.compile(r'[\u4e00-\u9fa5]')  
        return bool(pattern.search(s))
    

    def get_keys_by_value(self, d, value):
        return [k for k, v in d.items() if v == value]  
    
###############################################################################################
###############################################################################################
###############################################################################################
###############################################################################################

    def query_data(self):
        self.q = True
        # 获取列的数量  
        column_count = self.table.columnCount()  
        # 遍历所有列，将表头设置为空字符串  
        for column in range(column_count):  
            self.table.setHorizontalHeaderItem(column, QTableWidgetItem("")) 
        if self.import_enable:
            self.site_select.clear()
            self.direction_select.clear()
            self.site_select.addItems(self.station)
            self.direction_select.addItems(["上行", "下行"])  
            self.from_date.setDateTime(QDateTime.currentDateTime().addDays(-1))
            self.to_date.setDateTime(QDateTime.currentDateTime())  
            self.import_enable = False
        self.pages_num = 1
        self.import_plot = False
        start_datetime = self.from_date.dateTime()
        end_datetime = self.to_date.dateTime()
        start_datetime_info = start_datetime.toString("yyyy-MM-ddTHH:mm:ssZ")
        end_datetime_info = end_datetime.toString("yyyy-MM-ddTHH:mm:ssZ")
        self.s_day = datetime.strptime(start_datetime_info, '%Y-%m-%dT%H:%M:%SZ').day
        self.e_day = datetime.strptime(end_datetime_info, '%Y-%m-%dT%H:%M:%SZ').day
        self.from_date_str = start_datetime.toUTC().toString("yyyy-MM-ddTHH:mm:ssZ")
        self.to_date_str = end_datetime.toUTC().toString("yyyy-MM-ddTHH:mm:ssZ")
        from_date_timestamp = self.from_date.dateTime().toSecsSinceEpoch() * 1000
        to_date_timestamp = self.to_date.dateTime().toSecsSinceEpoch() * 1000
        from_date_timestamp_str = str(self.from_date.dateTime().toUTC().toSecsSinceEpoch() * 1000)
        to_date_timestamp_str = str(self.to_date.dateTime().toUTC().toSecsSinceEpoch() * 1000)
        now_day = datetime.now().date().day
        now_month = datetime.now().date().month
        self.from_day = datetime.strptime(self.from_date_str, '%Y-%m-%dT%H:%M:%SZ').day
        self.to_day = datetime.strptime(self.to_date_str, '%Y-%m-%dT%H:%M:%SZ').day
        from_day = datetime.strptime(self.from_date_str, '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d") 
        to_day = datetime.strptime(self.to_date_str, '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d") 
        self.from_day_t = datetime.strptime(self.from_date_str, '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d") 
        self.to_day_t = datetime.strptime(self.to_date_str, '%Y-%m-%dT%H:%M:%SZ').strftime("%Y-%m-%d") 
        from_month = datetime.strptime(self.from_date_str, '%Y-%m-%dT%H:%M:%SZ').month
        to_month = datetime.strptime(self.to_date_str, '%Y-%m-%dT%H:%M:%SZ').month
        from_date = datetime.strptime(from_day, "%Y-%m-%d")  
        to_date = datetime.strptime(to_day, "%Y-%m-%d") 
        diff_time = to_date - from_date
        diff_time = diff_time.days  
        self.site = self.site_select.currentText()
        self.direction = self.direction_select.currentText()
        to_day_in_future = ((now_day < self.to_day) and (now_month <= to_month))
        from_day_in_future = ((now_day < self.from_day) and (now_month <= from_month))
        self.all_pages_label.setText("当前第{}页 共{}页 ".format("  ", "  "))
     
  
        # 从QDateTime对象中提取日期部分  
        # 注意：QDate是从QDateTime中直接转换而来的，它只包含年、月、日  

    
        self.table.setRowCount(0)  # 清空表格内容
        self.chart.removeAllSeries()
        series = QLineSeries()
        previous_time = None
        previous_value = None
    
        if self.check_state == True:
            month_is_same = (from_month == to_month)
            in_diff_range = ((diff_time < 2) and (diff_time >= 0))
        else:
            month_is_same = True
            in_diff_range = True
        # 获取数据
        if (to_day_in_future or from_day_in_future):
            QMessageBox.warning(self, '警告', "无法查询未来时间的数据", QMessageBox.Ok)
        else:    
            if month_is_same and in_diff_range:
                self.import_file = False
                try:
                    self.relay_channel = {self.station[self.site][self.direction]['name'][key]: value for key, value in self.station[self.site][self.direction]['channel'].items()} 
                except:
                    QMessageBox.critical(self, '错误', "当前选择的查询项有误", QMessageBox.Ok)
                self.relay_buf.clear()
                for key, value in self.relay_channel.items():  
                    self.relay_buf.append(f"{key} (CH{value})")
                self.relay_buf.insert(0, 'time')
                self.relay_buf = self.remove_duplicates(self.relay_buf)
                self.relay_mark_len = len(self.relay_buf)
                self.table.setColumnCount(self.relay_mark_len)
                self.table.setHorizontalHeaderLabels(self.relay_buf)
                self.quary_enable = True
                self.data_head = self.relay_chinese_name

                self.databases_plot = True 
                self.inside_plot = False
                # 数据库数据
                try:
                    start_time = time.time()
                    self.result_point = self.CrossingDayAndSplicingTable(self.from_date_str, self.to_date_str, 1)
                    end_time = time.time()
                    self.quary_time = end_time - start_time
                    self.data_split_buf = self.data_split(self.from_Database(self.result_point))
                    self.buffer_name = self.site + '_' + self.direction
                    if self.pages_num == 1:
                        self.display_data(self.pages_num)
                    #QMessageBox.information(self, '查询完成', "查询时间为{}秒".format(self.quary_time), QMessageBox.Ok) 
                    self.all_pages_label.setText("当前第{}页 共{}页".format(self.pages_num, self.pages_max))
                    self.page_turn = True
                except:
                    if self.quary_null == False:
                        QMessageBox.critical(self, '查询错误', "数据查询异常", QMessageBox.Ok) 
                    else:
                        self.quary_null = False

            else:
                if diff_time < 0:
                    QMessageBox.warning(self, '警告', "查询时间范围设置有误", QMessageBox.Ok) 
                else:
                    QMessageBox.warning(self, '警告', "查询时间范围为24小时内", QMessageBox.Ok)
        self.client.close()

###############################################################################################
###############################################################################################
###############################################################################################
###############################################################################################      
    
    def import_data(self):
        self.q = True
        self.clear_data()
        self.pages_num = 1
        self.databases_plot = False
        self.import_file = True
        self.inside_plot = False
        from_date_str = self.from_date.dateTime().toString("yyyy-MM-ddTHH:mm:ssZ")
        to_date_str = self.to_date.dateTime().toString("yyyy-MM-ddTHH:mm:ssZ")
        from_date_timestamp = self.from_date.dateTime().toSecsSinceEpoch() * 1000
        to_date_timestamp = self.to_date.dateTime().toSecsSinceEpoch() * 1000
        from_date_timestamp_str = str(self.from_date.dateTime().toUTC().toSecsSinceEpoch() * 1000)
        to_date_timestamp_str = str(self.to_date.dateTime().toUTC().toSecsSinceEpoch() * 1000)
        now_day = datetime.now().date().day
        now_month = datetime.now().date().month
        from_day = datetime.strptime(from_date_str, '%Y-%m-%dT%H:%M:%SZ').day
        to_day = datetime.strptime(to_date_str, '%Y-%m-%dT%H:%M:%SZ').day
        from_month = datetime.strptime(from_date_str, '%Y-%m-%dT%H:%M:%SZ').month
        to_month = datetime.strptime(to_date_str, '%Y-%m-%dT%H:%M:%SZ').month
        diff_time = to_day - from_day
        self.site = self.site_select.currentText()
        self.direction = self.direction_select.currentText()
        QMessageBox.information(self, '导入数据', "将会导入所选的整个文件，请选择规定格式的Excel文件", QMessageBox.Ok)
    
        # 导入数据功能实现
        options = QFileDialog.Options()
        file_name, _ = QFileDialog.getOpenFileName(self, "导入数据文件", self.export_path, "All Files (*);;CSV Files (*.csv)", options=options)
        try:
            if file_name:     
                # 在这里处理导入的数据
                start_time = time.time()
                self.excel_import_data = pd.read_excel(file_name)
                self.data_head = [col for col in self.excel_import_data.columns if self.is_chinese(col)] 
                self.data_split_buf = self.data_split(self.from_Excel(self.excel_import_data))
                self.buffer_name = self.site + '_' + self.direction
                if self.pages_num == 1:
                    self.display_data(self.pages_num)
                self.excel_file_name = os.path.basename(file_name).replace('.xlsx', '')
                if self.excel_file_name.count('_') >= 3:
                    self.buffer_name = '_'.join(self.excel_file_name.split('_') [:-1]) 
                    parts = self.buffer_name.split('_') 
                    self.export_site, self.export_direction = parts[0] + '_' + parts[1], parts[2]  
                    # 临时更新combox
                    self.site_select.clear()
                    self.direction_select.clear()
                    self.site_select.addItem(self.export_site)
                    self.direction_select.addItem(self.export_direction)
                else:
                    self.buffer_name = '导入文件名称:' + os.path.basename(file_name)
                    
                # self.excel_import_data = self.excel_import_data[(self.excel_import_data['time'] >= pd.to_datetime(start_date)) & (self.excel_import_data['time'] <= pd.to_datetime(end_date))] 
                end_time = time.time()
                self.quary_time = end_time - start_time
                self.data_split_buf = self.data_split(self.from_Excel(self.excel_import_data))
                if self.pages_num == 1:
                    self.display_data(self.pages_num)
                #QMessageBox.information(self, '查询完成', "查询时间为{}秒".format(self.quary_time), QMessageBox.Ok) 
                self.page_turn = True
                self.all_pages_label.setText("当前第{}页 共{}页".format(self.pages_num, self.pages_max))
                self.import_plot = True
                self.import_enable = True
                self.page_turn = True
                self.page_next = True
                self.page_last = True
        except:
            QMessageBox.critical(self, '导入错误', "文件无法正常导入", QMessageBox.Ok) 
        self.i = True
        self.client.close()


    def export_data(self):
        if self.quary_enable or self.import_enable:
            self.page_turn = False
            self.page_next = False
            self.page_last = False
          
            # 导出数据功能实现
            options = QFileDialog.Options()
            file_name, _ = QFileDialog.getSaveFileName(self,  "导出数据文件", self.export_path, "All Files (*);;CSV Files (*.csv)", options=options)
            if file_name:
                output_file = os.path.basename(file_name)
                excel_file_name = output_file.replace(".csv", ".xlsx")
                root_file_path = file_name.replace("/" + output_file, "")
                excel_file_path = os.path.join(root_file_path, excel_file_name)
                try:
                    relay_list = self.relay_chinese_name 
                    data_list = relay_list.copy()
                    data_list.insert(0, 'time')
                    dispData_df = pd.DataFrame(self.data_group_buf[1:], columns=data_list)  
                    dispData_df.to_excel(excel_file_path, index=False, engine='openpyxl')  
                    QMessageBox.information(self, '导出完成', "文件已自动生成", QMessageBox.Ok) 
                except:
                    QMessageBox.critical(self, '导出错误', "文件无法正常导出", QMessageBox.Ok) 
        self.site_select.clear()
        self.direction_select.clear()
        self.site_select.addItems(self.station)
        self.direction_select.addItems(["上行", "下行"])          
        self.from_date.setDateTime(QDateTime.currentDateTime().addDays(-1))
        self.to_date.setDateTime(QDateTime.currentDateTime())  


    def plot_data(self):
        if ((self.import_plot == True) or (self.databases_plot == True) or (self.inside_plot == True)):
            import Square_Wave_Plot
            try:
                self.page_turn = True
                self.page_next = True
                self.page_last = True
                relay_list = self.data_head
                data_list = relay_list.copy()
                data_list.insert(0, 'time')
                dispData_df = pd.DataFrame(self.data_group_buf[1:], columns=data_list)  
                time_begin = dispData_df['time'].iloc[0]
                time_end = dispData_df['time'].iloc[-1]
                excel_buffer = self.folder_path + '\\' + self.buffer_name + '.xlsx'
                self.plot_title = self.buffer_name + "  ({} - {})".format(time_begin, time_end)
                dispData_df.to_excel(excel_buffer, index=False, engine='openpyxl')  
                dispData_df = pd.read_excel(excel_buffer)
                if os.path.exists(excel_buffer):  
                    os.remove(excel_buffer)
                self.second_window = Square_Wave_Plot.DiplayPlotWindow(self.plot_title, dispData_df[data_list], relay_list)
                self.second_window.showMaximized()
                #Square_Wave_Plot.PlotCanvas(self.plot_title, dispData_df[data_list], relay_list)
            except:
                QMessageBox.critical(self, '绘制波形出错', "无法正常解析数据", QMessageBox.Ok) 
        
           
    def clear_data(self):
        self.import_plot = False
        self.databases_plot = False
        self.inside_plot = False
        self.page_turn = False
        self.quary_enable = False
        self.import_enable = False
        self.i = False
        self.q = False
        self.pages_max = 0
        self.pages_num = 0
        self.table.setRowCount(0)
        # self.from_date.setDateTime(QDateTime.currentDateTime().addDays(-1))
        # self.to_date.setDateTime(QDateTime.currentDateTime())  
        # 获取列的数量  
        column_count = self.table.columnCount()  
        # 遍历所有列，将表头设置为空字符串  
        for column in range(column_count):  
            self.table.setHorizontalHeaderItem(column, QTableWidgetItem("")) 
        self.all_pages_label.setText("当前第{}页 共{}页".format(self.pages_max, self.pages_num))
        self.site_select.clear()
        self.direction_select.clear()
        self.site_select.addItems(self.station)
        self.direction_select.addItems(["上行", "下行"]) 

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
    app = QApplication(sys.argv)
    browser = HistoryBrowser()
    browser.showMaximized()
    browser.client.close()
    sys.exit(app.exec_())

