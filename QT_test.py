from pymodbus.client import ModbusTcpClient
from PyQt5 import QtCore, QtGui, QtWidgets
from PyQt5.QtCore import QThread, pyqtSignal, QMetaObject, Qt 
from pyqtgraph import PlotWidget
import numpy as np
import threading
import time
import sys
import ModbusTest


class Main():
    
    def __init__(self):
        super().__init__()
        self.setupUi
        self.ModbusTCPset = {"ip":"192.168.1.250", "port": 502}
        
        
        
    # 界面设置
    def setupUi(self, Dialog):
        Dialog.setObjectName("Dialog")
        Dialog.resize(1050, 700)
        self.verticalLayout_6 = QtWidgets.QVBoxLayout(Dialog)
        self.verticalLayout_6.setObjectName("verticalLayout_6")
        self.horizontalLayout = QtWidgets.QHBoxLayout()
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.verticalGroupBox = QtWidgets.QGroupBox(Dialog)
        self.verticalGroupBox.setObjectName("verticalGroupBox")
        self.verticalLayout_3 = QtWidgets.QVBoxLayout(self.verticalGroupBox)
        self.verticalLayout_3.setContentsMargins(10, 10, 10, 10)
        self.verticalLayout_3.setObjectName("verticalLayout_3")
        self.tabWidget = QtWidgets.QTabWidget(self.verticalGroupBox)
        self.tabWidget.setObjectName("tabWidget")
        self.tab_3 = QtWidgets.QWidget()
        self.tab_3.setObjectName("tab_3")
        self.horizontalLayout_11 = QtWidgets.QHBoxLayout(self.tab_3)
        self.horizontalLayout_11.setObjectName("horizontalLayout_11")
        self.s2__receive_text = QtWidgets.QTextBrowser(self.tab_3)
        self.s2__receive_text.setFrameShape(QtWidgets.QFrame.NoFrame)
        self.s2__receive_text.setLineWidth(1)
        self.s2__receive_text.setLineWrapMode(QtWidgets.QTextEdit.WidgetWidth)
        self.s2__receive_text.setObjectName("s2__receive_text")
        self.horizontalLayout_11.addWidget(self.s2__receive_text)
        self.tabWidget.addTab(self.tab_3, "")
        self.tab2 = QtWidgets.QWidget()
        self.tab2.setObjectName("tab2")
        self.horizontalLayout_12 = QtWidgets.QHBoxLayout(self.tab2)
        self.horizontalLayout_12.setObjectName("horizontalLayout_12")
        self.plotWidget = PlotWidget(self.tab2)
        self.plotWidget.setObjectName("plotWidget")
        self.horizontalLayout_12.addWidget(self.plotWidget)
        self.formGroupBox = QtWidgets.QGroupBox(self.tab2)
        self.formGroupBox.setObjectName("formGroupBox")
        self.verticalLayout_2 = QtWidgets.QVBoxLayout(self.formGroupBox)
        self.verticalLayout_2.setObjectName("verticalLayout_2")
        self.horizontalLayout_5 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_5.setObjectName("horizontalLayout_5")
        self.s1__lb_3 = QtWidgets.QLabel(self.formGroupBox)
        self.s1__lb_3.setObjectName("s1__lb_3")
        self.horizontalLayout_5.addWidget(self.s1__lb_3)
        self.s1__box_3 = QtWidgets.QComboBox(self.formGroupBox)
        self.s1__box_3.setObjectName("s1__box_3")
        self.s1__box_3.addItem("")
        self.horizontalLayout_5.addWidget(self.s1__box_3)
        self.verticalLayout_2.addLayout(self.horizontalLayout_5)
        self.horizontalLayout_4 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_4.setObjectName("horizontalLayout_4")
        self.s1__lb_4 = QtWidgets.QLabel(self.formGroupBox)
        self.s1__lb_4.setObjectName("s1__lb_4")
        self.horizontalLayout_4.addWidget(self.s1__lb_4)
        self.s1__box_4 = QtWidgets.QComboBox(self.formGroupBox)
        self.s1__box_4.setObjectName("s1__box_4")
        self.s1__box_4.addItem("")
        self.horizontalLayout_4.addWidget(self.s1__box_4)
        self.verticalLayout_2.addLayout(self.horizontalLayout_4)
        self.label = QtWidgets.QLabel(self.formGroupBox)
        self.label.setObjectName("label")
        self.verticalLayout_2.addWidget(self.label)
        self.doubleSpinBox = QtWidgets.QDoubleSpinBox(self.formGroupBox)
        self.doubleSpinBox.setProperty("value", 0.00)
        self.doubleSpinBox.setObjectName("doubleSpinBox")
        self.doubleSpinBox.valueChanged.connect(self.SpinBoxControl)
        self.verticalLayout_2.addWidget(self.doubleSpinBox)
        self.state_label = QtWidgets.QLabel(self.formGroupBox)
        self.state_label.setText("未连接")
        self.state_label.setTextFormat(QtCore.Qt.AutoText)
        self.state_label.setScaledContents(True)
        self.state_label.setAlignment(QtCore.Qt.AlignRight|QtCore.Qt.AlignTrailing|QtCore.Qt.AlignVCenter)
        self.state_label.setObjectName("state_label")
        self.verticalLayout_2.addWidget(self.state_label)
        self.verticalLayout = QtWidgets.QVBoxLayout()
        self.verticalLayout.setObjectName("verticalLayout")
        self.ClearData_Message = QtWidgets.QMessageBox()
        self.open_button = QtWidgets.QPushButton(self.formGroupBox)
        self.open_button.setObjectName("open_button")
        self.open_button.setCheckable(True) 
        self.verticalLayout.addWidget(self.open_button)
        self.s2__clear_button = QtWidgets.QPushButton(self.formGroupBox)
        self.s2__clear_button.setObjectName("s2__clear_button")
        self.verticalLayout.addWidget(self.s2__clear_button)
        self.close_button = QtWidgets.QPushButton(self.formGroupBox)
        self.close_button.setObjectName("close_button")
        self.close_button.setCheckable(True)
        self.verticalLayout.addWidget(self.close_button)
        self.verticalLayout_2.addLayout(self.verticalLayout)
        self.horizontalLayout_12.addWidget(self.formGroupBox)
        self.tabWidget.addTab(self.tab2, "")
        self.verticalLayout_3.addWidget(self.tabWidget)
        self.horizontalLayout.addWidget(self.verticalGroupBox)
        self.verticalLayout_6.addLayout(self.horizontalLayout)
        self.horizontalLayout_9 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_9.setObjectName("horizontalLayout_9")
        self.groupBox = QtWidgets.QGroupBox(Dialog)
        self.groupBox.setObjectName("groupBox")
        self.horizontalLayout_14 = QtWidgets.QHBoxLayout(self.groupBox)
        self.horizontalLayout_14.setObjectName("horizontalLayout_14")
        self.tableWidget = QtWidgets.QTableWidget(self.groupBox)
        self.tableWidget.setObjectName("tableWidget")
        self.tableWidget.setColumnCount(10)
        self.tableWidget.setRowCount(1)
        item = QtWidgets.QTableWidgetItem()
        font = QtGui.QFont()
        font.setPointSize(9)
        item.setFont(font)
        self.tableWidget.setVerticalHeaderItem(0, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(0, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(1, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(2, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(3, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(4, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(5, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(6, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(7, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(8, item)
        item = QtWidgets.QTableWidgetItem()
        self.tableWidget.setHorizontalHeaderItem(9, item)
        self.horizontalLayout_14.addWidget(self.tableWidget)
        self.horizontalLayout_9.addWidget(self.groupBox)
        self.horizontalLayout_8 = QtWidgets.QHBoxLayout()
        self.horizontalLayout_8.setObjectName("horizontalLayout_8")
        self.horizontalLayout_9.addLayout(self.horizontalLayout_8)
        self.verticalLayout_6.addLayout(self.horizontalLayout_9)

        self.retranslateUi(Dialog)
        self.tabWidget.setCurrentIndex(1)
        QtCore.QMetaObject.connectSlotsByName(Dialog)
        self.StartLink_ButtonControl()
        self.StopLink_ButtonControl()
        self.ClearData_ButtonControl()
        

    def retranslateUi(self, Dialog):
        _translate = QtCore.QCoreApplication.translate
        Dialog.setWindowTitle(_translate("Dialog", "Dialog"))
        
        self.verticalGroupBox.setTitle(_translate("Dialog", "接收区"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab_3), _translate("Dialog", "异常跳变显示"))
        self.formGroupBox.setTitle(_translate("Dialog", "ModbusTCP设置"))
        self.s1__lb_3.setText(_translate("Dialog", "IP地址：  "))
        self.s1__box_3.setItemText(0, _translate("Dialog", self.ModbusTCPset['ip']))
        self.s1__lb_4.setText(_translate("Dialog", "端口号："))
        self.s1__box_4.setItemText(0, _translate("Dialog", str(self.ModbusTCPset['port'])))
        self.label.setText(_translate("Dialog", "采样周期（s）"))
        self.open_button.setText(_translate("Dialog", "开始连接"))
        self.s2__clear_button.setText(_translate("Dialog", "清除接收"))
        self.close_button.setText(_translate("Dialog", "断开连接"))
        self.tabWidget.setTabText(self.tabWidget.indexOf(self.tab2), _translate("Dialog", "波形显示"))
        self.groupBox.setTitle(_translate("Dialog", "数据列表"))
        item = self.tableWidget.verticalHeaderItem(0)
        # item.setText(_translate("Dialog", "1"))
        item = self.tableWidget.horizontalHeaderItem(0)
        item.setText(_translate("Dialog", "日期"))
        item = self.tableWidget.horizontalHeaderItem(1)
        item.setText(_translate("Dialog", "时间"))
        item = self.tableWidget.horizontalHeaderItem(2)
        item.setText(_translate("Dialog", "车停稳"))
        item = self.tableWidget.horizontalHeaderItem(3)
        item.setText(_translate("Dialog", "机电电源"))
        item = self.tableWidget.horizontalHeaderItem(4)
        item.setText(_translate("Dialog", "信号电源"))
        item = self.tableWidget.horizontalHeaderItem(5)
        item.setText(_translate("Dialog", "开门"))
        item = self.tableWidget.horizontalHeaderItem(6)
        item.setText(_translate("Dialog", "关门"))
        item = self.tableWidget.horizontalHeaderItem(7)
        item.setText(_translate("Dialog", "门关好"))
        item = self.tableWidget.horizontalHeaderItem(8)
        item.setText(_translate("Dialog", "互锁解除"))
        item = self.tableWidget.horizontalHeaderItem(9)
        item.setText(_translate("Dialog", "备注"))

# ——————————————————————————————————————————————————————————————————————————————————————————————————

    def StartLink_ButtonControl(self):
        self.open_button.clicked.connect(self.StartLink)
        

    def StopLink_ButtonControl(self):
        self.close_button.clicked.connect(self.StopLink)

    def ClearData_ButtonControl(self):
        self.s2__clear_button.clicked.connect(self.ClearData)
                
    def SpinBoxControl(self, BoxValue):
        print(BoxValue)
        return BoxValue
    
# ——————————————————————————————————————————————————————————————————————————————————————————————————
        
    def StartLink(self):
        self.state_label.setText("正在连接") 
        self.Modbus_Thread = ModbusRunThread()
        self.Modbus_Thread.start()
         
    def StopLink(self):
        self.state_label.setText("中断连接")
        self.Modbus_Thread.requestInterruption()
       

    def ClearData(self):
        print("清除数据")
        pass

# ———————————————————————————————————————————————————————————————————————————————————————————————————

class ModbusRunThread(QThread):
    def _init_(self):
        super()._init_()
        
    def run(self):
        collect_data = ModbusTest.Modbus_MainTest(Main().ModbusTCPset['ip'], Main().ModbusTCPset['port'])
        if(collect_data.receive_data() == -1):
            ui.state_label.setText("连接已中断")
        else:
            collect_data.receive_data()
            
                
if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    Dialog = QtWidgets.QDialog()
    ui = Main()
    ui.setupUi(Dialog)
    Dialog.setWindowTitle('屏蔽门记录波形测试')
    Dialog.show()
    sys.exit(app.exec_())
       
            
    



    