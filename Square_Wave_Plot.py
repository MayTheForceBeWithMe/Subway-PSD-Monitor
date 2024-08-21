import numpy as np
import matplotlib.pyplot as plt
from PyQt5.QtWidgets import QApplication, QMainWindow, QVBoxLayout, QWidget
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
from matplotlib.widgets import Cursor
from matplotlib.dates import DateFormatter  
from matplotlib.dates import num2date
from matplotlib.widgets import Button
from datetime import datetime, timedelta
import matplotlib.dates as mdates
import pandas as pd


plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号


class PlotCanvas(FigureCanvas):
    def __init__(self, title, values, relay_list):
        self.title = title
        self.values = values
        self.relay_list = relay_list
        # 坐标参数设置
        # plt.xticks(rotation=90)
        super().__init__()
        self.fig, self.ax = plt.subplots(figsize=(12, 6))
        # 坐标显示设置
        self.timestamps = pd.to_datetime(self.values['time']).values
        self.values = self.values[self.relay_list]
        self.plot_square_wave()
        #self.cursor = Cursor(self.ax, useblit=True, color='red', linewidth=1)
        self.annot = self.ax.annotate("", xy=(0, 0), xytext=(-40, 40),
                                      textcoords="offset points",
                                      bbox=dict(boxstyle="round,pad=0.5", fc="yellow", alpha=0.5),
                                      arrowprops=dict(arrowstyle="->"))
        self.annot.set_visible(False)
        self.fig.canvas.mpl_connect('button_press_event', self.on_resize)  
  
        
    def plot_square_wave(self):
        self.lines = []
        self.baseline_offset = 2
        # 遍历每一列数据，绘制正方形波形
        for idx, (column, data) in enumerate(self.values.items()):
            data = data.values
            vertical_offset = self.baseline_offset * idx    # 计算垂直偏移量，确保每条线在y轴的不同水平线上
            # self.relay_name_txt = self.ax.text(self.timestamps[0], vertical_offset, column, 
            #             verticalalignment='center', horizontalalignment='right', fontsize=12)   # 显示列名
            self.ax.set_xlim(self.timestamps[0], self.timestamps[-1])  # 保证文本显示在绘图区域内
            self.new_time = []  # 创建新的时间列表
            self.new_values = []  # 创建新的值列表

            # 遍历当前列的所有数据点
            for i in range(len(data) - 1): 
                self.new_time.append(self.timestamps[i]) # 添加当前点
                self.new_values.append(data[i] + vertical_offset)  # 加上偏移量

                # 检查是否需要插入额外的点来创建垂直边
                if data[i] != data[i + 1]:
                    self.new_time.append(self.timestamps[i]) # 添加相同的时间戳但值为下一个点的值（加上偏移量）到 new_time 和 new_values 中
                    self.new_values.append(data[i + 1] + vertical_offset)

            self.new_time.append(self.timestamps[-1])    # 添加最后一个点
            self.new_values.append(data[-1] + vertical_offset)

            # 绘制处理后的数据作为正方形波
            line, = self.ax.plot(self.new_time, self.new_values, label=column)
            self.lines.append(line)
        
        
        self.ax.set_yticks([])
        self.ax.set_ylim(-0.1, len(self.values.columns) * self.baseline_offset)  # 设置 y 轴的范围
        self.ax.grid(True) 
        self.ax.set_title(self.title) 
        self.ax.xaxis.set_major_formatter(DateFormatter('%m-%d %H:%M:%S'))
        self.ax.set_xlabel('Time')
        self.new_time.clear()
        self.new_values.clear()
        self.xlim_init = self.ax.get_xlim()
        self.ax.set_yticks( [i * self.baseline_offset for i in range(len(self.relay_list))] )
        self.ax.set_yticklabels(self.relay_list)
        plt.tight_layout()
        # plt.show()

    
    def on_resize(self, event):  
        xlim = event.inaxes.get_xlim()  
        #print(f'缩放后的X轴区间范围: {xlim}') 



class CustomNavigationToolbar(NavigationToolbar):
    def __init__(self, canvas, parent=None):
        super(CustomNavigationToolbar, self).__init__(canvas, parent)
        self._idPress = None
        self._idRelease = None
        self._xypress = None
        self._active = None
        self.zoom_threshold = 0.00001  # Set the threshold distance for zooming
        self.pan_threshold = 0.00001  # Set the threshold distance for zooming

        # Connect to the necessary events
        self._idPress = self.canvas.mpl_connect('button_press_event', self.press)
        self._idRelease = self.canvas.mpl_connect('button_release_event', self.release_zoom_pan)

    def zoom(self, *args):
        super(CustomNavigationToolbar, self).zoom(*args)
        self._active = 'ZOOM'

    def pan(self, *args):
        super(CustomNavigationToolbar, self).pan(*args)
        self._active = 'PAN'

    def press(self, event):
        # Block the zoom out (right-click) functionality by ignoring right-click events
        if event.button == 3:  # Right mouse button
            self._xypress = [(event.xdata, event.ydata, event.inaxes, event.button)]
            self._xypress_orig_xlim = event.inaxes.get_xlim()
            self._xypress_orig_ylim = event.inaxes.get_ylim()
            return
        if event.inaxes is None or event.button != 1:
            return
        self._xypress = [(event.xdata, event.ydata, event.inaxes, event.button)]
        self._xypress_orig_xlim = event.inaxes.get_xlim()
        self._xypress_orig_ylim = event.inaxes.get_ylim()


    def release_zoom_pan(self, event):
        if self._active == 'ZOOM':
            self.zoom_xaxis(event)
        elif self._active == 'PAN':
            self.pan_xaxis(event)

    def zoom_xaxis(self, event):
        if event.button == 3:  # Right mouse button
            self.ax.set_xlim(self._xypress_orig_xlim)
            self.ax.set_ylim(self._xypress_orig_ylim)  # Keep y-axis limits unchanged
            self.canvas.draw_idle()           
            return
        # if self._xypress is None or event.inaxes is None or event.button != 1:
        #     return
        if self._xypress is None or event.button != 1:
            return
        xpress, ypress, self.ax, button = self._xypress[0]
        xrelease = event.xdata
        # xlim = self._xypress_orig_xlim

        # Only proceed if the drag distance exceeds the threshold
        if abs(xrelease - xpress) < self.zoom_threshold:
            self.ax.set_ylim(self._xypress_orig_ylim)  # Keep y-axis limits unchanged
            self.canvas.draw_idle()
            return

        if xrelease < xpress:
            xpress, xrelease = xrelease, xpress

        self.ax.set_xlim(xpress, xrelease)
        self.ax.set_ylim(self._xypress_orig_ylim)  # Keep y-axis limits unchanged
        self.canvas.draw_idle()

    def pan_xaxis(self, event):
        if self._xypress is None or event.inaxes is None or event.button != 1:
            return
        xpress, ypress, self.ax, button = self._xypress[0]
        xrelease = event.xdata
        xlim = self._xypress_orig_xlim

        # Calculate the shift based on initial and release positions
        dx = xrelease - xpress
        # Only proceed if the drag distance exceeds the threshold
        if abs(dx) < self.pan_threshold:
            self.ax.set_ylim(self._xypress_orig_ylim)  # Keep y-axis limits unchanged
            self.canvas.draw_idle()  
            return
        # Set new limits for x-axis
        self.ax.set_xlim(xlim[0] + dx, xlim[1] + dx)
        self.ax.set_ylim(self._xypress_orig_ylim)  # Keep y-axis limits unchanged
        self.canvas.draw_idle()               



class DiplayPlotWindow(QMainWindow):
    def __init__(self, title, values, relay_list):
        super().__init__()
        self.fig = PlotCanvas(title, values, relay_list).fig
        self.initUI()

    def initUI(self):
        self.setWindowTitle("波形显示")
        self.setGeometry(100, 100, 800, 600)
        self.canvas = FigureCanvasQTAgg(self.fig)
        self.toolbar = CustomNavigationToolbar(self.canvas, self)
        layout = QVBoxLayout()
        layout.addWidget(self.toolbar)
        layout.addWidget(self.canvas)
        central_widget = QWidget(self)
        central_widget.setLayout(layout)
        self.setCentralWidget(central_widget)

if __name__ == '__main__':
    pass
