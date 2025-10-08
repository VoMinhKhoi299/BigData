import sys
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QTabWidget, QWidget, QVBoxLayout, QHBoxLayout,
    QTableWidget, QTableWidgetItem, QPushButton, QLabel, QLineEdit, QStatusBar,
    QMenuBar, QAction, QFormLayout
)
from PyQt5.QtGui import QColor, QPalette, QIcon
from PyQt5.QtCore import Qt

class BigDataGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Big Data Project GUI - VNExpress & Nhaccuatui Analysis")
        self.setGeometry(100, 100, 800, 600)
        
        # Main background very pale blue (near white)
        palette = QPalette()
        palette.setColor(QPalette.Window, QColor("#E6F2FF"))
        palette.setColor(QPalette.Base, QColor("#FFFFFF"))  # White tables
        palette.setColor(QPalette.AlternateBase, QColor("#80BFFF"))  # Sky blue alternating
        self.setPalette(palette)
        
        # Stylesheet for professional UI, with bolder tab colors
        self.setStyleSheet("""
            QPushButton {
                border-radius: 15px;
                padding: 8px;
                color: white;
                font-weight: bold;
            }
            QTableWidget {
                background-color: #FFFFFF;
                alternate-background-color: #80BFFF;
            }
            QTableWidget::item {
                background-color: #FFFFFF;
            }
            QHeaderView::section {
                background-color: #4CA3FF;  # Medium light blue
                color: #001F3F;  # Dark navy text
                padding: 5px;
                border: 1px solid #B3D9FF;
            }
            QLabel#chart_placeholder {
                background-color: #B3D9FF;  # Pale blue
                border: 1px solid #4CA3FF;
                border-radius: 10px;
                padding: 20px;
            }
            QTabBar::tab {
                background-color: #004D99;  # Bolder blue for tabs to stand out
                color: white;
                border-radius: 10px;
                padding: 10px 20px;  # Increased padding for wider tabs
                min-width: 150px;  # Minimum width to expand tabs, especially the 3rd one
            }
            QTabBar::tab:selected {
                background-color: #0066CC;  # Vivid blue for selected tab
            }
            QTabWidget::pane {
                background-color: #E6F2FF;
            }
            QStatusBar {
                background-color: #B3D9FF;  # Pale blue to make status bar more visible
                color: #001F3F;  # Dark text for readability
            }
        """)
        
        # Menu bar - Force non-native on macOS to show inside window
        menubar = self.menuBar()
        menubar.setNativeMenuBar(False)  # Important: Disable native menu bar on macOS
        file_menu = menubar.addMenu("File")
        file_menu.addAction(QAction("Open", self))
        file_menu.addAction(QAction("Exit", self, triggered=self.close))
        help_menu = menubar.addMenu("Help")
        help_menu.addAction(QAction("About", self))
        
        # Central widget: Tabs
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)
        
        # Tab 1: CRUD VNExpress News
        vnexpress_tab = QWidget()
        vnexpress_layout = QHBoxLayout()
        # Table for data display
        self.vnexpress_table = QTableWidget(0, 4)  # 4 columns: Title, Date, Category, Views
        self.vnexpress_table.setHorizontalHeaderLabels(["Title", "Date", "Category", "Views"])
        self.vnexpress_table.setAlternatingRowColors(True)
        vnexpress_layout.addWidget(self.vnexpress_table, stretch=3)
        
        # Right panel: CRUD controls
        vnexpress_controls = QVBoxLayout()
        form_layout = QFormLayout()
        self.title_edit = QLineEdit(placeholderText="Title")
        self.date_edit = QLineEdit(placeholderText="Date (YYYY-MM-DD)")
        self.category_edit = QLineEdit(placeholderText="Category")
        self.views_edit = QLineEdit(placeholderText="Views")
        form_layout.addRow("Title:", self.title_edit)
        form_layout.addRow("Date:", self.date_edit)
        form_layout.addRow("Category:", self.category_edit)
        form_layout.addRow("Views:", self.views_edit)
        vnexpress_controls.addLayout(form_layout)
        
        add_btn = QPushButton("Add", self)
        add_btn.setStyleSheet("background-color: #0066CC;")  # Vivid blue
        edit_btn = QPushButton("Edit", self)
        edit_btn.setStyleSheet("background-color: #004D99;")  # Strong blue
        delete_btn = QPushButton("Delete", self)
        delete_btn.setStyleSheet("background-color: #FF4081;")  # Pink accent
        refresh_btn = QPushButton("Refresh", self)
        refresh_btn.setStyleSheet("background-color: #0066CC;")  # Vivid blue
        
        vnexpress_controls.addWidget(add_btn)
        vnexpress_controls.addWidget(edit_btn)
        vnexpress_controls.addWidget(delete_btn)
        vnexpress_controls.addWidget(refresh_btn)
        vnexpress_controls.addStretch()
        
        vnexpress_layout.addLayout(vnexpress_controls, stretch=1)
        vnexpress_tab.setLayout(vnexpress_layout)
        self.tabs.addTab(vnexpress_tab, "VNExpress News CRUD")
        
        # Tab 2: CRUD Nhaccuatui Top Charts
        nhaccuatui_tab = QWidget()
        nhaccuatui_layout = QHBoxLayout()
        # Table for data display
        self.nhaccuatui_table = QTableWidget(0, 4)  # 4 columns: Song Title, Artist, Rank, Streams
        self.nhaccuatui_table.setHorizontalHeaderLabels(["Song Title", "Artist", "Rank", "Streams"])
        self.nhaccuatui_table.setAlternatingRowColors(True)
        nhaccuatui_layout.addWidget(self.nhaccuatui_table, stretch=3)
        
        # Right panel: CRUD controls (similar to VNExpress)
        nhaccuatui_controls = QVBoxLayout()
        form_layout2 = QFormLayout()
        self.song_edit = QLineEdit(placeholderText="Song Title")
        self.artist_edit = QLineEdit(placeholderText="Artist")
        self.rank_edit = QLineEdit(placeholderText="Rank")
        self.streams_edit = QLineEdit(placeholderText="Streams")
        form_layout2.addRow("Song Title:", self.song_edit)
        form_layout2.addRow("Artist:", self.artist_edit)
        form_layout2.addRow("Rank:", self.rank_edit)
        form_layout2.addRow("Streams:", self.streams_edit)
        nhaccuatui_controls.addLayout(form_layout2)
        
        add_btn2 = QPushButton("Add", self)
        add_btn2.setStyleSheet("background-color: #0066CC;")
        edit_btn2 = QPushButton("Edit", self)
        edit_btn2.setStyleSheet("background-color: #004D99;")
        delete_btn2 = QPushButton("Delete", self)
        delete_btn2.setStyleSheet("background-color: #FF4081;")
        refresh_btn2 = QPushButton("Refresh", self)
        refresh_btn2.setStyleSheet("background-color: #0066CC;")
        
        nhaccuatui_controls.addWidget(add_btn2)
        nhaccuatui_controls.addWidget(edit_btn2)
        nhaccuatui_controls.addWidget(delete_btn2)
        nhaccuatui_controls.addWidget(refresh_btn2)
        nhaccuatui_controls.addStretch()
        
        nhaccuatui_layout.addLayout(nhaccuatui_controls, stretch=1)
        nhaccuatui_tab.setLayout(nhaccuatui_layout)
        self.tabs.addTab(nhaccuatui_tab, "Nhaccuatui Top Charts CRUD")
        
        # Tab 3: Analysis & Charts
        analysis_tab = QWidget()
        analysis_layout = QVBoxLayout()
        run_mapreduce_btn = QPushButton("Chạy lại MapReduce", self)
        run_mapreduce_btn.setStyleSheet("background-color: #FF4081; font-size: 16px;")
        analysis_layout.addWidget(run_mapreduce_btn, alignment=Qt.AlignCenter)
        
        # Placeholder for charts
        chart_label = QLabel("Biểu đồ phân tích sẽ hiển thị ở đây (e.g., Xu hướng tin tức, Top Charts stats)")
        chart_label.setObjectName("chart_placeholder")
        chart_label.setAlignment(Qt.AlignCenter)
        analysis_layout.addWidget(chart_label, stretch=1)
        
        status_label = QLabel("Status: Sẵn sàng")
        analysis_layout.addWidget(status_label, alignment=Qt.AlignCenter)
        
        analysis_tab.setLayout(analysis_layout)
        self.tabs.addTab(analysis_tab, "Analysis & Charts")
        
        # Status bar - Make more visible
        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        self.statusBar.showMessage("Welcome to Big Data GUI")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = BigDataGUI()
    window.show()
    sys.exit(app.exec_())