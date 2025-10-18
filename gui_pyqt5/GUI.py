import sys
import math
import uuid
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QTabWidget, QWidget, QVBoxLayout, QHBoxLayout,
    QTableWidget, QTableWidgetItem, QPushButton, QLabel, QStatusBar,
    QAction, QFormLayout, QSplitter, QGroupBox, QSizePolicy,
    QStyle, QMessageBox, QLineEdit, QHeaderView, QGraphicsDropShadowEffect,
    QPlainTextEdit, QCompleter, QDialog, QDialogButtonBox
)
from PyQt5.QtGui import QColor, QPalette, QFont
from PyQt5.QtCore import Qt, QTimer, QObject, QEvent, QStringListModel

# ---------------- Custom QPlainTextEdit with Autocomplete ----------------
class SqlTextEdit(QPlainTextEdit):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.completer = None
        self.setFont(QFont("Courier New", 13))

    def setCompleter(self, completer):
        if self.completer:
            self.disconnect(self.completer)
        self.completer = completer
        if not self.completer:
            return
        self.completer.setWidget(self)
        self.completer.setCompletionMode(QCompleter.PopupCompletion)
        self.completer.setCaseSensitivity(Qt.CaseInsensitive)
        self.completer.activated.connect(self.insertCompletion)

    def insertCompletion(self, completion):
        if self.completer.widget() != self:
            return
        tc = self.textCursor()
        extra = len(completion) - len(self.completer.completionPrefix())
        tc.movePosition(tc.Left)
        tc.movePosition(tc.EndOfWord)
        tc.insertText(completion[-extra:])
        self.setTextCursor(tc)

    def textUnderCursor(self):
        tc = self.textCursor()
        tc.select(tc.WordUnderCursor)
        return tc.selectedText()

    def keyPressEvent(self, event):
        if self.completer and self.completer.popup().isVisible():
            if event.key() in (Qt.Key_Enter, Qt.Key_Return, Qt.Key_Escape, Qt.Key_Tab):
                event.ignore()
                return
        super().keyPressEvent(event)
        if not self.completer:
            return
        completion_prefix = self.textUnderCursor()
        if len(completion_prefix) < 1:
            self.completer.popup().hide()
            return
        if completion_prefix != self.completer.completionPrefix():
            self.completer.setCompletionPrefix(completion_prefix)
            self.completer.popup().setCurrentIndex(
                self.completer.completionModel().index(0, 0)
            )
        cr = self.cursorRect()
        cr.setWidth(
            self.completer.popup().sizeHintForColumn(0)
            + self.completer.popup().verticalScrollBar().sizeHint().width()
        )
        self.completer.complete(cr)

# ---------------- Animation viền gradient cho QLineEdit ----------------
class GradientLineEditAnimator:
    def __init__(self, parent=None):
        self._active = set()
        self._phase = {}
        self._timer = QTimer(parent)
        self._timer.setInterval(55)
        self._timer.timeout.connect(self._tick)
        self._t = 0.0

    def add(self, w: QLineEdit):
        if w not in self._active:
            self._active.add(w)
            self._phase[w] = (len(self._phase) * 0.37) % 1.0
        if not self._timer.isActive():
            self._timer.start()

    def remove(self, w: QLineEdit):
        if w in self._active:
            self._active.remove(w)
            self._phase.pop(w, None)
            w.setStyleSheet("")
        if not self._active and self._timer.isActive():
            self._timer.stop()

    def _tick(self):
        self._t += 0.06
        for w in list(self._active):
            ph = self._phase.get(w, 0.0)
            angle = (self._t * 80.0 + ph * 360.0) % 360.0
            c1 = "#4CA6FF"
            c2 = "#FFFFFF"
            c3 = "#A8D8FF"
            grad = (
                f"qconicalgradient(cx:0.5, cy:0.5, angle:{angle:.1f}, "
                f"stop:0.00 {c1}, stop:0.45 {c2}, stop:0.90 {c3}, stop:1.00 {c1})"
            )
            inline = (
                "QLineEdit {"
                f"border: 2px solid {grad};"
                "border-radius: 10px;"
                "padding: 6px 10px;"
                "background: #FFFFFF;"
                "color: #0B2545;"
                "}"
            )
            w.setStyleSheet(inline)


# ---------------- Event filter cho QLineEdit (hover, focus) ----------------
class LineEditFilter(QObject):
    def __init__(self, animator: GradientLineEditAnimator, parent=None):
        super().__init__(parent)
        self.anim = animator

    def eventFilter(self, obj, event):
        if isinstance(obj, QLineEdit):
            if event.type() == QEvent.Enter:
                self.anim.add(obj)
                return False
            elif event.type() == QEvent.Leave:
                if not obj.hasFocus():
                    self.anim.remove(obj)
                    obj.setStyleSheet("")
                return False
            elif event.type() == QEvent.FocusIn:
                self.anim.add(obj)
                return False
            elif event.type() == QEvent.FocusOut:
                if not obj.underMouse():
                    self.anim.remove(obj)
                    obj.setStyleSheet("")
                return False
        return super().eventFilter(obj, event)


# ---------------- Dialog lọc (dùng chung) ----------------
class FilterDialog(QDialog):
    def __init__(self, parent=None, include_rank=False):
        super().__init__(parent)
        self.setWindowTitle("Lọc dữ liệu")
        self.setModal(True)
        self.include_rank = include_rank

        layout = QVBoxLayout()
        form = QFormLayout()

        self.month_input = QLineEdit()
        self.month_input.setPlaceholderText("MM (ví dụ: 01) - để trống nếu không lọc theo tháng")
        self.year_input = QLineEdit()
        self.year_input.setPlaceholderText("YYYY (ví dụ: 2025) - để trống nếu không lọc theo năm")
        form.addRow("Tháng:", self.month_input)
        form.addRow("Năm:", self.year_input)

        if include_rank:
            self.rank_input = QLineEdit()
            self.rank_input.setPlaceholderText("Rank hoặc khoảng (ví dụ: 1 hoặc 1-10). Để trống nếu không lọc theo hạng")
            form.addRow("Hạng:", self.rank_input)
        else:
            self.rank_input = None

        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self.setLayout(layout)

    def get_values(self):
        m = self.month_input.text().strip()
        y = self.year_input.text().strip()
        r = self.rank_input.text().strip() if self.rank_input is not None else ""
        return m, y, r


# ---------------- Giao diện chính ----------------
class BigDataGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BigData Project GUI — VNExpress & Nhaccuatui")
        self.setGeometry(120, 80, 1100, 720)

        # Palette & font
        palette = QPalette()
        palette.setColor(QPalette.Window, QColor("#FAFBFD"))
        palette.setColor(QPalette.Base, QColor("#FFFFFF"))
        palette.setColor(QPalette.AlternateBase, QColor("#F9FBFF"))
        palette.setColor(QPalette.WindowText, QColor("#0B2545"))
        self.setPalette(palette)
        self.setFont(QFont("SF Pro Display", 10, QFont.Normal))

        # Stylesheet cơ bản (chứa token __TAB_ALPHA__ và __TAB_BORDER__ để cập nhật động)
        self._base_stylesheet = r"""
            QMainWindow { background: #FAFBFD; }

            /* --- Tabs --- */
            QTabWidget::pane {
                border: none;
                background: transparent;
                top: -1px;
            }
            QTabBar::tab {
                background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                    stop:0 #FAFCFF, stop:1 #F0F5FA);
                color: #0B2545;
                padding: 10px 18px;
                border-top-left-radius: 10px;
                border-top-right-radius: 10px;
                min-width: 130px;
                margin-right: 6px;
                border: 1px solid #E5EFF9;
                font-weight: 500;
                transition: all 0.14s ease;
            }
            QTabBar::tab:hover {
                background: #F6FAFF;
                transform: translateY(-3px);
            }
            QTabBar::tab:selected {
                background: qlineargradient(x1:0,y1:0,x2:0,y2:1,
                    stop:0 #FFFFFF, stop:1 #E9F2FF);
                border: __TAB_BORDER__;
                color: #0055AA;
                font-weight: 600;
                box-shadow: 0px 6px 18px rgba(0,85,170, __TAB_ALPHA__);
            }

            /* --- Group card --- */
            QGroupBox {
                background: #FFFFFF;
                border: none;
                border-radius: 14px;
                margin-top: 8px;
                padding: 14px;
            }
            QGroupBox:title {
                subcontrol-origin: margin;
                subcontrol-position: top left;
                padding: 0 6px;
                color: #0055AA;
                font-weight: 600;
            }

            /* --- Input (fallback styles; animator will override inline when active) --- */
            QLineEdit {
                border: 1px solid #E1E9F6;
                border-radius: 10px;
                padding: 6px 10px;
                min-height: 30px;
                background: #FFFFFF;
                color: #0B2545;
                transition: all 0.18s ease;
            }
            QLineEdit:hover {
                border: 1px solid #B8D8FF;
            }
            QLineEdit:focus {
                border: 2px solid #4CA6FF;
                background: #F9FCFF;
                box-shadow: 0 0 6px rgba(80,150,255,0.4);
            }

            /* --- Table --- */
            QTableWidget {
                background-color: #FFFFFF;
                gridline-color: #EEF4FA;
                selection-background-color: #DCEEFF;
                selection-color: #032B50;
                font-size: 13px;
                border-radius: 12px;
                border: 1px solid #E8F1FA;
            }
            QTableWidget::item { padding: 8px 6px; }
            QTableWidget::item:hover { background: #F7FBFF; }
            QHeaderView::section {
                background-color: #F8FAFD;
                color: #08406A;
                padding: 8px;
                border: 1px solid #EEF5FF;
                font-weight: 600;
                border-top-left-radius: 6px;
                border-top-right-radius: 6px;
            }

            /* --- Buttons --- */
            QPushButton {
                border-radius: 10px;
                padding: 8px 12px;
                font-weight: 600;
                margin: 0px;
                transition: all 0.14s ease-in-out;
            }
            QPushButton.primary {
                background: qlineargradient(x1:0,y1:0,x2:0,y2:1,
                    stop:0 #4CA6FF, stop:1 #1D7EED);
                color: white;
                border: none;
                letter-spacing: 0.2px;
            }
            QPushButton.primary:hover {
                background: qlineargradient(x1:0,y1:0,x2:0,y2:1,
                    stop:0 #66B8FF, stop:1 #3A8FEF);
                transform: translateY(-2px);
            }
            QPushButton.primary:pressed {
                background: #1B6AC8;
                transform: translateY(0);
            }

            QPushButton.ghost {
                background: #FFFFFF;
                border: 1px solid #E1EAF5;
                color: #0B2545;
            }
            QPushButton.ghost:hover {
                background: #F5FAFF;
                border-color: #CDE4FF;
                transform: translateY(-2px);
            }
            QPushButton.ghost:pressed {
                background: #EDF7FF;
            }
            QPushButton[text="Xoá"] {
                background: #FFECEC;
                color: #C44747;
                border: 1px solid #F2CACA;
            }
            QPushButton[text="Xoá"]:hover {
                background: #FFD7D7;
                border: 1px solid #EFAAAA;
                transform: translateY(-2px);
            }
            QPushButton[text="Xoá"]:pressed {
                background: #FFBABA;
            }

            QLabel#chart_placeholder {
                background: #FFFFFF;
                border: 1px dashed #E6F2FF;
                border-radius: 14px;
                padding: 20px;
                color: #0B2545;
                font-weight: 500;
            }

            QStatusBar {
                background: #F7FAFD;
                color: #0B2545;
                border-top: 1px solid #EAF3FF;
                font-weight: 500;
            }

            /* --- QPlainTextEdit for SQL Query --- */
            QPlainTextEdit {
                border: 1px solid #E1E9F6;
                border-radius: 10px;
                padding: 8px;
                background: #FFFFFF;
                color: #0B2545;
                font-family: 'Courier New', monospace;
                font-size: 13px;
            }
            QPlainTextEdit:hover {
                border: 1px solid #B8D8FF;
            }
            QPlainTextEdit:focus {
                border: 2px solid #4CA6FF;
                background: #F9FCFF;
                box-shadow: 0 0 6px rgba(80,150,255,0.4);
            }
        """

        # initial values for dynamic tokens
        self._current_tab_alpha = 6
        # animation control for tab border gradient
        self._tab_animating = False
        self._tab_anim_t = 0.0
        self._tab_anim_timer = QTimer(self)
        self._tab_anim_timer.setInterval(55)
        self._tab_anim_timer.timeout.connect(self._advance_tab_anim)
        self._apply_base_stylesheet()

        # --- Menu ---
        menubar = self.menuBar()
        menubar.setNativeMenuBar(False)
        menubar.setStyleSheet("""
            QMenuBar {
                background-color: #FAFBFD;
                color: #0B2545;
                font-weight: 500;
            }
            QMenuBar::item:selected { background: #E9F3FF; border-radius: 6px; }
        """)
        file_menu = menubar.addMenu("File")
        file_menu.addAction(QAction(self.style().standardIcon(QStyle.SP_DialogOpenButton), "Open", self))
        file_menu.addAction(QAction("Exit", self, triggered=self.close))
        help_menu = menubar.addMenu("Help")
        help_menu.addAction(QAction("About", self, triggered=self.show_about))

        # --- Tabs ---
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)
        self.tabs.setDocumentMode(True)

        # animator + filter
        self._le_animator = GradientLineEditAnimator(self)
        self._le_filter = LineEditFilter(self._le_animator, self)

        # build UI
        self._create_vnexpress_tab()
        self._create_nhaccuatui_tab()
        # Note: analysis tab removed per request
        self._create_direct_query_tab()

        # --- Status bar ---
        self.status = QStatusBar()
        self.setStatusBar(self.status)
        self.status.showMessage("Sẵn sàng")

        # Tab pulsing timer
        self._tab_pulse_t = 0.0
        self._tab_timer = QTimer(self)
        self._tab_timer.setInterval(90)
        self._tab_timer.timeout.connect(self._update_tab_pulse)
        self._tab_timer.start()

        # Install event filter on the tabBar to detect clicks and start gradient animation
        self.tabs.tabBar().installEventFilter(self)

    # ---------------- Hiệu ứng & stylesheet động ----------------
    def _apply_base_stylesheet(self):
        alpha_val = max(0.02, min(0.16, self._current_tab_alpha / 100.0))
        # choose border depending on whether gradient animation is running
        if getattr(self, "_tab_animating", False):
            # compute current angle and create conical gradient for border
            angle = (self._tab_anim_t * 80.0) % 360.0
            c1 = "#4CA6FF"
            c2 = "#FFFFFF"
            c3 = "#A8D8FF"
            grad = f"qconicalgradient(cx:0.5, cy:0.5, angle:{angle:.1f}, stop:0.00 {c1}, stop:0.45 {c2}, stop:0.90 {c3}, stop:1.00 {c1})"
            border_val = f"2px solid {grad}"
        else:
            border_val = "1px solid #D6E8FF"
        css = self._base_stylesheet.replace("__TAB_ALPHA__", f"{alpha_val:.3f}").replace("__TAB_BORDER__", border_val)
        self.setStyleSheet(css)

    def _update_tab_pulse(self):
        self._tab_pulse_t += 0.12
        a = 0.03 + 0.09 * (0.5 * (1 + math.sin(self._tab_pulse_t)))
        self._current_tab_alpha = a
        self._apply_base_stylesheet()
        if self._le_animator._timer.isActive():
            self._le_animator._tick()

    def _advance_tab_anim(self):
        self._tab_anim_t += 0.08
        self._apply_base_stylesheet()

    # catch clicks on tabBar to enable rotating gradient border for a short duration
    def eventFilter(self, obj, event):
        # intercept tabBar clicks only
        if obj == self.tabs.tabBar():
            if event.type() == QEvent.MouseButtonPress:
                # start gradient animation (running for a short duration)
                self._tab_animating = True
                self._tab_anim_t = 0.0
                self._tab_anim_timer.start()
                self._apply_base_stylesheet()
                # stop animation after 900ms
                QTimer.singleShot(900, self._stop_tab_anim)
                return False
        return super().eventFilter(obj, event)

    def _stop_tab_anim(self):
        self._tab_animating = False
        self._tab_anim_timer.stop()
        self._apply_base_stylesheet()

    # ---------------- Tabs (VNExpress modified to match `articles` table) ----------------
    def _create_vnexpress_tab(self):
        tab = QWidget()
        root_layout = QHBoxLayout()
        root_layout.setContentsMargins(12, 12, 12, 12)
        splitter = QSplitter(Qt.Horizontal)

        left_card = QGroupBox()
        left_layout = QVBoxLayout()
        left_layout.setContentsMargins(8, 8, 8, 8)

        # columns aligned to your `articles` table schema (now Vietnamese)
        self.vnexpress_table = QTableWidget(0, 6)
        self.vnexpress_table.setHorizontalHeaderLabels(["ID", "Tiêu đề", "Ngày xuất bản", "Thể loại", "Tóm tắt", "Nguồn"])
        # <-- adjusted header sizing so headers don't get cut
        hdr = self.vnexpress_table.horizontalHeader()
        hdr.setSectionResizeMode(QHeaderView.Interactive)
        hdr.setDefaultSectionSize(180)
        hdr.setMinimumSectionSize(100)
        self.vnexpress_table.horizontalHeader().setStretchLastSection(False)

        self.vnexpress_table.setAlternatingRowColors(True)
        self.vnexpress_table.setSelectionBehavior(QTableWidget.SelectRows)

        left_layout.addWidget(self.vnexpress_table)
        left_card.setLayout(left_layout)

        controls_card = QGroupBox("Chức năng CRUD")
        controls_layout = QVBoxLayout()
        controls_layout.setContentsMargins(12, 12, 12, 12)
        controls_layout.setSpacing(10)
        controls_layout.setAlignment(Qt.AlignTop)

        # form matching articles table fields (labels now Vietnamese)
        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignRight)
        form.setHorizontalSpacing(12)
        form.setVerticalSpacing(6)

        # ID readOnly (auto-generated)
        self.article_id_input = QLineEdit(placeholderText="(auto) id")
        self.article_id_input.setReadOnly(True)

        self.article_title_input = QLineEdit(placeholderText="tiêu đề")
        self.article_published_input = QLineEdit(placeholderText="YYYY-MM-DD HH:MM:SS (hoặc để trống)")
        self.article_category_input = QLineEdit(placeholderText="thể loại")
        self.article_summary_input = QLineEdit(placeholderText="tóm tắt (ngắn)")
        self.article_source_input = QLineEdit(placeholderText="nguồn")

        for inp in (
            self.article_id_input, self.article_title_input, self.article_published_input,
            self.article_category_input, self.article_summary_input, self.article_source_input
        ):
            inp.setMinimumHeight(30)
            inp.setFixedWidth(240)
            inp.installEventFilter(self._le_filter)

        form.addRow("ID:", self.article_id_input)
        form.addRow("Tiêu đề:", self.article_title_input)
        form.addRow("Ngày xuất bản:", self.article_published_input)
        form.addRow("Thể loại:", self.article_category_input)
        form.addRow("Tóm tắt:", self.article_summary_input)
        form.addRow("Nguồn:", self.article_source_input)
        controls_layout.addLayout(form)

        btn_row = QHBoxLayout()
        btn_row.setSpacing(10)

        def make_btn(text, css, handler):
            b = QPushButton(text)
            b.setProperty("class", css)
            b.setMinimumHeight(38)
            b.clicked.connect(handler)
            if text == "Xoá":
                b.setProperty("text", "Xoá")
            return b

        btn_row.addWidget(make_btn("Thêm", "primary", self.add_vnexpress))
        btn_row.addWidget(make_btn("Sửa", "ghost", self.edit_vnexpress))
        btn_row.addWidget(make_btn("Xoá", "ghost", self.delete_vnexpress))
        btn_row.addWidget(make_btn("Lọc", "ghost", self.filter_vnexpress))  # Nút Lọc
        btn_row.addWidget(make_btn("Mới", "ghost", self.refresh_vnexpress))
        controls_layout.addLayout(btn_row)

        controls_card.setLayout(controls_layout)
        controls_card.setMaximumWidth(340)
        shadow = QGraphicsDropShadowEffect()
        shadow.setBlurRadius(24)
        shadow.setOffset(0, 6)
        shadow.setColor(QColor(0, 40, 90, 25))
        controls_card.setGraphicsEffect(shadow)

        splitter.addWidget(left_card)
        splitter.addWidget(controls_card)
        splitter.setStretchFactor(0, 2)
        splitter.setStretchFactor(1, 1)
        root_layout.addWidget(splitter)
        tab.setLayout(root_layout)
        self.tabs.addTab(tab, "Tin tức")

    # ---------------- Tabs (Nhaccuatui modified to match `spotify_viral_chart`) ----------------
    def _create_nhaccuatui_tab(self):
        tab = QWidget()
        root_layout = QHBoxLayout()
        root_layout.setContentsMargins(12, 12, 12, 12)
        splitter = QSplitter(Qt.Horizontal)

        left_card = QGroupBox()
        left_layout = QVBoxLayout()
        left_layout.setContentsMargins(8, 8, 8, 8)

        # columns aligned to your spotify_viral_chart table schema (now Vietnamese)
        headers = [
            "ID", "Ngày", "Vùng", "Loại BXH", "Hạng", "Hạng trước",
            "Chênh lệch", "Di chuyển", "Tên bài", "Ca sĩ", "Ngày phát hành"
        ]
        self.nhaccuatui_table = QTableWidget(0, len(headers))
        self.nhaccuatui_table.setHorizontalHeaderLabels(headers)
        # <-- adjusted header sizing so headers don't get cut
        hdr = self.nhaccuatui_table.horizontalHeader()
        hdr.setSectionResizeMode(QHeaderView.Interactive)
        hdr.setDefaultSectionSize(160)
        hdr.setMinimumSectionSize(90)
        self.nhaccuatui_table.horizontalHeader().setStretchLastSection(False)

        self.nhaccuatui_table.setAlternatingRowColors(True)
        self.nhaccuatui_table.setSelectionBehavior(QTableWidget.SelectRows)

        left_layout.addWidget(self.nhaccuatui_table)
        left_card.setLayout(left_layout)

        controls_card = QGroupBox("Chức năng CRUD")
        controls_layout = QVBoxLayout()
        controls_layout.setContentsMargins(12, 12, 12, 12)
        controls_layout.setSpacing(10)
        controls_layout.setAlignment(Qt.AlignTop)

        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignRight)
        form.setHorizontalSpacing(12)
        form.setVerticalSpacing(6)

        # ID readOnly
        self.track_id_input = QLineEdit(placeholderText="(auto) id")
        self.track_id_input.setReadOnly(True)

        self.track_date_input = QLineEdit(placeholderText="YYYY-MM-DD")
        self.track_region_input = QLineEdit(placeholderText="vùng")
        self.track_chart_type_input = QLineEdit(placeholderText="loại_bxh")
        self.track_rank_input = QLineEdit(placeholderText="hạng")
        self.track_prev_rank_input = QLineEdit(placeholderText="hạng_trước")
        self.track_rank_delta_input = QLineEdit(placeholderText="chênh_lệch")
        self.track_movement_input = QLineEdit(placeholderText="di chuyển (Lên/Xuống/Không)")
        self.track_name_input = QLineEdit(placeholderText="tên bài")
        self.track_artists_input = QLineEdit(placeholderText="ca sĩ (ngăn bằng dấu phẩy)")
        self.track_release_input = QLineEdit(placeholderText="ngày_phát_hành YYYY-MM-DD")

        for inp in (
            self.track_id_input, self.track_date_input, self.track_region_input, self.track_chart_type_input,
            self.track_rank_input, self.track_prev_rank_input, self.track_rank_delta_input,
            self.track_movement_input, self.track_name_input, self.track_artists_input, self.track_release_input
        ):
            inp.setMinimumHeight(30)
            inp.setFixedWidth(240)
            inp.installEventFilter(self._le_filter)

        form.addRow("ID:", self.track_id_input)
        form.addRow("Ngày:", self.track_date_input)
        form.addRow("Vùng:", self.track_region_input)
        form.addRow("Loại BXH:", self.track_chart_type_input)
        form.addRow("Hạng:", self.track_rank_input)
        form.addRow("Hạng trước:", self.track_prev_rank_input)
        form.addRow("Chênh lệch:", self.track_rank_delta_input)
        form.addRow("Di chuyển:", self.track_movement_input)
        form.addRow("Tên bài:", self.track_name_input)
        form.addRow("Ca sĩ:", self.track_artists_input)
        form.addRow("Ngày phát hành:", self.track_release_input)
        controls_layout.addLayout(form)

        btn_row = QHBoxLayout()
        btn_row.setSpacing(10)
        for text, func, css in [
            ("Thêm", self.add_song, "primary"),
            ("Sửa", self.edit_song, "ghost"),
            ("Xoá", self.delete_song, "ghost"),
            ("Lọc", self.filter_song, "ghost"),  # Nút Lọc
            ("Mới", self.refresh_song, "ghost"),
        ]:
            btn = QPushButton(text)
            btn.setProperty("class", css)
            btn.setMinimumHeight(38)
            if text == "Xoá":
                btn.setProperty("text", "Xoá")
            btn.clicked.connect(func)
            btn_row.addWidget(btn)

        controls_layout.addLayout(btn_row)
        controls_card.setLayout(controls_layout)
        controls_card.setMaximumWidth(340)
        shadow = QGraphicsDropShadowEffect()
        shadow.setBlurRadius(24)
        shadow.setOffset(0, 6)
        shadow.setColor(QColor(0, 40, 90, 25))
        controls_card.setGraphicsEffect(shadow)

        splitter.addWidget(left_card)
        splitter.addWidget(controls_card)
        splitter.setStretchFactor(0, 2)
        splitter.setStretchFactor(1, 1)
        root_layout.addWidget(splitter)
        tab.setLayout(root_layout)
        self.tabs.addTab(tab, "BXH Spotify")

    # ---------------- Tabs (Direct Query) ----------------
    def _create_direct_query_tab(self):
        tab = QWidget()
        layout = QVBoxLayout()
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        # SQL Input (SqlTextEdit for autocomplete)
        self.sql_input = SqlTextEdit()
        self.sql_input.setPlaceholderText("Nhập truy vấn SQL (ví dụ: SELECT * FROM articles LIMIT 10)")
        self.sql_input.setMinimumHeight(150)

        # Autocomplete for SQL
        sql_keywords = [
            "SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
            "GROUP BY", "ORDER BY", "LIMIT", "OFFSET", "AND", "OR", "NOT",
            "INSERT", "UPDATE", "DELETE", "INTO", "VALUES", "SET",
            "articles", "spotify_viral_chart",  # Table names
            "id", "title", "published_date", "category", "summary", "source",  # Columns for articles
            "date", "region", "chart_type", "rank", "previous_rank", "rank_delta",
            "movement", "track_name", "artists", "release_date"  # Columns for spotify_viral_chart
        ]
        completer = QCompleter(sql_keywords, self)
        completer.setCaseSensitivity(Qt.CaseInsensitive)
        self.sql_input.setCompleter(completer)

        # Submit Button
        submit_btn = QPushButton("Submit")
        submit_btn.setProperty("class", "primary")
        submit_btn.setMaximumWidth(200)
        submit_btn.setMinimumHeight(38)
        submit_btn.clicked.connect(self.execute_sql_query)

        # Result Table
        self.sql_result_table = QTableWidget(0, 0)
        self.sql_result_table.setAlternatingRowColors(True)
        self.sql_result_table.setSelectionBehavior(QTableWidget.SelectRows)
        hdr = self.sql_result_table.horizontalHeader()
        hdr.setSectionResizeMode(QHeaderView.Interactive)
        hdr.setDefaultSectionSize(180)
        hdr.setMinimumSectionSize(80)
        self.sql_result_table.horizontalHeader().setStretchLastSection(False)

        # Layout
        layout.addWidget(QLabel("Nhập truy vấn SQL:"))
        layout.addWidget(self.sql_input)
        layout.addWidget(submit_btn, alignment=Qt.AlignLeft)
        layout.addWidget(QLabel("Kết quả truy vấn:"))
        layout.addWidget(self.sql_result_table)
        layout.addStretch()

        tab.setLayout(layout)
        self.tabs.addTab(tab, "Direct Query")

    # ---------------- Execute SQL Query ----------------
    def execute_sql_query(self):
        query = self.sql_input.toPlainText().strip()
        if not query:
            QMessageBox.warning(self, "Lỗi", "Vui lòng nhập truy vấn SQL.")
            return

        try:
            from db import connect_db
        except Exception as e:
            QMessageBox.warning(self, "Lỗi kết nối", f"Không thể import db.connect_db(): {e}")
            return

        try:
            conn = connect_db()
            # use dictionary cursor when possible
            try:
                cursor = conn.cursor(dictionary=True)
            except Exception:
                # fallback
                cursor = conn.cursor()
        except Exception as e:
            QMessageBox.warning(self, "Lỗi kết nối DB", f"Không thể kết nối tới MySQL: {e}")
            return

        try:
            cursor.execute(query)
            # fetch results (may be empty list)
            try:
                rows = cursor.fetchall()
            except Exception:
                # some drivers raise on fetchall for non-select; treat as no rows
                rows = []

            # determine columns robustly
            cols = []
            # prefer cursor.description
            if getattr(cursor, "description", None):
                try:
                    cols = [d[0] for d in cursor.description if d and d[0]]
                except Exception:
                    cols = []
            # fallback to column_names attribute (MySQL connector exposes this)
            if not cols and getattr(cursor, "column_names", None):
                try:
                    cols = list(cursor.column_names)
                except Exception:
                    cols = []
            # if still no cols but rows exist and rows are dicts, extract keys (merge keys across rows, preserve order first-seen)
            if not cols and rows:
                first = rows[0]
                if isinstance(first, dict):
                    seen = []
                    for r in rows:
                        if isinstance(r, dict):
                            for k in r.keys():
                                if k not in seen:
                                    seen.append(k)
                    cols = seen
                elif isinstance(first, (list, tuple)):
                    cols = [f"col{i+1}" for i in range(len(first))]

            # if no cols and no rows -> likely a non-SELECT (INSERT/UPDATE/DELETE)
            if not cols and not rows:
                affected = getattr(cursor, "rowcount", None)
                self.sql_result_table.setRowCount(0)
                self.sql_result_table.setColumnCount(0)
                self.status.showMessage(f"Truy vấn thực thi — {affected if affected is not None else 0} hàng bị ảnh hưởng.")
                return

            # populate table (this handles dict rows or tuple rows)
            self._populate_table_from_rows(self.sql_result_table, cols, rows)
            self.status.showMessage(f"Truy vấn thành công: {len(rows)} hàng được trả về.")
        except Exception as e:
            QMessageBox.warning(self, "Lỗi truy vấn", f"Truy vấn thất bại: {e}")
        finally:
            try:
                cursor.close()
                conn.close()
            except Exception:
                pass

    # ---------------- Populate Table with Query Results ----------------
    def _populate_table_from_rows(self, tw: QTableWidget, cols, rows):
        """
        Dùng để điền dữ liệu vào QTableWidget:
         - cols: iterable tên cột (có thể rỗng)
         - rows: list of dict hoặc list of tuples
        """
        # robust column list
        col_list = list(cols) if cols else []
        if not col_list and rows:
            first = rows[0]
            if isinstance(first, dict):
                # union keys in order seen
                seen = []
                for r in rows:
                    if isinstance(r, dict):
                        for k in r.keys():
                            if k not in seen:
                                seen.append(k)
                col_list = seen
            elif isinstance(first, (list, tuple)):
                col_list = [f"col{i+1}" for i in range(len(first))]

        # set header
        tw.setColumnCount(len(col_list))
        if col_list:
            tw.setHorizontalHeaderLabels([str(c) for c in col_list])
        else:
            tw.setHorizontalHeaderLabels([])

        # populate rows
        tw.setRowCount(0)
        for r in rows:
            row_idx = tw.rowCount()
            tw.insertRow(row_idx)
            if isinstance(r, dict):
                for ci, col in enumerate(col_list):
                    val = r.get(col, "")
                    item = QTableWidgetItem("" if val is None else str(val))
                    tw.setItem(row_idx, ci, item)
            else:
                # tuple/list row
                for ci in range(len(col_list)):
                    val = r[ci] if ci < len(r) else ""
                    item = QTableWidgetItem("" if val is None else str(val))
                    tw.setItem(row_idx, ci, item)

        # adjust minimal sizes
        tw.resizeColumnsToContents()
        # ensure at least default sizes
        hdr = tw.horizontalHeader()
        for i in range(tw.columnCount()):
            if hdr.sectionSize(i) < 80:
                hdr.resizeSection(i, 100)

    # ---------------- Nút thao tác - VNExpress (articles) ----------------
    def add_vnexpress(self):
        idv = self.article_id_input.text().strip()
        title = self.article_title_input.text().strip()
        published = self.article_published_input.text().strip()
        category = self.article_category_input.text().strip()
        summary = self.article_summary_input.text().strip()
        source = self.article_source_input.text().strip()

        # id auto if empty
        if not idv:
            idv = str(uuid.uuid4())

        if not title:
            QMessageBox.warning(self, "Thiếu dữ liệu", "Vui lòng nhập ít nhất Title.")
            return

        self._add_vnexpress_row(idv, title, published or "-", category or "-", summary or "-", source or "-")
        self.status.showMessage("Đã thêm 1 tin VNExpress.")
        # show generated id back to readOnly field for reference
        self.article_id_input.setText(idv)

    def edit_vnexpress(self):
        row = self.vnexpress_table.currentRow()
        if row < 0:
            QMessageBox.information(self, "Chọn hàng", "Vui lòng chọn một hàng để chỉnh sửa.")
            return
        # ID không cho sửa
        if self.article_title_input.text():
            self.vnexpress_table.setItem(row, 1, QTableWidgetItem(self.article_title_input.text()))
        if self.article_published_input.text():
            self.vnexpress_table.setItem(row, 2, QTableWidgetItem(self.article_published_input.text()))
        if self.article_category_input.text():
            self.vnexpress_table.setItem(row, 3, QTableWidgetItem(self.article_category_input.text()))
        if self.article_summary_input.text():
            self.vnexpress_table.setItem(row, 4, QTableWidgetItem(self.article_summary_input.text()))
        if self.article_source_input.text():
            self.vnexpress_table.setItem(row, 5, QTableWidgetItem(self.article_source_input.text()))
        self.status.showMessage("Đã chỉnh 1 tin VNExpress.")

    def delete_vnexpress(self):
        row = self.vnexpress_table.currentRow()
        if row < 0:
            QMessageBox.information(self, "Chọn hàng", "Vui lòng chọn một hàng để xoá.")
            return
        self.vnexpress_table.removeRow(row)
        self.status.showMessage("Đã xoá 1 tin VNExpress.")

    def refresh_vnexpress(self):
        # Clear inputs and clear table (làm mới cả bảng CRUD và danh sách)
        for inp in (self.article_id_input, self.article_title_input, self.article_published_input,
                    self.article_category_input, self.article_summary_input, self.article_source_input):
            inp.clear()
        self.vnexpress_table.setRowCount(0)
        self.status.showMessage("Dữ liệu VNExpress đã được làm mới.")

    # ---------------- Nút thao tác - Nhaccuatui (spotify_viral_chart) ----------------
    def add_song(self):
        idv = self.track_id_input.text().strip()
        date = self.track_date_input.text().strip()
        region = self.track_region_input.text().strip()
        chart_type = self.track_chart_type_input.text().strip()
        rank = self.track_rank_input.text().strip()
        prev = self.track_prev_rank_input.text().strip()
        delta = self.track_rank_delta_input.text().strip()
        movement = self.track_movement_input.text().strip()
        name = self.track_name_input.text().strip()
        artists = self.track_artists_input.text().strip()
        release = self.track_release_input.text().strip()

        # id auto if empty
        if not idv:
            idv = str(uuid.uuid4())

        if not name:
            QMessageBox.warning(self, "Thiếu dữ liệu", "Vui lòng nhập ít nhất Track Name.")
            return

        self._add_song_row(
            idv,
            date or "-",
            region or "-",
            chart_type or "-",
            rank or "-",
            prev or "-",
            delta or "-",
            movement or "-",
            name,
            artists or "-",
            release or "-"
        )
        self.status.showMessage("Đã thêm bài hát mới.")
        self.track_id_input.setText(idv)

    def edit_song(self):
        row = self.nhaccuatui_table.currentRow()
        if row < 0:
            QMessageBox.information(self, "Chọn hàng", "Vui lòng chọn một hàng để chỉnh sửa.")
            return
        # ID không cho sửa
        if self.track_date_input.text():
            self.nhaccuatui_table.setItem(row, 1, QTableWidgetItem(self.track_date_input.text()))
        if self.track_region_input.text():
            self.nhaccuatui_table.setItem(row, 2, QTableWidgetItem(self.track_region_input.text()))
        if self.track_chart_type_input.text():
            self.nhaccuatui_table.setItem(row, 3, QTableWidgetItem(self.track_chart_type_input.text()))
        if self.track_rank_input.text():
            self.nhaccuatui_table.setItem(row, 4, QTableWidgetItem(self.track_rank_input.text()))
        if self.track_prev_rank_input.text():
            self.nhaccuatui_table.setItem(row, 5, QTableWidgetItem(self.track_prev_rank_input.text()))
        if self.track_rank_delta_input.text():
            self.nhaccuatui_table.setItem(row, 6, QTableWidgetItem(self.track_rank_delta_input.text()))
        if self.track_movement_input.text():
            self.nhaccuatui_table.setItem(row, 7, QTableWidgetItem(self.track_movement_input.text()))
        if self.track_name_input.text():
            self.nhaccuatui_table.setItem(row, 8, QTableWidgetItem(self.track_name_input.text()))
        if self.track_artists_input.text():
            self.nhaccuatui_table.setItem(row, 9, QTableWidgetItem(self.track_artists_input.text()))
        if self.track_release_input.text():
            self.nhaccuatui_table.setItem(row, 10, QTableWidgetItem(self.track_release_input.text()))
        self.status.showMessage("Đã chỉnh 1 bài hát.")

    def delete_song(self):
        row = self.nhaccuatui_table.currentRow()
        if row < 0:
            QMessageBox.information(self, "Chọn hàng", "Vui lòng chọn một hàng để xoá.")
            return
        self.nhaccuatui_table.removeRow(row)
        self.status.showMessage("Đã xoá bài hát.")

    def refresh_song(self):
        # Clear inputs and clear table (làm mới cả bảng CRUD và danh sách)
        for inp in (self.track_id_input, self.track_date_input, self.track_region_input, self.track_chart_type_input,
                    self.track_rank_input, self.track_prev_rank_input, self.track_rank_delta_input,
                    self.track_movement_input, self.track_name_input, self.track_artists_input, self.track_release_input):
            inp.clear()
        self.nhaccuatui_table.setRowCount(0)
        self.status.showMessage("Dữ liệu Nhaccuatui đã được làm mới.")

    # ---------------- Filter handlers ----------------
    def filter_vnexpress(self):
        dlg = FilterDialog(self, include_rank=False)
        if dlg.exec_() != QDialog.Accepted:
            return
        month, year, _ = dlg.get_values()
        self._apply_filter_to_table(self.vnexpress_table, date_col=2, month=month, year=year, rank_col=None)

    def filter_song(self):
        dlg = FilterDialog(self, include_rank=True)
        if dlg.exec_() != QDialog.Accepted:
            return
        month, year, rank = dlg.get_values()
        self._apply_filter_to_table(self.nhaccuatui_table, date_col=1, month=month, year=year, rank_col=4, rank_filter=rank)

    def _apply_filter_to_table(self, table: QTableWidget, date_col:int=None, month:str="", year:str="", rank_col:int=None, rank_filter:str=""):
        try:
            rows = []
            for r in range(table.rowCount()):
                keep = True
                # check date
                if date_col is not None and date_col < table.columnCount():
                    item = table.item(r, date_col)
                    txt = item.text() if item else ""
                    # try to extract YYYY and MM
                    y = ""
                    m = ""
                    parts = txt.strip().split()
                    if parts:
                        datepart = parts[0]
                        segments = datepart.split("-")
                        if len(segments) >= 2:
                            y = segments[0]
                            m = segments[1]
                    if year and y != year:
                        keep = False
                    if month and m != month:
                        keep = False
                # check rank
                if keep and rank_col is not None and rank_col < table.columnCount() and rank_filter:
                    item = table.item(r, rank_col)
                    txt = item.text() if item else ""
                    # handle single or range
                    rank_filter = rank_filter.strip()
                    if "-" in rank_filter:
                        try:
                            lo, hi = rank_filter.split("-", 1)
                            lo = int(lo.strip()); hi = int(hi.strip())
                            val = int(txt) if txt.isdigit() else None
                            if val is None or not (lo <= val <= hi):
                                keep = False
                        except Exception:
                            # invalid range => no match
                            keep = False
                    else:
                        try:
                            want = int(rank_filter)
                            val = int(txt) if txt.isdigit() else None
                            if val is None or val != want:
                                keep = False
                        except Exception:
                            keep = False
                rows.append((r, keep))
            # apply: hide rows that do not match (we will remove and re-add matched rows to show filtered set)
            matched = []
            for r, keep in rows:
                if keep:
                    # capture row data
                    rowdata = []
                    for c in range(table.columnCount()):
                        it = table.item(r, c)
                        rowdata.append(it.text() if it else "")
                    matched.append(rowdata)
            # replace table contents with matched rows
            table.setRowCount(0)
            for rowdata in matched:
                r = table.rowCount()
                table.insertRow(r)
                for c, v in enumerate(rowdata):
                    table.setItem(r, c, QTableWidgetItem(v))
            self.status.showMessage(f"Lọc xong — {len(matched)} hàng khớp.")
        except Exception as e:
            QMessageBox.warning(self, "Lỗi lọc", f"Lỗi khi lọc dữ liệu: {e}")

    # ---------------- internal helpers to insert rows into the QTableWidgets ----------------
    def _add_vnexpress_row(self, idv, title, published, category, summary, source):
        r = self.vnexpress_table.rowCount()
        self.vnexpress_table.insertRow(r)
        self.vnexpress_table.setItem(r, 0, QTableWidgetItem(idv))
        self.vnexpress_table.setItem(r, 1, QTableWidgetItem(title))
        self.vnexpress_table.setItem(r, 2, QTableWidgetItem(published))
        self.vnexpress_table.setItem(r, 3, QTableWidgetItem(category))
        self.vnexpress_table.setItem(r, 4, QTableWidgetItem(summary))
        self.vnexpress_table.setItem(r, 5, QTableWidgetItem(source))

    def _add_song_row(self, idv, date, region, chart_type, rank, prev_rank, rank_delta, movement, track_name, artists, release_date):
        r = self.nhaccuatui_table.rowCount()
        self.nhaccuatui_table.insertRow(r)
        self.nhaccuatui_table.setItem(r, 0, QTableWidgetItem(idv))
        self.nhaccuatui_table.setItem(r, 1, QTableWidgetItem(date))
        self.nhaccuatui_table.setItem(r, 2, QTableWidgetItem(region))
        self.nhaccuatui_table.setItem(r, 3, QTableWidgetItem(chart_type))
        self.nhaccuatui_table.setItem(r, 4, QTableWidgetItem(str(rank)))
        self.nhaccuatui_table.setItem(r, 5, QTableWidgetItem(str(prev_rank)))
        self.nhaccuatui_table.setItem(r, 6, QTableWidgetItem(str(rank_delta)))
        self.nhaccuatui_table.setItem(r, 7, QTableWidgetItem(movement))
        self.nhaccuatui_table.setItem(r, 8, QTableWidgetItem(track_name))
        self.nhaccuatui_table.setItem(r, 9, QTableWidgetItem(artists))
        self.nhaccuatui_table.setItem(r, 10, QTableWidgetItem(release_date))

    def show_about(self):
        QMessageBox.information(self, "About", "Big Data GUI — Pro\nTác giả: bạn")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = BigDataGUI()
    window.show()
    sys.exit(app.exec_())
