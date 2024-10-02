from PyQt5.QtWidgets import QWidget
from qgis.PyQt.QtGui import QStandardItemModel, QStandardItem
from qgis.PyQt import uic
from qgis.PyQt.QtCore import pyqtSignal, QVariant, QByteArray
from qgis.PyQt.QtWidgets import QDialog, QMessageBox
from qgis.core import QgsApplication
import os
import typing

from ..tasks.sf_convert_sql_query_to_layer_task import SFConvertSQLQueryToLayerTask

from ..tasks.sf_execute_sql_query_task import SFExecuteSQLQueryTask

from ..helpers.utils import get_qsettings


FORM_CLASS_SFCS, _ = uic.loadUiType(
    os.path.join(os.path.dirname(__file__), "sf_sql_query_dialog.ui")
)


class SFSQLQueryDialog(QDialog, FORM_CLASS_SFCS):
    update_connections_signal = pyqtSignal()

    def __init__(
        self,
        connection_name: str,
        parent: typing.Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self.setupUi(self)
        self.connection_name = connection_name
        self.settings = get_qsettings()
        self.temp_deactivated_options()
        self.mExecuteButton.setEnabled(True)
        self.mExecuteButton.clicked.connect(self.on_execute_button_clicked)
        self.mLoadLayerPushButton.clicked.connect(
            self.on_load_layer_push_button_clicked
        )
        self.mClearButton.clicked.connect(self.on_clear_button_clicked)

    def on_clear_button_clicked(self):
        self.model = QStandardItemModel()
        self.mQueryResultsTableView.setModel(self.model)
        self.mSqlErrorText.clear()
        self.mGeometryColumnCheckBox.setChecked(False)

    def on_load_layer_push_button_clicked(self):
        try:
            if self.mGeometryColumnCheckBox.isChecked():
                sf_convert_sql_query_to_layer_task = SFConvertSQLQueryToLayerTask(
                    self.connection_name,
                    self.mSqlErrorText.text(),
                    self.mGeometryColumnComboBox.currentText(),
                    self.mLayerNameLineEdit.text(),
                )
                sf_convert_sql_query_to_layer_task.on_handle_error.connect(
                    self.on_handle_error
                )
                QgsApplication.taskManager().addTask(sf_convert_sql_query_to_layer_task)
        except Exception as e:
            QMessageBox.information(
                None,
                "SFSQLQueryDialog - Add Layer from SQL Query",
                f"Adding Layer from SQL Query failed.\n\nExtended error information:\n{str(e)}",
            )

    def temp_deactivated_options(self) -> None:
        self.mProgressBar.setVisible(False)
        self.mPkColumnsCheckBox.setVisible(False)
        self.mPkColumnsComboBox.setVisible(False)
        self.mFilterLabel.setVisible(False)
        self.mFilterLineEdit.setVisible(False)
        self.mFilterToolButton.setVisible(False)
        self.mAvoidSelectingAsFeatureIdCheckBox.setVisible(False)
        self.mStopButton.setVisible(False)
        self.mStatusLabel.setVisible(False)

    def on_execute_button_clicked(self):
        try:
            snowflake_covert_column_to_layer_task = SFExecuteSQLQueryTask(
                self.connection_name,
                self.mSqlErrorText.text(),
            )
            snowflake_covert_column_to_layer_task.on_handle_error.connect(
                self.on_handle_error
            )
            snowflake_covert_column_to_layer_task.on_data_ready.connect(
                self.on_data_ready
            )
            QgsApplication.taskManager().addTask(snowflake_covert_column_to_layer_task)
        except Exception as e:
            QMessageBox.information(
                None,
                "SFSQLQueryDialog - Execute SQL Query",
                f"Executing SQL Query failed.\n\nExtended error information:\n{str(e)}",
            )

    def on_data_ready(self, column_names, features):
        try:
            self.model = QStandardItemModel()
            self.model.setHorizontalHeaderLabels(column_names)
            self.mQueryResultsTableView.setModel(self.model)
            for feat in features:
                row_items = []
                is_null = False
                for attr in feat.attributes():
                    if isinstance(attr, QVariant):
                        if attr.isNull():
                            is_null = True
                            continue
                    if attr is None:
                        is_null = True
                        continue
                    byte_array = QByteArray(feat.geometry().asWkb())

                    # Convert QByteArray to hexadecimal string
                    hex_string = byte_array.toHex().data().decode()
                    row_items.append(QStandardItem(hex_string))
                if not is_null:
                    self.model.appendRow(row_items)

            self.mGeometryColumnComboBox.clear()
            for f in column_names:
                self.mGeometryColumnComboBox.addItem(f)
        except Exception as e:
            QMessageBox.information(
                None,
                "SFSQLQueryDialog - Data Ready",
                f"Data Ready failed.\n\nExtended error information:\n{str(e)}",
            )

    def on_handle_error(self, title, message):
        # select * from test_allan.country_information_100
        QMessageBox.information(None, title, message)
