from PyQt5.QtWidgets import QWidget
from qgis.PyQt.QtGui import QStandardItemModel, QStandardItem
from qgis.PyQt import uic
from qgis.PyQt.QtCore import pyqtSignal
from qgis.PyQt.QtWidgets import QDialog, QDialogButtonBox, QMessageBox, QTableView
import os
import typing

from ..providers.sf_data_source_provider import SFDataProvider

from ..helpers.utils import get_authentification_information, get_qsettings


FORM_CLASS_SFCS, _ = uic.loadUiType(
    os.path.join(os.path.dirname(__file__), "sf_new_schema_dialog.ui")
)


class SFNewSchemaDialog(QDialog, FORM_CLASS_SFCS):
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
        ok_button = self.buttonBox.button(QDialogButtonBox.Ok)
        ok_button.clicked.connect(self.button_box_ok_clicked)

    def button_box_ok_clicked(self):
        try:
            auth_information = get_authentification_information(
                self.settings, self.connection_name
            )
            sf_data_provider = SFDataProvider(auth_information)
            schema_name = self.txtSchemaName.text()
            if schema_name == "":
                QMessageBox.information(
                    None,
                    "New Schema Add Schema Btn",
                    "Schema name cannot be empty.",
                )
                return

            query = (
                f"CREATE OR REPLACE SCHEMA {auth_information['database']}.{schema_name}"
            )
            sf_data_provider.load_data(query, self.connection_name)
            self.update_connections_signal.emit()
        except Exception as e:
            QMessageBox.information(
                None,
                "New Schema Add Schema Btn",
                f"Adding Schema failed.\n\nExtended error information:\n{str(e)}",
            )
