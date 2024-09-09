from PyQt5.QtWidgets import QWidget
from qgis.core import Qgis
from qgis.gui import QgsMessageBar
from qgis.PyQt import uic
from qgis.PyQt.QtCore import QSettings, pyqtSignal
from qgis.PyQt.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QMessageBox,
)
import os
import typing

from ..managers.sf_connection_manager import SFConnectionManager


FORM_CLASS_SFCS, _ = uic.loadUiType(
    os.path.join(os.path.dirname(__file__), "sf_connection_string_dialog.ui")
)


class SFConnectionStringDialog(QDialog, FORM_CLASS_SFCS):
    update_connections_signal = pyqtSignal()

    def __init__(
        self, parent: typing.Optional[QWidget] = None, connection_name: str = ""
    ) -> None:
        """
        Initialize the SFConnectionStringDialog.

        Args:
            parent (QWidget, optional): The parent widget. Defaults to None.
            connection_name (str, optional): The name of the connection. Defaults to "".
        """
        super().__init__(parent)
        self.setupUi(self)
        self.btnConnect.clicked.connect(self.test_connection_clicked)
        ok_button = self.buttonBox.button(QDialogButtonBox.Ok)
        ok_button.clicked.connect(self.button_box_ok_clicked)
        self.settings = QSettings(
            QSettings.IniFormat, QSettings.UserScope, "Snowflake", "SF_QGIS_PLUGIN"
        )
        self.cbxConnectionType.addItem("Default Authentication")
        self.cbxConnectionType.addItem("Single sign-on (SSO)")
        self.connection_name = connection_name
        self.deactivate_temp()

    def deactivate_temp(self) -> None:
        """
        Hides certain checkboxes in the UI.

        This method is used to hide specific checkboxes in the user interface.
        The checkboxes that are hidden include:
        - cb_geometryColumnsOnly
        - cb_dontResolveType
        - cb_publicSchemaOnly
        - cb_allowGeometrylessTables
        - cb_useEstimatedMetadata
        - cb_projectsInDatabase
        - cb_metadataInDatabase

        This method does not return any value.
        """
        self.cb_geometryColumnsOnly.setVisible(False)
        self.cb_dontResolveType.setVisible(False)
        self.cb_publicSchemaOnly.setVisible(False)
        self.cb_allowGeometrylessTables.setVisible(False)
        self.cb_useEstimatedMetadata.setVisible(False)
        self.cb_projectsInDatabase.setVisible(False)
        self.cb_metadataInDatabase.setVisible(False)

    def button_box_ok_clicked(self) -> None:
        """
        Save the connection settings when the OK button is clicked.

        This method saves the values entered in the dialog's text fields and combo boxes
        to the corresponding settings in the QSettings object. It also handles the removal
        of the previous connection settings if the connection name has been changed.

        Raises:
            Exception: If there is an error saving the connection settings.

        Signals:
            update_connections_signal: Emitted after the connection settings have been saved.
        """
        try:
            from ..helpers.utils import remove_connection

            self.settings.beginGroup(self.txtName.text())
            self.settings.setValue("warehouse", self.txtWarehouse.text())
            self.settings.setValue("account", self.txtAccount.text())
            self.settings.setValue("database", self.txtDatabase.text())
            self.settings.setValue("username", self.mAuthSettings.username())
            self.settings.setValue(
                "connection_type", self.cbxConnectionType.currentText()
            )
            if self.cbxConnectionType.currentText() == "Default Authentication":
                self.settings.setValue("password", self.mAuthSettings.password())
            self.settings.endGroup()
            self.settings.sync()
            if self.connection_name != self.txtName.text():
                if self.connection_name is not None and self.connection_name != "":
                    remove_connection(self.settings, self.connection_name)
            self.update_connections_signal.emit()
        except Exception as e:
            QMessageBox.information(
                None,
                "Save Connection",
                f"Error saving connection settings.\n\nExtended error information:\n{str(e)}",
            )

    def test_connection_clicked(self) -> None:
        """
        Test the connection to the Snowflake database using the provided settings.

        Raises:
            snowflake.connector.Error: If there is an error connecting to the Snowflake database.
            Exception: If there is an unexpected error during the connection process.
        """
        try:
            sf_connection_manager = SFConnectionManager.get_instance()
            connection_params = {
                "user": self.mAuthSettings.username(),
                "account": self.txtAccount.text(),
                "warehouse": self.txtWarehouse.text(),
                "database": self.txtDatabase.text(),
                "login_timeout": 5,
            }
            if self.cbxConnectionType.currentText() == "Default Authentication":
                connection_params["password"] = self.mAuthSettings.password()
                conn = sf_connection_manager.create_snowflake_connection(
                    connection_params
                )

            elif self.cbxConnectionType.currentText() == "Single sign-on (SSO)":
                connection_params["authenticator"] = "externalbrowser"
                conn = sf_connection_manager.create_snowflake_connection(
                    connection_params
                )

            if conn:
                conn.close()

            qg_message_bar = QgsMessageBar(self)
            self.layout().addWidget(qg_message_bar, 0)
            style_sheet = "color: black;"
            qg_message_bar.setStyleSheet(style_sheet)
            qg_message_bar.pushMessage(
                "Test Connection",
                f"Connection to {self.txtName} was successful.",
                Qgis.MessageLevel.Success,
                5,
            )
        except Exception as e:
            QMessageBox.information(
                None,
                "Test Connection",
                f"Connection failed - Check settings and try again.\n\nExtended error information:\n{str(e)}",
            )
