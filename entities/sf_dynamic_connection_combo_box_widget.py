import json
from PyQt5.QtWidgets import (
    QVBoxLayout,
    QLabel,
    QComboBox,
    QWidget,
    QMessageBox,
)

from ..providers.sf_data_source_provider import SFDataProvider

from ..helpers.utils import get_authentification_information, get_qsettings
from processing.gui.wrappers import WidgetWrapper


class DynamicConnectionComboBoxWidget(WidgetWrapper):
    def createWidget(self):
        widget = QWidget()
        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        self.connections_cb = QComboBox()
        self.schemas_cb = QComboBox()
        self.tables_cb = QComboBox()

        # Populate the comboboxes with your desired items
        self.settings = get_qsettings()
        self.connections_cb.addItems(self.get_connections_cb_options())
        self.connections_cb.currentIndexChanged.connect(self.update_schemas_cb)
        self.schemas_cb.currentIndexChanged.connect(self.update_tables_cb)

        layout.addWidget(self.connections_cb)
        self.schemas_lb = QLabel("Schema (schema name)")
        layout.addWidget(self.schemas_lb)
        layout.addWidget(self.schemas_cb)
        self.tables_lb = QLabel("Table to export to (leave blank to use layer name)")
        layout.addWidget(self.tables_lb)
        layout.addWidget(self.tables_cb)
        widget.setLayout(layout)

        return widget

    def get_connections_cb_options(self):
        connections_cb_options = []
        root_groups = self.settings.childGroups()
        for group in root_groups:
            connections_cb_options.append(group)
        connections_cb_options.insert(0, "")
        return connections_cb_options

    def update_tables_cb(self):
        try:
            selected_connection = self.connections_cb.currentText()
            selected_schema = self.schemas_cb.currentText()
            self.tables_cb.clear()
            if selected_connection == "" or selected_schema == "":
                return
            auth_information = get_authentification_information(
                self.settings, selected_connection
            )
            sf_data_provider = SFDataProvider(auth_information)

            query = f"""
                SELECT DISTINCT TABLE_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_catalog = '{auth_information["database"]}'
                AND TABLE_SCHEMA = '{selected_schema}'
                ORDER BY TABLE_NAME;
            """

            sf_data_provider.load_data(query, selected_connection)
            feature_iterator = sf_data_provider.get_feature_iterator()
            self.tables_cb.addItem("")
            for feat in feature_iterator:
                self.tables_cb.addItem(feat.attribute("TABLE_NAME"))
            feature_iterator.close()
        except Exception as e:
            QMessageBox.information(
                None,
                "Connection Widget - Update Table Combobox",
                f"Connection Widget - Update table failed.\n\nExtended error information:\n{str(e)}",
            )

    def update_schemas_cb(self):
        try:
            selected_connection = self.connections_cb.currentText()
            self.schemas_cb.clear()
            self.tables_cb.clear()
            if selected_connection == "":
                return
            auth_information = get_authentification_information(
                self.settings, selected_connection
            )
            sf_data_provider = SFDataProvider(auth_information)

            query = f"""
                SELECT DISTINCT TABLE_SCHEMA
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_catalog = '{auth_information["database"]}'
                ORDER BY TABLE_SCHEMA;
            """

            sf_data_provider.load_data(query, selected_connection)
            feature_iterator = sf_data_provider.get_feature_iterator()
            self.schemas_cb.addItem("")
            for feat in feature_iterator:
                self.schemas_cb.addItem(feat.attribute("TABLE_SCHEMA"))
            feature_iterator.close()
        except Exception as e:
            QMessageBox.information(
                None,
                "Connection Widget - Update Schema Combobox",
                f"Connection Widget - Update schema failed.\n\nExtended error information:\n{str(e)}",
            )

    def get_selected_options(self):
        auth_information = get_authentification_information(
            self.settings, self.connections_cb.currentText()
        )
        return json.dumps(
            [
                self.connections_cb.currentText(),
                auth_information["database"],
                self.schemas_cb.currentText(),
                self.tables_cb.currentText(),
            ]
        )

    def value(self):
        return self.get_selected_options()