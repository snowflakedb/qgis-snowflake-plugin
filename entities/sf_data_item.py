from ..helpers.utils import (
    get_authentification_information,
    get_qsettings,
    on_handle_error,
    remove_connection,
)
from ..providers.sf_data_source_provider import SFDataProvider
from ..tasks.sf_convert_column_to_layer_task import SFConvertColumnToLayerTask
from ..ui.sf_connection_string_dialog import SFConnectionStringDialog
from PyQt5.QtCore import pyqtSignal
from qgis.core import QgsDataItem, Qgis, QgsApplication, QgsErrorItem
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtWidgets import QMessageBox, QAction, QWidget
import typing


class SFDataItem(QgsDataItem):
    message_handler = pyqtSignal(str, str)

    def __init__(
        self,
        type: Qgis.BrowserItemType,
        parent: "SFDataItem",
        name: str,
        path: str,
        provider_key: str,
        item_type: str,
        icon_path: str,
        connection_name: str = None,
    ) -> None:
        """
        Initializes a SFDataItem object.

        Args:
            type (Qgis.BrowserItemType): The type of the browser item.
            parent (SFDataItem): The parent item of the browser item.
            name (str): The name of the browser item.
            path (str): The path of the browser item.
            provider_key (str): The provider key of the browser item.
            item_type (str): The type of the item.
            icon_path (str): The path to the icon of the browser item.
            connection_name (str, optional): The name of the connection. Defaults to None.
        """
        super().__init__(type, parent, name, path, provider_key)
        self.item_type = item_type
        self.settings = get_qsettings()
        self.setIcon(QIcon(icon_path))
        self.connection_name = connection_name
        self.message_handler.connect(self.on_message_handler)

    def createChildren(self) -> typing.List["QgsDataItem"]:
        """
        Creates and returns a list of child QgsDataItem objects based on the item type.

        Returns:
            List[QgsDataItem]: A list of child QgsDataItem objects.

        Raises:
            Exception: If an error occurs during the creation of child items.
        """
        children: typing.List["SFDataItem"] = []
        try:
            if self.item_type == "column":
                pass
            elif self.item_type == "root":
                root_groups = self.settings.childGroups()
                for group in root_groups:
                    item = self._create_data_item(group, "connection", group)
                    children.append(item)
            else:
                auth_information, column_name, children_item_type, query = (
                    self._get_query_metadata()
                )
                if (
                    self.item_type == "connection"
                    and auth_information["connection_type"] == "Single sign-on (SSO)"
                ):
                    self.parent().message_handler.emit(
                        "Single Sign-On (SSO) Authorization Required",
                        "Please check your third-party authentication application to authorize the connection. Ensure that any required permissions or approvals are granted to complete the Single Sign-On (SSO) process..",
                    )

                sf_data_provider = SFDataProvider(auth_information)

                sf_data_provider.load_data(query, self.connection_name)
                feature_iterator = sf_data_provider.get_feature_iterator()

                for feat in feature_iterator:
                    item = self._create_data_item(
                        feat.attribute(column_name),
                        children_item_type,
                        self.connection_name,
                    )
                    children.append(item)
                feature_iterator.close()

            return children

        except Exception as e:
            error_item = QgsErrorItem(
                self,
                f"SFDataItem - Data item children creation failed.\n\nExtended error information:\n{str(e)}",
                self.path() + "/error",
            )
            children.append(error_item)
        return children

    def _get_query_metadata(self) -> typing.Tuple[dict, str, str, str]:
        """
        Retrieves the query metadata for the Snowflake data item.
        Returns:
            A tuple containing the following elements:
            - auth_information (dict): The authentication information for the Snowflake connection.
            - column_name (str): The name of the column used for filtering.
            - children_item_type (str): The type of children items.
            - query (str): The SQL query used to retrieve the metadata.
        """
        auth_information = get_authentification_information(
            self.settings, self.connection_name
        )

        schema_filter = ""
        table_filter = ""
        if self.item_type == "root":
            column_name = ""
            children_item_type = "connection"
        elif self.item_type == "connection":
            column_name = "TABLE_SCHEMA"
            children_item_type = "schema"
        elif self.item_type == "schema":
            column_name = "TABLE_NAME"
            children_item_type = "table"
            schema_filter = f"AND TABLE_SCHEMA = '{self.name()}'"
        elif self.item_type == "table":
            schema_filter = f"AND TABLE_SCHEMA = '{self.parent().name()}'"
            table_filter = f"AND TABLE_NAME = '{self.name()}'"
            column_name = "COLUMN_NAME"
            children_item_type = "column"
        query = f"""
                    SELECT DISTINCT {column_name}
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE table_catalog = '{auth_information["database"]}'
                    {schema_filter}
                    {table_filter}
                    AND DATA_TYPE in ('GEOGRAPHY', 'GEOMETRY')
                    ORDER BY {column_name};
                """

        return auth_information, column_name, children_item_type, query

    def _create_data_item(
        self, name: str, type: str, connection_name: str
    ) -> "SFDataItem":
        """
        Create a SFDataItem object.

        Parameters:
        - name (str): The name of the data item.
        - type (str): The type of the data item.
        - connection_name (str): The name of the connection.

        Returns:
        - SFDataItem: The created SFDataItem object.
        """
        item = SFDataItem(
            type=Qgis.BrowserItemType.Field,
            parent=self,
            name=name,
            path=f"{self.path()}/{name}",
            provider_key=self.providerKey(),
            item_type=type,
            icon_path=f":/plugins/qgis-py-plugin/ui/images/{type}.svg",
            connection_name=connection_name,
        )

        return item

    def handleDoubleClick(self) -> bool:
        """
        Handles the double click event for the data item.

        Returns:
            bool: True if the double click event is handled successfully, False otherwise.
        """
        try:
            if self.item_type == "column":
                path_split = self.path().split("/")
                information_dict = {
                    "schema": path_split[3],
                    "table": path_split[4],
                    "column": path_split[5],
                }
                snowflake_covert_column_to_layer_task = SFConvertColumnToLayerTask(
                    self.connection_name,
                    information_dict,
                )
                snowflake_covert_column_to_layer_task.on_handle_error.connect(
                    on_handle_error
                )
                QgsApplication.taskManager().addTask(
                    snowflake_covert_column_to_layer_task
                )

            return True
        except Exception as _:
            return False

    def actions(self, parent: QWidget) -> typing.List[QAction]:
        """
        Generates a list of QAction objects based on the item type.

        Args:
            parent (QWidget): The parent widget.

        Returns:
            typing.List[QAction]: A list of QAction objects.

        Raises:
            Exception: If there is an error creating the actions.

        """
        try:
            # Create a list to hold actions
            action_list: typing.List[QAction] = []

            # # Create another action
            self.refresh_action = QAction("Refresh", None)
            self.refresh_action.triggered.connect(self.on_refresh_action_triggered)
            action_list.append(self.refresh_action)

            if self.item_type == "schema":
                self.new_table_action = QAction("New Table...", None)
                self.new_table_action.triggered.connect(
                    self.on_new_table_action_triggered
                )
                action_list.append(self.new_table_action)

            if self.item_type == "connection":
                self.edit_connection_action = QAction("Edit Connection", None)
                self.edit_connection_action.triggered.connect(
                    self.on_edit_connection_action_triggered
                )
                action_list.append(self.edit_connection_action)

                self.remove_connection_action = QAction("Remove Connection", None)
                self.remove_connection_action.triggered.connect(
                    self.on_remove_connection_action_triggered
                )
                action_list.append(self.remove_connection_action)

                self.new_schema_action = QAction("New Schema...", None)
                self.new_schema_action.triggered.connect(
                    self.on_new_schema_action_triggered
                )
                action_list.append(self.new_schema_action)

            if self.item_type != "root":
                self.execute_sql_action = QAction("Execute SQL...", None)
                self.execute_sql_action.triggered.connect(
                    self.on_execute_sql_action_triggered
                )
                action_list.append(self.execute_sql_action)

            if self.item_type == "root":
                self.new_connection_action = QAction("New Connection", None)
                self.new_connection_action.triggered.connect(
                    self.on_new_connection_action_triggered
                )
                action_list.append(self.new_connection_action)

            return action_list
        except Exception as e:
            QMessageBox.information(
                None,
                "Data Item Actions Creation Error",
                f"SFDataItem - Data item actions creation failed.\n\nExtended error information:\n{str(e)}",
            )

    def on_execute_sql_action_triggered(self) -> None:
        """
        Opens a dialog for executing SQL queries on the Snowflake database.
        The dialog allows the user to enter the SQL query to be executed on the Snowflake database.
        After the user enters the SQL query and confirms, the query is executed and the connections are handled accordingly.
        """
        from ..ui.sf_sql_query_dialog import SFSQLQueryDialog

        sf_sql_query_dialog = SFSQLQueryDialog(self.connection_name, None)
        sf_sql_query_dialog.update_connections_signal.connect(
            self.on_update_connections_handle
        )
        sf_sql_query_dialog.exec_()

    def on_new_schema_action_triggered(self) -> None:
        """
        Opens a dialog for creating a new schema in the Snowflake database.
        The dialog allows the user to enter the necessary information for creating a new schema in the Snowflake database.
        After the user enters the schema details and confirms, the schema is created and the connections are handled accordingly.
        """
        from ..ui.sf_new_schema_dialog import SFNewSchemaDialog

        sf_connection_string_dialog = SFNewSchemaDialog(self.connection_name, None)
        sf_connection_string_dialog.update_connections_signal.connect(
            self.on_update_connections_handle
        )
        sf_connection_string_dialog.exec_()

    def on_new_table_action_triggered(self) -> None:
        """
        Opens a dialog for creating a new table in the Snowflake database.
        The dialog allows the user to enter the necessary information for creating a new table in the Snowflake database.
        After the user enters the table details and confirms, the table is created and the connections are handled accordingly.
        """
        from ..ui.sf_new_table_dialog import SFNewTableDialog

        sf_connection_string_dialog = SFNewTableDialog(
            self.name(), self.connection_name, None
        )
        sf_connection_string_dialog.update_connections_signal.connect(
            self.on_update_connections_handle
        )
        sf_connection_string_dialog.exec_()

    def on_new_connection_action_triggered(self) -> None:
        """
        Opens a dialog for creating a new Snowflake connection string.
        The dialog allows the user to enter the necessary information for establishing a connection to a Snowflake database.
        After the user enters the connection details and confirms, the connection string is updated and the connections are handled accordingly.
        """
        sf_connection_string_dialog = SFConnectionStringDialog(None)
        sf_connection_string_dialog.update_connections_signal.connect(
            self.on_update_connections_handle
        )
        sf_connection_string_dialog.exec_()

    def on_edit_connection_action_triggered(self) -> None:
        """
        Opens a dialog window for editing the connection settings.

        This method creates an instance of the SFConnectionStringDialog class and sets the parent to None.
        It passes the connection name as a parameter to the dialog window.
        The authentication information is retrieved using the get_authentification_information function.

        The connection type is set in the dialog window based on the value retrieved from the authentication information.
        The name, warehouse, account, and database fields in the dialog window are populated with the corresponding values from the authentication information.

        The username and password fields in the dialog window are set using the setUsername and setPassword methods of the mAuthSettings object.

        The update_connections_signal signal is connected to the on_update_connections_handle method.

        Finally, the dialog window is executed.
        """

        sf_connection_string_dialog_window = SFConnectionStringDialog(
            parent=None, connection_name=self.name()
        )
        auth_information = get_authentification_information(self.settings, self.name())

        index = sf_connection_string_dialog_window.cbxConnectionType.findText(
            auth_information["connection_type"]
        )
        if index != -1:
            sf_connection_string_dialog_window.cbxConnectionType.setCurrentIndex(index)

        sf_connection_string_dialog_window.txtName.setText(self.name())
        sf_connection_string_dialog_window.txtWarehouse.setText(
            auth_information["warehouse"]
        )
        sf_connection_string_dialog_window.txtAccount.setText(
            auth_information["account"]
        )
        sf_connection_string_dialog_window.txtDatabase.setText(
            auth_information["database"]
        )

        sf_connection_string_dialog_window.mAuthSettings.setUsername(
            auth_information["username"]
        )
        sf_connection_string_dialog_window.mAuthSettings.setPassword(
            auth_information["password"]
        )
        sf_connection_string_dialog_window.update_connections_signal.connect(
            self.on_update_connections_handle
        )
        sf_connection_string_dialog_window.exec_()

    def on_update_connections_handle(self) -> None:
        """
        Handle the update of connections.

        If the item type is "root", refresh the item.
        If the item type is "connection", refresh the parent item.
        """
        if self.item_type == "root":
            self.refresh()
        if self.item_type == "connection":
            self.parent().refresh()

    def on_remove_connection_action_triggered(self) -> None:
        """
        Removes the connection associated with the data item.

        This method removes the connection from the settings and refreshes the parent widget.

        Returns:
            None
        """
        remove_connection(self.settings, self.name())
        self.parent().refresh()

    def on_refresh_action_triggered(self) -> None:
        """
        Refreshes the data item.
        """
        self.refresh()

    def on_message_handler(self, title: str, message: str) -> None:
        """
        Handle the message by displaying it in a QMessageBox.

        Parameters:
        - title (str): The title of the message box.
        - message (str): The message to be displayed.

        Returns:
        - None
        """
        QMessageBox.information(None, title, message)
