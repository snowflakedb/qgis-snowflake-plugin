import traceback

from ..helpers.data_base import get_columns_cursor
from ..helpers.utils import get_qsettings, get_authentification_information
from ..helpers.layer_creation import get_layers
from qgis.core import (
    QgsMapLayer,
    QgsProject,
    QgsTask,
    QgsFields,
)
from qgis.PyQt.QtCore import pyqtSignal


class SFConvertColumnToLayerTask(QgsTask):
    on_handle_error = pyqtSignal(str, str)
    on_handle_warning = pyqtSignal(str, str)

    def __init__(self, connection_name: str, information_dict: dict):
        """
        Initializes a SFConvertColumnToLayerTask object.

        Args:
            connection_name (str): The name of the connection.
            information_dict (dict): A dictionary containing information about the column, table, schema, and optional column type.

        Raises:
            Exception: If initialization fails.

        """
        try:
            self.column = information_dict["column"]
            self.table = information_dict["table"]
            self.schema = information_dict["schema"]
            self.column_type = None
            if "column_type" in information_dict:
                self.column_type = information_dict["column_type"]
            super().__init__(
                f"Snowflake Add Map Layer From {self.schema}.{self.table}.{self.column}",
                QgsTask.CanCancel,
            )
            self.settings = get_qsettings()
            self.auth_information = get_authentification_information(
                self.settings, connection_name
            )
            self.database_name = self.auth_information["database"]
            self.information_dict = information_dict
            self.connection_name = connection_name
            self.feature_fields_list: QgsFields = None
            self.layers = []
        except Exception as e:
            self.on_handle_error.emit(
                "SFConvertColumnToLayerTask init failed",
                f"Initializing snowflake convert column to layer task failed.\n\nExtended error information:\n{str(e)}",
            )

    def run(self) -> bool:
        """
        Executes the task to convert a Snowflake column to a layer.

        Returns:
            bool: True if the task is executed successfully, False otherwise.
        """
        try:
            cur_select_columns = get_columns_cursor(
                auth_information=self.auth_information,
                database_name=self.database_name,
                schema=self.schema,
                table=self.table,
                connection_name=self.connection_name,
            )
            if cur_select_columns.rowcount == 0:
                return True
            query_columns = ""
            for row in cur_select_columns:
                if row[1] in ["GEOMETRY", "GEOGRAPHY"]:
                    if row[0] == self.column:
                        if query_columns != "":
                            query_columns += ", "
                        query_columns += f"ST_ASWKB({row[0]}) AS {self.column}"
                    else:
                        continue
                else:
                    if query_columns != "":
                        query_columns += ", "
                    query_columns += row[0]
            cur_select_columns.close()
            query = f"""SELECT {query_columns}
FROM "{self.database_name}"."{self.schema}"."{self.table}"
ORDER BY RANDOM()
LIMIT 1000000"""

            layer_pre_name = f"{self.auth_information['database']}.{self.information_dict['schema']}.{self.information_dict['table']}_{self.information_dict['column']}"

            cancel_error_status, self.layers = get_layers(
                self.auth_information,
                layer_pre_name,
                query,
                self.connection_name,
                self.column,
                self,
            )
            return cancel_error_status
        except Exception as e:
            stack_trace = traceback.format_exc()
            self.on_handle_error.emit(
                "SFConvertColumnToLayerTask run failed",
                f"Running snowflake convert column to layer task failed.\n\nExtended error information:\n{str(e)}-{stack_trace}",
            )
            return False

    def finished(self, result: bool) -> None:
        """
        Callback function called when the task is finished.

        Args:
            result (bool): The result of the task.

        Returns:
            None
        """
        if result:
            for layer in self.layers:
                if isinstance(layer, QgsMapLayer):
                    QgsProject.instance().addMapLayer(layer)
                    QgsProject.instance().layerTreeRoot()
