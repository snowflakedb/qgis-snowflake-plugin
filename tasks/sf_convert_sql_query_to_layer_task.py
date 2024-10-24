from ..helpers.data_base import get_cursor_description
from ..helpers.utils import get_qsettings, get_authentification_information
from ..helpers.layer_creation import get_layers
from qgis.core import (
    QgsMapLayer,
    QgsProject,
    QgsTask,
)
from qgis.PyQt.QtCore import pyqtSignal


class SFConvertSQLQueryToLayerTask(QgsTask):
    on_handle_error = pyqtSignal(str, str)

    def __init__(
        self, connection_name: str, query: str, geo_column_name: str, layer_name: str
    ):
        try:
            self.query = query
            self.geo_column_name = geo_column_name
            self.layer_name = layer_name
            super().__init__(
                f"Snowflake convert query to layer: {self.query}",
                QgsTask.CanCancel,
            )
            self.settings = get_qsettings()
            self.auth_information = get_authentification_information(
                self.settings, connection_name
            )
            self.database_name = self.auth_information["database"]
            self.connection_name = connection_name
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
            cur_description = get_cursor_description(
                auth_information=self.auth_information,
                query=self.query,
                connection_name=self.connection_name,
            )

            column_names = ""
            for desc in cur_description:
                if column_names != "":
                    column_names += ", "
                desc_col_name = desc.name
                if desc_col_name == self.geo_column_name:
                    column_names += (
                        f"ST_ASWKB({self.geo_column_name}) AS {self.geo_column_name}"
                    )
                else:
                    column_names += desc_col_name

            query = f"select {column_names} from ({self.query})"

            error_cancel_status, self.layers = get_layers(
                auth_information=self.auth_information,
                layer_pre_name=self.layer_name,
                query=query,
                connection_name=self.connection_name,
                geo_column_name=self.geo_column_name,
                task=self,
            )
            return error_cancel_status
        except Exception as e:
            self.on_handle_error.emit(
                "SFConvertColumnToLayerTask run failed",
                f"Running snowflake convert column to layer task failed.\n\nExtended error information:\n{str(e)}",
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
