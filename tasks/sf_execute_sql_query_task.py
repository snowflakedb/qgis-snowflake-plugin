from ..helpers.utils import get_qsettings, get_authentification_information
from ..providers.sf_data_source_provider import SFDataProvider
from qgis.core import (
    QgsFeature,
    QgsField,
    QgsGeometry,
    QgsMapLayer,
    QgsProject,
    QgsTask,
    QgsVectorLayer,
)
from qgis.PyQt.QtCore import pyqtSignal, QVariant
from typing import Dict, Union, List


class SFExecuteSQLQueryTask(QgsTask):
    on_handle_error = pyqtSignal(str, str)
    on_data_ready = pyqtSignal(list, list)

    def __init__(self, connection_name: str, query: str):
        try:
            self.query = query
            super().__init__(
                f"Snowflake Execute Query: {self.query}",
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
                "SFExecuteSQLQueryTask init failed",
                f"Initializing snowflake execute sql query task failed.\n\nExtended error information:\n{str(e)}",
            )

    def run(self) -> bool:
        """
        Executes the task to convert a Snowflake column to a layer.

        Returns:
            bool: True if the task is executed successfully, False otherwise.
        """
        try:
            sf_data_provider = SFDataProvider(self.auth_information)
            sf_data_provider.load_data(self.query, self.connection_name)
            feature_iterator = sf_data_provider.get_feature_iterator()
            self.column_names = [
                desc[0] for desc in feature_iterator.cursor.description
            ]

            self.features = []
            for feat in feature_iterator:
                self.features.append(feat)

            feature_iterator.close()

            return True
        except Exception as e:
            self.on_handle_error.emit(
                "SFExecuteSQLQueryTask run failed",
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
            self.on_data_ready.emit(
                self.column_names,
                self.features,
            )
