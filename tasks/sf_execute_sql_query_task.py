import typing

from ..helpers.data_base import get_features_iterator
from ..helpers.utils import get_qsettings, get_authentification_information
from qgis.core import QgsTask
from qgis.PyQt.QtCore import pyqtSignal


class SFExecuteSQLQueryTask(QgsTask):
    on_handle_error = pyqtSignal(str, str)
    on_data_ready = pyqtSignal(list, list)

    def __init__(
        self,
        context_information: typing.Dict[str, typing.Union[str, None]],
        query: str,
        limit: typing.Union[int, None] = None,
    ):
        try:
            self.query = query
            super().__init__(
                f"Snowflake Execute Query: {self.query}",
                QgsTask.CanCancel,
            )
            self.settings = get_qsettings()
            self.auth_information = get_authentification_information(
                self.settings, context_information["connection_name"]
            )
            self.database_name = self.auth_information["database"]
            self.context_information = context_information
            self.limit = limit
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
            feature_iterator = get_features_iterator(
                auth_information=self.auth_information,
                query=self.query,
                connection_name=self.context_information["connection_name"],
                context_information=self.context_information,
            )
            self.columns_descriptions = feature_iterator.cursor.description

            self.features = []
            for index, feat in enumerate(feature_iterator):
                if self.limit is not None and index >= self.limit:
                    break
                if self.isCanceled():
                    return False
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
                self.columns_descriptions,
                self.features,
            )
