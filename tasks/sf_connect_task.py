import typing

from ..helpers.data_base import get_features_iterator
from ..helpers.utils import get_authentification_information, get_qsettings
from qgis.core import QgsTask
from qgis.PyQt.QtCore import pyqtSignal
from qgis.PyQt.QtCore import QVariant
from qgis.PyQt.QtGui import QStandardItem


class SFConnectTask(QgsTask):
    on_handle_error = pyqtSignal(str, str)
    on_data_ready = pyqtSignal(list)

    def __init__(self, connection_name: str) -> None:
        """
        Initializes the SFConnectTask object.

        Args:
            connection_name (str): The name of the Snowflake connection.

        Raises:
            Exception: If the initialization fails.
        """
        try:
            super().__init__("Snowflake Connection Task", QgsTask.CanCancel)
            self.settings = get_qsettings()
            self.auth_information = get_authentification_information(
                self.settings, connection_name
            )
            self.query = f"""
            SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COMMENT, COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_CATALOG = '{self.auth_information["database"].upper()}'
            AND DATA_TYPE in ('GEOGRAPHY', 'GEOMETRY')
            ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
            """
            self.connection_name = connection_name
        except Exception as e:
            self.on_handle_error.emit(
                "SFConnectTask init failed",
                f"Initiating sf connect task failed.\n\nExtended error information:\n{str(e)}",
            )

    def run(self) -> bool:
        """
        Executes the SFConnectTask.

        Returns:
            bool: True if the task is executed successfully, False otherwise.
        """
        try:
            feature_iterator = get_features_iterator(
                self.auth_information, self.query, self.connection_name
            )
            current_schema = None
            current_item = None
            self.rows_items: typing.List[QStandardItem] = []
            for feat in feature_iterator:
                if current_schema != feat.attribute("TABLE_SCHEMA"):
                    current_schema = feat.attribute("TABLE_SCHEMA")
                    current_item = QStandardItem(feat.attribute("TABLE_SCHEMA"))
                    self.rows_items.append(current_item)
                srid = ""
                if feat.attribute("DATA_TYPE") == "GEOGRAPHY":
                    srid = "4326"

                if (
                    isinstance(feat.attribute("COMMENT"), QVariant)
                    and feat.attribute("COMMENT").isNull()
                ) or feat.attribute("COMMENT") is None:
                    standard_item_comment = QStandardItem()
                else:
                    standard_item_comment = QStandardItem(feat.attribute("COMMENT"))

                row_items = [
                    QStandardItem(feat.attribute("TABLE_SCHEMA")),
                    QStandardItem(feat.attribute("TABLE_NAME")),
                    standard_item_comment,
                    QStandardItem(feat.attribute("COLUMN_NAME")),
                    QStandardItem(feat.attribute("DATA_TYPE")),
                    QStandardItem(srid),
                ]

                if current_item is not None:
                    current_item.appendRow(row_items)

            feature_iterator.close()
            return True
        except Exception as e:
            self.on_handle_error.emit(
                "SFConnectTask run failed",
                f"Running sf connect task failed.\n\nExtended error information:\n{str(e)}",
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
            self.on_data_ready.emit(self.rows_items)
