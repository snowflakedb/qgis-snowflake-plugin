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
from typing import Dict, Union


class SFConvertColumnToLayerTask(QgsTask):
    on_handle_error = pyqtSignal(str, str)

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
            sf_data_provider = SFDataProvider(self.auth_information)
            query = f"SELECT ST_ASWKB({self.column}) FROM {self.database_name}.{self.schema}.{self.table}"
            sf_data_provider.load_data(query, self.connection_name)
            feature_iterator = sf_data_provider.get_feature_iterator()

            layer_dict: Dict[str, Dict[str, Union[list, QgsVectorLayer]]] = {}
            for feat in feature_iterator:
                column_0 = feat.attribute(0)
                if isinstance(column_0, QVariant):
                    if column_0.isNull():
                        continue
                if column_0 is None:
                    continue
                qgsGeometry = QgsGeometry()
                qgsGeometry.fromWkb(column_0)
                geometry_type_obj = qgsGeometry.wkbType()
                geometry_type = str(geometry_type_obj).split(".")[1]
                if geometry_type == "Unknown":
                    continue
                feature = QgsFeature()
                if geometry_type not in layer_dict:
                    if geometry_type.lower() not in [
                        "point",
                        "multipoint",
                        "linestring",
                        "multilinestring",
                        "polygon",
                        "multipolygon",
                    ]:
                        continue
                    layer_name = f"{self.database_name}.{self.schema}.{self.table}_{self.column}_{geometry_type}"
                    layer_dict[geometry_type] = {
                        "features": [],
                        "layer": QgsVectorLayer(
                            f"{geometry_type}?crs=epsg:4326", layer_name, "memory"
                        ),
                    }
                feature.setGeometry(qgsGeometry)
                layer_dict[geometry_type]["features"].append(feature)

            feature_iterator.close()

            self._add_features_attributes_to_layer(layer_dict)
            return True
        except Exception as e:
            self.on_handle_error.emit(
                "SFConvertColumnToLayerTask run failed",
                f"Running snowflake convert column to layer task failed.\n\nExtended error information:\n{str(e)}",
            )
            return False

    def _add_features_attributes_to_layer(
        self, layer_dict: Dict[str, Dict[str, Union[list, QgsVectorLayer]]]
    ):
        """
        Adds features and attributes to the specified layers.

        Args:
            layer_dict (Dict[str, Dict[str, Union[list, QgsVectorLayer]]]): A dictionary containing layer information.
                The keys are layer types and the values are dictionaries containing the layer and features.

        Returns:
            None
        """
        self.layers = []

        for layer_type in layer_dict:
            layer_fields = []
            layer_fields.append(QgsField("Name", QVariant.String))
            layer_dict[layer_type]["layer"].startEditing()
            layer_dict[layer_type]["layer"].dataProvider().addAttributes(layer_fields)
            layer_dict[layer_type]["layer"].updateFields()

            layer_dict[layer_type]["layer"].dataProvider().addFeatures(
                layer_dict[layer_type]["features"]
            )
            layer_dict[layer_type]["layer"].commitChanges()
            self.layers.append(layer_dict[layer_type]["layer"])

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
