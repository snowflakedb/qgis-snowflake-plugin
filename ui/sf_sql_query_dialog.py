import json
import traceback
from PyQt5.QtWidgets import QWidget
from qgis.PyQt.QtGui import QStandardItemModel, QStandardItem
from qgis.PyQt import uic
from qgis.PyQt.QtCore import pyqtSignal, QVariant
from qgis.PyQt.QtWidgets import QDialog, QMessageBox
from qgis.core import QgsApplication, QgsFeature, QgsGeometry, QgsPointXY
import os
import typing

from ..helpers.data_base import checks_sql_query_exceeds_size
from ..helpers.messages import get_proceed_cancel_message_box

from ..tasks.sf_convert_sql_query_to_layer_task import SFConvertSQLQueryToLayerTask

from ..tasks.sf_execute_sql_query_task import SFExecuteSQLQueryTask

from ..helpers.utils import get_qsettings
from snowflake.connector.cursor import ResultMetadata


FORM_CLASS_SFCS, _ = uic.loadUiType(
    os.path.join(os.path.dirname(__file__), "sf_sql_query_dialog.ui")
)


class SFSQLQueryDialog(QDialog, FORM_CLASS_SFCS):
    update_connections_signal = pyqtSignal()

    def __init__(
        self,
        context_information: typing.Dict[str, typing.Union[str, None]] = None,
        parent: typing.Optional[QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self.setupUi(self)
        self.context_information = context_information
        self.settings = get_qsettings()
        self.temp_deactivated_options()
        self.mExecuteButton.setEnabled(True)
        self.mExecuteButton.clicked.connect(self.on_execute_button_clicked)
        self.mLoadLayerPushButton.clicked.connect(
            self.on_load_layer_push_button_clicked
        )
        self.mClearButton.clicked.connect(self.on_clear_button_clicked)

    def on_clear_button_clicked(self):
        self.model = QStandardItemModel()
        self.mQueryResultsTableView.setModel(self.model)
        self.mSqlErrorText.clear()
        self.mGeometryColumnCheckBox.setChecked(False)

    def on_load_layer_push_button_clicked(self):
        try:
            if self.mGeometryColumnCheckBox.isChecked():
                query_without_semicolon = (
                    self.mSqlErrorText.text()[:-1]
                    if self.mSqlErrorText.text().endswith(";")
                    else self.mSqlErrorText.text()
                )
                context_information_with_sql_query = self.context_information.copy()
                context_information_with_sql_query["sql_query"] = (
                    query_without_semicolon
                )
                table_exceeds_size = checks_sql_query_exceeds_size(
                    context_information=context_information_with_sql_query,
                )

                if table_exceeds_size:
                    response = get_proceed_cancel_message_box(
                        title="Resultset is too large",
                        text=(
                            "You are trying to load more than 50 thousand rows. We "
                            'recommend you to apply a limit clause (e.g. "LIMIT 50000") to your query. '
                            'If you click "Proceed" your query will execute as is.'
                        ),
                    )
                    if response != QMessageBox.Cancel:
                        geo_column_name = self.mGeometryColumnComboBox.currentText()
                        self.context_information["geo_column_name"] = geo_column_name
                        sf_convert_sql_query_to_layer_task = (
                            SFConvertSQLQueryToLayerTask(
                                query=query_without_semicolon,
                                layer_name=self.mLayerNameLineEdit.text(),
                                context_information=self.context_information,
                            )
                        )

                        sf_convert_sql_query_to_layer_task.on_handle_error.connect(
                            self.on_handle_error
                        )
                        sf_convert_sql_query_to_layer_task.on_success.connect(
                            self.on_success
                        )
                        QgsApplication.taskManager().addTask(
                            sf_convert_sql_query_to_layer_task
                        )
        except Exception as e:
            QMessageBox.information(
                None,
                "SFSQLQueryDialog - Add Layer from SQL Query",
                f"Adding Layer from SQL Query failed.\n\nExtended error information:\n{str(e)}",
            )

    def on_success(self):
        self.close()

    def temp_deactivated_options(self) -> None:
        self.mProgressBar.setVisible(False)
        self.mPkColumnsCheckBox.setVisible(False)
        self.mPkColumnsComboBox.setVisible(False)
        self.mFilterLabel.setVisible(False)
        self.mFilterLineEdit.setVisible(False)
        self.mFilterToolButton.setVisible(False)
        self.mAvoidSelectingAsFeatureIdCheckBox.setVisible(False)
        self.mStopButton.setVisible(False)
        self.mStatusLabel.setVisible(False)

    def on_execute_button_clicked(self):
        try:
            snowflake_covert_column_to_layer_task = SFExecuteSQLQueryTask(
                context_information=self.context_information,
                query=self.mSqlErrorText.text(),
                limit=100,
            )
            snowflake_covert_column_to_layer_task.on_handle_error.connect(
                self.on_handle_error
            )
            snowflake_covert_column_to_layer_task.on_data_ready.connect(
                self.on_data_ready
            )
            QgsApplication.taskManager().addTask(snowflake_covert_column_to_layer_task)
        except Exception as e:
            QMessageBox.information(
                None,
                "SFSQLQueryDialog - Execute SQL Query",
                f"Executing SQL Query failed.\n\nExtended error information:\n{str(e)}",
            )

    def on_data_ready(
        self,
        result_meta_data_list: typing.List[ResultMetadata],
        features: typing.List[QgsFeature],
    ) -> None:
        try:
            self.model = QStandardItemModel()
            self.mQueryResultsTableView.setModel(self.model)
            self.mGeometryColumnComboBox.clear()
            col_names = []
            for result_meta_data in result_meta_data_list:
                col_name = result_meta_data[0]
                col_names.append(col_name)
                self.mGeometryColumnComboBox.addItem(col_name)
            self.model.setHorizontalHeaderLabels(col_names)
            for feat in features:
                row_items = []
                is_null = False
                for index, attr in enumerate(feat.attributes()):
                    if isinstance(attr, QVariant):
                        if attr.isNull():
                            is_null = True
                            continue
                    if attr is None:
                        is_null = True
                        continue

                    if result_meta_data_list[index].type_code in [14, 15]:
                        # Parse the JSON string
                        geojson = json.loads(attr)
                        if "type" in geojson or "coordinates" in geojson:
                            # Extract the geometry type and coordinates
                            geom_type = geojson["type"]
                            coordinates = geojson["coordinates"]

                            # Create QgsGeometry based on the geometry type
                            if geom_type == "Point":
                                point = QgsPointXY(coordinates[0], coordinates[1])
                                geom = QgsGeometry.fromPointXY(point)
                            elif geom_type == "LineString":
                                line = [QgsPointXY(xy[0], xy[1]) for xy in coordinates]
                                geom = QgsGeometry.fromPolylineXY(line)
                            elif geom_type == "Polygon":
                                outer_ring = [
                                    QgsPointXY(xy[0], xy[1]) for xy in coordinates[0]
                                ]
                                geom = QgsGeometry.fromPolygonXY([outer_ring])
                            elif geom_type == "MultiPoint":
                                points = [
                                    QgsPointXY(xy[0], xy[1]) for xy in coordinates
                                ]
                                geom = QgsGeometry.fromMultiPointXY(points)
                            elif geom_type == "MultiLineString":
                                lines = [
                                    [QgsPointXY(xy[0], xy[1]) for xy in line]
                                    for line in coordinates
                                ]
                                geom = QgsGeometry.fromMultiPolylineXY(lines)
                            elif geom_type == "MultiPolygon":
                                polygons = []
                                for ring in coordinates:
                                    for xy in ring[0]:
                                        polygons.append([QgsPointXY(xy[0], xy[1])])
                                geom = QgsGeometry.fromMultiPolygonXY([polygons])
                            elif geom_type == "GeometryCollection":
                                geometries = []
                                for geom_data in coordinates:
                                    geom_type = geom_data["type"]
                                    geom_coords = geom_data["coordinates"]
                                    if geom_type == "Point":
                                        point = QgsPointXY(
                                            geom_coords[0], geom_coords[1]
                                        )
                                        geometries.append(
                                            QgsGeometry.fromPointXY(point)
                                        )
                                    elif geom_type == "LineString":
                                        line = [
                                            QgsPointXY(xy[0], xy[1])
                                            for xy in geom_coords
                                        ]
                                        geometries.append(
                                            QgsGeometry.fromPolylineXY(line)
                                        )
                                    elif geom_type == "Polygon":
                                        outer_ring = [
                                            QgsPointXY(xy[0], xy[1])
                                            for xy in geom_coords[0]
                                        ]
                                        geometries.append(
                                            QgsGeometry.fromPolygonXY(outer_ring)
                                        )
                                geom = QgsGeometry.collect(geometries)
                            else:
                                geom = None

                            # Convert geometry to WKB
                            if geom is None:
                                q_standard_item = QStandardItem("")
                            else:
                                wkb = geom.asWkt()
                                q_standard_item = QStandardItem(wkb)
                        else:
                            q_standard_item = QStandardItem("")
                    else:
                        q_standard_item = QStandardItem(str(attr))

                    row_items.append(q_standard_item)
                if not is_null:
                    self.model.appendRow(row_items)

        except Exception as e:
            stack_trace = traceback.format_exc()
            QMessageBox.information(
                None,
                "SFSQLQueryDialog - Data Ready",
                f"Data Ready failed.\n\nExtended error information:\n{str(e)}\n{stack_trace}",
            )

    def on_handle_error(self, title, message):
        QMessageBox.information(None, title, message)
