# standard
from __future__ import (
    annotations,  # used to manage type annotation for method that return Self in Python < 3.11
)

from typing import Any, Callable

from PyQt5.QtCore import QDate, QDateTime, QMetaType, QTime

# PyQGIS
from qgis.core import (
    QgsAbstractFeatureIterator,
    QgsCoordinateTransform,
    QgsCsException,
    QgsFeature,
    QgsFeatureRequest,
    QgsGeometry,
)


# plugin
# from ..providers.sf_provider import SFProvider
from ..providers.sf_feature_source import SFFeatureSource


class SFFeatureIterator(QgsAbstractFeatureIterator):
    def __init__(
        self,
        source: SFFeatureSource,
        request: QgsFeatureRequest,
    ):
        """Constructor"""
        super().__init__(request)
        # import is crashing
        self._provider = source.get_provider()
        # self._settings = PlgOptionsManager.get_plg_settings()
        # self.log = PlgLogger().log

        self._request = request if request is not None else QgsFeatureRequest()
        self._transform = QgsCoordinateTransform()

        if (
            self._request.destinationCrs().isValid()
            and self._request.destinationCrs() != source._provider.crs()
        ):
            self._transform = QgsCoordinateTransform(
                source._provider.crs(),
                self._request.destinationCrs(),
                self._request.transformContext(),
            )

        try:
            filter_rect = self.filterRectToSourceCrs(self._transform)
        except QgsCsException:
            self.close()
            return

        if not self._provider.isValid():
            return

        geom_column = self._provider.get_geometry_column()

        # Check if some attributes which contain date or time
        # In that case, they need to be converted to a Qt type
        # to be correctly handled by QGIS.
        attributes_conversion_functions: dict[QMetaType, Callable[[Any], Any]] = {
            QMetaType.QDate: QDate,
            QMetaType.QTime: QTime,
            QMetaType.QDateTime: QDateTime,
        }
        # By default, do not convert
        self._attributes_converters = {}
        for idx in range(len(self._provider.fields())):
            self._attributes_converters[idx] = lambda x: x

        # Check if some fields need to be converted
        # If that's the case, enable the _attributes_need_conversion flag
        # and assign the converter with the attributes index.
        self._attributes_need_conversion = False
        for field_type, converter in attributes_conversion_functions.items():
            for index in self._provider.get_field_index_by_type(field_type):
                self._attributes_need_conversion = True
                self._attributes_converters[index] = converter

        # Create the list of fields that need to be retrieved
        self._request_sub_attributes = (
            self._request.flags() & QgsFeatureRequest.Flag.SubsetOfAttributes
        )
        if self._request_sub_attributes and not self._provider.subsetString():
            idx_required = [idx for idx in self._request.subsetOfAttributes()]

            # The primary key column must be added if it is not present in the field list.
            if (
                self._provider.primary_key() != -1
                and self._provider.primary_key() not in idx_required
            ):
                idx_required.append(self._provider.primary_key())

            list_field_names = [
                self._provider.fields()[idx].name() for idx in idx_required
            ]
        else:
            list_field_names = [field.name() for field in self._provider.fields()]

        if len(list_field_names) > 0:
            fields_name_for_query = '"' + '", "'.join(list_field_names) + '"'
        else:
            fields_name_for_query = ""

        if fields_name_for_query:
            fields_name_for_query += ","
        self.index_geom_column = len(list_field_names)

        # Create fid/fids list
        feature_id_list = None
        if (
            self._request.filterType() == QgsFeatureRequest.FilterFid
            or self._request.filterType() == QgsFeatureRequest.FilterFids
        ):
            feature_id_list = (
                [self._request.filterFid()]
                if self._request.filterType() == QgsFeatureRequest.FilterFid
                else self._request.filterFids()
            )

        where_clause_list = []
        if feature_id_list:
            if self._provider.primary_key() == -1:
                feature_clause = f"index in {tuple(feature_id_list)}"
            else:
                primary_key_name = list_field_names[self._provider.primary_key()]
                feature_clause = f"{primary_key_name} in {tuple(feature_id_list)}"

            where_clause_list.append(feature_clause)

        # Apply the filter expression
        if self._request.filterType() == QgsFeatureRequest.FilterExpression:
            # A provider is supposed to implement a QgsSqlExpressionCompiler
            # in order to handle expression. However, this class is not
            # available in the Python bindings.
            # Try to use the expression as is. It should work in most
            # cases for simple expression.
            expression = self._request.filterExpression().expression()
            if expression:
                try:
                    self._provider.con().sql(
                        f"SELECT count(*)"
                        f" FROM {self._provider._from_clause}"
                        f" WHERE {expression}"
                        " LIMIT 0"
                    )
                    self._expression = expression
                    where_clause_list.append(expression)
                except Exception:
                    # PlgLogger.log(
                    #     f"Duckdb provider does not handle expression: {expression}",
                    #     log_level=2,
                    #     duration=5,
                    #     push=False,
                    # )
                    self._expression = ""
            else:
                self._expression = ""

        # Apply the subset string filter
        if self._provider.subsetString():
            subset_clause = self._provider.subsetString().replace('"', "")
            where_clause_list.append(subset_clause)

        # Apply the geometry filter
        # if not filter_rect.isNull():
        #     filter_geom_clause = (
        #         f"ST_INTERSECTS({geom_column}, "
        #         f"ST_GEOMETRYFROMWKT('{filter_rect.asWktPolygon()}'))"
        #     )
        #     where_clause_list.append(filter_geom_clause)

        # build the complete where clause
        where_clause = ""
        if where_clause_list:
            where_clause = f"where {where_clause_list[0]}"
            if len(where_clause_list) > 1:
                for clause in where_clause_list[1:]:
                    where_clause += f" and {clause}"

        geom_query = f"ST_ASWKB({geom_column}), {geom_column}, "
        self._request_no_geometry = (
            self._request.flags() & QgsFeatureRequest.Flag.NoGeometry
        )
        if self._request_no_geometry:
            geom_query = ""

        if self._provider.primary_key() == -1:
            # if self._provider._table_name and self._provider.is_view():
            #     index_word = "ROW_NUMBER() OVER (order by 1) as index "
            # else:
            #     index_word = "rowid"
            # index = f"{index_word}+1 as index "
            index = "ROW_NUMBER() OVER (order by 1) as index "
            # order_by = "index"
        else:
            index = self._provider._fields[self._provider.primary_key()].name()
            # order_by = index

        final_query = (
            "select * from ("
            f"select {fields_name_for_query} "
            f"{geom_query} {index} "
            f"from {self._provider._from_clause}) "
            f"{where_clause} "
            # f"order by {order_by}"
        )

        # if self._settings.debug_mode:
        #     self.log(
        #         message="feature iterator execute query: {}".format(final_query),
        #         log_level=4,  # 4 = info
        #         push=False,
        #     )

        print(final_query)

        context_information = {
            "connection_name": self._provider._connection_name,
            "schema_name": self._provider._schema_name,
            "table_name": self._provider._table_name,
        }

        self._result = self._provider.connection_manager.execute_query(
            connection_name=self._provider._connection_name,
            query=final_query,
            context_information=context_information,
        )
        # self._result = self._provider.con().execute(final_query)
        self._index = 0

    def fetchFeature(self, f: QgsFeature) -> bool:
        """fetch next feature, return true on success

        :param f: Next feature
        :type f: QgsFeature
        :return: True if success
        :rtype: bool
        """
        next_result = self._result.fetchone()

        if not next_result or not self._provider.isValid():
            f.setValid(False)
            return False

        f.setFields(self._provider.fields())
        f.setValid(True)

        if not self._request_no_geometry:
            geometry = QgsGeometry()
            geometry.fromWkb(next_result[self.index_geom_column])
            f.setGeometry(geometry)
            self.geometryToDestinationCrs(f, self._transform)

        f.setId(next_result[-1])

        # set attributes
        if self._attributes_need_conversion:
            # Some attributes need to be converted
            if self._request_sub_attributes:
                for idx, attr_idx in enumerate(self._request.subsetOfAttributes()):
                    attribute = self._attributes_converters[idx](next_result[idx])
                    f.setAttribute(attr_idx, attribute)
            else:
                for idx, attribute in enumerate(next_result[: self.index_geom_column]):
                    converted_attribute = self._attributes_converters[idx](attribute)
                    f.setAttribute(idx, converted_attribute)
        else:
            # No need for conversion, the values can directly be used
            if self._request_sub_attributes:
                for idx, attr_idx in enumerate(self._request.subsetOfAttributes()):
                    f.setAttribute(attr_idx, next_result[idx])
            else:
                f.setAttributes(list(next_result[: self.index_geom_column]))

        self._index += 1
        return True

    def nextFeatureFilterExpression(self, f: QgsFeature) -> bool:
        if not self._expression:
            return super().nextFeatureFilterExpression(f)
        else:
            return self.fetchFeature(f)

    def __iter__(self) -> SFFeatureIterator:
        """Returns self as an iterator object"""
        self._index = 0
        return self

    def __next__(self) -> QgsFeature:
        """Returns the next value till current is lower than high"""
        f = QgsFeature()
        if not self.nextFeature(f):
            raise StopIteration
        else:
            return f

    def rewind(self) -> bool:
        """reset the iterator to the starting position"""
        # virtual bool rewind() = 0;
        if self._index < 0:
            return False
        self._index = 0
        return True

    def close(self) -> bool:
        """end of iterating: free the resources / lock"""
        # virtual bool close() = 0;
        self._index = -1
        return True
