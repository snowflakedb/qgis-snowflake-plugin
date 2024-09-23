import typing
from qgis._core import QgsAbstractFeatureSource
from qgis.core import (
    QgsDataProvider,
    QgsFeatureIterator,
    QgsFeatureRequest,
    QgsField,
    QgsFields,
    QgsVectorDataProvider,
    Qgis,
    QgsCoordinateReferenceSystem,
    QgsRectangle,
    QgsFeature,
    QgsFeatureSink,
)
from PyQt5.QtCore import QVariant


from ..entities.sf_feature_iterator import SFFeatureIterator

from ..helpers.utils import get_authentification_information, get_qsettings

from ..managers.sf_connection_manager import SFConnectionManager


class SFVectorDataProvider(QgsVectorDataProvider):
    TYPE_CODE_DICT = {
        0: {"name": "FIXED", "qvariant_type": QVariant.Double},  # NUMBER/INT
        1: {"name": "REAL", "qvariant_type": QVariant.Double},  # REAL
        2: {"name": "TEXT", "qvariant_type": QVariant.String},  # VARCHAR/STRING
        3: {"name": "DATE", "qvariant_type": QVariant.Date},  # DATE
        4: {"name": "TIMESTAMP", "qvariant_type": QVariant.DateTime},  # TIMESTAMP
        5: {"name": "VARIANT", "qvariant_type": QVariant.String},  # VARIANT
        6: {
            "name": "TIMESTAMP_LTZ",
            "qvariant_type": QVariant.DateTime,
        },  # TIMESTAMP_LTZ
        7: {"name": "TIMESTAMP_TZ", "qvariant_type": QVariant.DateTime},  # TIMESTAMP_TZ
        8: {
            "name": "TIMESTAMP_NTZ",
            "qvariant_type": QVariant.DateTime,
        },  # TIMESTAMP_NTZ
        9: {"name": "OBJECT", "qvariant_type": QVariant.String},  # OBJECT
        10: {"name": "ARRAY", "qvariant_type": QVariant.String},  # ARRAY
        11: {"name": "BINARY", "qvariant_type": QVariant.BitArray},  # BINARY
        12: {"name": "TIME", "qvariant_type": QVariant.Time},  # TIME
        13: {"name": "BOOLEAN", "qvariant_type": QVariant.Bool},  # BOOLEAN
        14: {"name": "GEOGRAPHY", "qvariant_type": QVariant.String},  # GEOGRAPHY
        15: {"name": "GEOMETRY", "qvariant_type": QVariant.String},  # GEOMETRY
        16: {"name": "VECTOR", "qvariant_type": QVariant.Vector2D},  # VECTOR
    }

    def __init__(
        self,
        uri: str,
        providerOptions: QgsDataProvider.ProviderOptions = QgsDataProvider.ProviderOptions(),
        flags: QgsDataProvider.ReadFlags = QgsDataProvider.ReadFlags(0),
    ):
        """
        Initializes the SFVectorDataProvider class.

        Args:
            uri (str): The URI of the vector data provider. e.g. snowflake://connection_name/database/schema/table/column.
            providerOptions (QgsDataProvider.ProviderOptions, optional): Provider options. Defaults to QgsDataProvider.ProviderOptions().
            flags (QgsDataProvider.ReadFlags, optional): Read flags. Defaults to QgsDataProvider.ReadFlags(0).
        """
        print("SFVectorDataProvider.__init__")
        super().__init__(uri, providerOptions, flags)
        self.connection_manager: SFConnectionManager = (
            SFConnectionManager.get_instance()
        )
        self.settings = get_qsettings()
        self.connection_name, self.database, self.schema, self.table, self.column = (
            uri.split("://")[1].split("/")
        )
        self.auth_information = get_authentification_information(
            self.settings, self.connection_name
        )
        self.query = f"SELECT ST_ASWKB({self.column}) AS ASWKB FROM {self.database}.{self.schema}.{self.table} WHERE ASWKB IS NOT NULL"
        print(self.query)
        self._fields = None
        self._extent = QgsRectangle()
        # self._extent.setMinimal()
        self._features = {}

    def fields(self) -> QgsFields:
        print("fields in vector data provider")
        if self._fields is None:
            self.__load_fields()
        return self._fields

    def __load_fields(self):
        if self.connection_manager.get_connection(self.connection_name) is None:
            self.connection_manager.connect(self.connection_name, self.auth_information)
        cursor = self.connection_manager.execute_query(self.connection_name, self.query)

        # Create QgsFields based on Snowflake schema
        self._fields = QgsFields()
        c_description = cursor.description
        for f in c_description:
            code_type = f[1]
            field_type = self.__get_field_type_from_code_type(code_type)
            qgsField = QgsField(f[0], field_type)
            self._fields.append(qgsField)
        cursor.close()
        print(f"__load_fields = {self._fields}")

    # def getFeatures(
    #     self, request: QgsFeatureRequest = QgsFeatureRequest()
    # ) -> QgsFeatureIterator:
    #     print("getFeatures in vector data provider")
    #     # Filter features based on the request if needed
    #     if self.connection_manager.get_connection(self.connection_name) is None:
    #         self.connection_manager.connect(self.connection_name, self.auth_information)
    #     cursor = self.connection_manager.execute_query(self.connection_name, self.query)
    #     print(f"getFeatures = {cursor}")
    #     print(f"getFeatures = {self.query}")

    #     return SFFeatureIterator(cursor=cursor, fields=self.fields())

    def name(self) -> str:
        """Return the name of the provider."""
        return "snowflake_vector_data_provider"

    def featureCount(self) -> int:
        print("featureCount in vector data provider")
        if self.connection_manager.get_connection(self.connection_name) is None:
            self.connection_manager.connect(self.connection_name, self.auth_information)
        query = f"SELECT count({self.column}) FROM {self.database}.{self.schema}.{self.table}"
        cursor = self.connection_manager.execute_query(self.connection_name, query)
        count = cursor.fetchall()[0][0]
        print(f"feature count = {count}")
        return count

    def isValid(self) -> bool:
        print("isValid in vector data provider")
        connection_exists = (
            self.connection_manager.get_connection(self.connection_name) is not None
        )
        print(f"connection_exists = {connection_exists}")
        return connection_exists

    def featureSource(self) -> QgsAbstractFeatureSource:
        print("featureSource in vector data provider")
        from ..entities.sf_feature_source import SFFeatureSource

        return SFFeatureSource(self)

    def crs(self) -> QgsCoordinateReferenceSystem:
        print("crs in vector data provider")
        return QgsCoordinateReferenceSystem()

    def wkbType(self) -> Qgis.WkbType:
        print("wkbType in vector data provider")
        return Qgis.WkbType(2)

    def __get_field_type_from_code_type(self, code_type: int) -> QVariant.Type:
        """
        Returns the QVariant.Type corresponding to the given code_type.

        Parameters:
            code_type (int): The code_type to get the QVariant.Type for.

        Returns:
            QVariant.Type: The QVariant.Type corresponding to the given code_type. If the code_type is not found in the TYPE_CODE_DICT, QVariant.String is returned.
        """
        if code_type in self.TYPE_CODE_DICT:
            return self.TYPE_CODE_DICT[code_type]["qvariant_type"]
        else:
            return QVariant.String

    def extent(self) -> QgsRectangle:
        """Return the extent of all geometries from the provider."""
        print("extent in vector data provider")
        if not self._features:
            return QgsRectangle()  # Return an empty rectangle if no features
        print(self._features)

        # Initialize the bounding box with the first feature's geometry
        bbox = self._features[0].geometry().boundingBox()

        # Combine extents of all features
        for feature in self._features:
            bbox.combineExtentWith(feature.geometry().boundingBox())

        return bbox

    # next_feature_id = 1

    # def addFeatures(
    #     self,
    #     flist: typing.Iterable[QgsFeature],
    #     flags: typing.Union[QgsFeatureSink.Flags, QgsFeatureSink.Flag] = ...,
    # ) -> typing.Tuple[bool, typing.List[QgsFeature]]:
    #     print("addFeatures")
    #     added = False
    #     f_added = []
    #     for f in flist:
    #         print(f.geometry().wkbType())
    #         if f.hasGeometry() and (f.geometry().wkbType() != self.wkbType()):
    #             return added, f_added

    #     for f in flist:
    #         _f = QgsFeature(self.fields())
    #         _f.setGeometry(f.geometry())
    #         attrs = [None for i in range(_f.fields().count())]
    #         for i in range(min(len(attrs), len(f.attributes()))):
    #             attrs[i] = f.attributes()[i]
    #         _f.setAttributes(attrs)
    #         _f.setId(self.next_feature_id)
    #         self._features[self.next_feature_id] = _f
    #         self.next_feature_id += 1
    #         added = True
    #         f_added.append(_f)

    #         # if self._spatialindex is not None:
    #         #     self._spatialindex.insertFeature(_f)

    #     if len(f_added):
    #         self.clearMinMaxCache()
    #         self.updateExtents()

    #     return added, f_added
