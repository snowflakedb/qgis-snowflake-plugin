from qgis.core import (
    QgsVectorDataProvider,
    QgsFeature,
    QgsFeatureRequest,
    QgsGeometry,
    QgsFields,
    QgsField,
    QgsFeatureIterator,
    Qgis,
    QgsCoordinateReferenceSystem,
    QgsRectangle,
    QgsAbstractFeatureSource,
)
from PyQt5.QtCore import QVariant

from ..helpers.utils import get_authentification_information, get_qsettings

from ..managers.sf_connection_manager import (
    SFConnectionManager,
)  # Use the appropriate database library for your needs


class CustomFeatureIterator(QgsFeatureIterator):
    def __init__(self, features):
        super().__init__()
        self.features = iter(features)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self.features)
        except StopIteration:
            raise StopIteration()


class CustomFeatureSource(QgsAbstractFeatureSource):
    def __init__(self, provider: QgsVectorDataProvider):
        super().__init__()
        self.provider = provider
        self.loading_features = False

    # def getFeatures(self, request: QgsFeatureRequest) -> QgsFeatureIterator:
    #     print("getFeatures in custom feature source")
    #     # if request is not None:
    #     #     print("Request Filter: %s", request.filter())
    #     #     print("Request Attributes: %s", request.attributeNames())
    #     #     print(
    #     #         "Request Geometry: %s",
    #     #         request.geometry().asWkt() if request.geometry() else "No geometry",
    #     #     )
    #     #     print("Has Subset String: %s", request.hasSubsetString())
    #     #     print("Has Filters: %s", request.hasFilters())
    #     return self.provider.getFeatures(request)
    def getFeatures(self, request: QgsFeatureRequest) -> QgsFeatureIterator:
        print("getFeatures in vector data provider test")
        # print("getFeatures in vector data provider test")
        # print(f"Request Filter: {request.filter()}")
        # print(f"Request Attributes: {request.attributeNames()}")

        # Check if there are already features in memory

        if not self.loading_features:
            self.loading_features = True
            if len(self.provider.features) > 0:
                print("Returning cached features.")
                return CustomFeatureIterator(self.provider.features)

            # Execute query
            cursor = self.provider.connection_manager.execute_query(
                self.provider.connection_name, self.provider.query
            )

            # Process features from the cursor
            while True:
                current_feature = cursor.fetchone()
                if current_feature is None:
                    break

                try:
                    column_0 = current_feature[0]
                    print("Processing geometry...")

                    # Create QgsGeometry from WKB
                    qgsGeometry = QgsGeometry()
                    qgsGeometry.fromWkb(column_0)
                    if qgsGeometry.isEmpty():
                        print("Geometry is empty; skipping feature.")
                        continue

                    geometry_type = qgsGeometry.wkbType()
                    print(f"Geometry Type: {str(geometry_type)}")

                    # Create QgsFeature and set its geometry
                    feature = QgsFeature()
                    feature.setGeometry(qgsGeometry)

                    # Append the feature to the list
                    self.provider.features.append(feature)
                except Exception as e:
                    print(f"Error processing feature: {e}")

            # Create and return a QgsFeatureIterator
            print(f"Total features fetched: {len(self.provider.features)}")
            self.loading_features = False
        return CustomFeatureIterator(self.provider.features)

    def crs(self) -> QgsCoordinateReferenceSystem:
        return self.provider.crs()

    def extent(self) -> QgsRectangle:
        return self.provider.extent()

    def wkbType(self) -> Qgis.WkbType:
        return self.provider.wkbType()


class SFVectorDataProviderTest(QgsVectorDataProvider):
    def __init__(
        self,
        uri: str,
    ):
        print("SFVectorDataProviderTest")
        print(uri)
        super().__init__()
        # self.database_path = database_path
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
        self.query = f"SELECT ST_ASWKB({self.column}) AS ASWKB FROM {self.database}.{self.schema}.{self.table} WHERE ASWKB IS NOT NULL limit 10"
        # self.conn = sqlite3.connect(self.database_path)  # Connect to the database
        # self.cursor = self.conn.cursor()

        self._fields = None
        self.features = []
        if self.connection_manager.get_connection(self.connection_name) is None:
            self.connection_manager.connect(self.connection_name, self.auth_information)

    def fields(self) -> QgsFields:
        # return self.fields
        print("fields in vector data provider test")
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

    def featureCount(self) -> int:
        print("featureCount in vector data provider")
        if self.connection_manager.get_connection(self.connection_name) is None:
            self.connection_manager.connect(self.connection_name, self.auth_information)
        query = f"SELECT count({self.column}) FROM {self.database}.{self.schema}.{self.table} limit 10"
        cursor = self.connection_manager.execute_query(self.connection_name, query)
        count = cursor.fetchall()[0][0]
        print(f"feature count = {count}")
        return count

    # def getFeatures(self, request: QgsFeatureRequest) -> QgsFeatureIterator:
    #     print("getFeatures in vector data provider test")
    #     # print("getFeatures in vector data provider test")
    #     # print(f"Request Filter: {request.filter()}")
    #     # print(f"Request Attributes: {request.attributeNames()}")

    #     # Check if there are already features in memory

    #     if len(self.features) > 0:
    #         print("Returning cached features.")
    #         return CustomFeatureIterator(self.features)

    #     # Execute query
    #     cursor = self.connection_manager.execute_query(self.connection_name, self.query)

    #     # Process features from the cursor
    #     while True:
    #         current_feature = cursor.fetchone()
    #         if current_feature is None:
    #             break

    #         try:
    #             column_0 = current_feature[0]
    #             print("Processing geometry...")

    #             # Create QgsGeometry from WKB
    #             qgsGeometry = QgsGeometry()
    #             qgsGeometry.fromWkb(column_0)
    #             if qgsGeometry.isEmpty():
    #                 print("Geometry is empty; skipping feature.")
    #                 continue

    #             geometry_type = qgsGeometry.wkbType()
    #             print(f"Geometry Type: {str(geometry_type)}")

    #             # Create QgsFeature and set its geometry
    #             feature = QgsFeature()
    #             feature.setGeometry(qgsGeometry)

    #             # Append the feature to the list
    #             self.features.append(feature)
    #         except Exception as e:
    #             print(f"Error processing feature: {e}")

    #     # Create and return a QgsFeatureIterator
    #     print(f"Total features fetched: {len(self.features)}")
    #     return CustomFeatureIterator(self.features)

    def capabilities(self) -> QgsVectorDataProvider.Capabilities:
        return (
            QgsVectorDataProvider.AddFeatures
            | QgsVectorDataProvider.DeleteFeatures
            | QgsVectorDataProvider.ChangeGeometries
        )

    # def __del__(self):
    #     self.conn.close()

    def name(self) -> str:
        """Return the name of the provider."""
        return "snowflake_vector_data_provider"

    def isValid(self) -> bool:
        # Check if the connection to the database is valid
        return self.connection_manager.get_connection(self.connection_name) is not None

    def wkbType(self) -> Qgis.WkbType:
        # Specify the WKB type of the geometries
        return Qgis.WkbType.Point

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

    def __get_field_type_from_code_type(self, code_type: int) -> QVariant.Type:
        print("__get_field_type_from_code_type in vector data provider test")
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

    def crs(self) -> QgsCoordinateReferenceSystem:
        print("crs in vector data provider test")
        # Return the CRS for the vector data
        # Example: WGS 84 (EPSG:4326)
        crs = QgsCoordinateReferenceSystem()
        crs.createFromSrid(4326)  # WGS 84
        # crs.createFromId(4326)  # WGS 84
        return crs

    def featureSource(self) -> "QgsVectorDataProvider":
        print("featureSource in vector data provider test")
        # This method should return the provider itself, but this is just a placeholder
        return CustomFeatureSource(self)

    def extent(self) -> QgsRectangle:
        print("extent in vector data provider test")
        if not self.isValid():
            return QgsRectangle()

        if self.connection_manager.get_connection(self.connection_name) is None:
            self.connection_manager.connect(self.connection_name, self.auth_information)

        # Query to get the bounding box of the features
        query = f"""
            SELECT
                MIN(ST_XMin({self.column})) AS min_x,
                MAX(ST_XMax({self.column})) AS max_x,
                MIN(ST_YMin({self.column})) AS min_y,
                MAX(ST_YMax({self.column})) AS max_y
            FROM {self.database}.{self.schema}.{self.table} limit 10
        """
        cursor = self.connection_manager.execute_query(self.connection_name, query)
        cursor.execute(query)
        row = cursor.fetchone()

        if row:
            min_x, max_x, min_y, max_y = row
            return QgsRectangle(min_x, min_y, max_x, max_y)
        else:
            return QgsRectangle()  # Return an empty rectangle if no data


# from qgis.core import QgsVectorDataProvider, QgsFeature, QgsGeometry

# from ..helpers.utils import get_authentification_information, get_qsettings

# from ..managers.sf_connection_manager import SFConnectionManager


# class SFVectorDataProviderTest(QgsVectorDataProvider):
#     def __init__(self, uri):
#         super().__init__()
#         self.uri = uri
#         self.connection = self.connectToDatabase(uri)
#         self.features = []  # Placeholder for features

#     def connectToDatabase(self, uri):
#         # Implement your database connection logic here
#         # For example, using SQLAlchemy or another library
#         self.connection_manager: SFConnectionManager = (
#             SFConnectionManager.get_instance()
#         )
#         self.settings = get_qsettings()
#         self.connection_name, self.database, self.schema, self.table, self.column = (
#             uri.split("://")[1].split("/")
#         )
#         self.auth_information = get_authentification_information(
#             self.settings, self.connection_name
#         )
#         self.query = f"SELECT ST_ASWKB({self.column}) AS ASWKB FROM {self.database}.{self.schema}.{self.table} WHERE ASWKB IS NOT NULL limit 10"
#         if self.connection_manager.get_connection(self.connection_name) is None:
#             self.connection_manager.connect(self.connection_name, self.auth_information)
#         self.cursor = self.connection_manager.execute_query(
#             self.connection_name, self.query
#         )

#     def getFeatures(self):
#         # Fetch features from the database and yield QgsFeature objects
#         # for row in self.fetchDataFromDatabase():
#         while True:
#             row = self.cursor.fetchone()
#             if row is None:
#                 break
#             feature = QgsFeature()
#             feature.setGeometry(
#                 QgsGeometry.fromWkt(row[0])
#             )  # Assuming 'geom' is your geometry column
#             # feature.setAttributes([row["id"], row["name"]])  # Adjust as necessary
#             yield feature

#     def fetchDataFromDatabase(self):
#         # Implement logic to retrieve data from the database
#         return []  # Replace with actual data retrieval logic

#     def featureCount(self):
#         return 100

#     def isValid(self):
#         # Implement logic to check if the provider is valid
#         return self.connection_name in self.connection_manager

#     def name(self):
#         # Return a unique name for your provider
#         return "CustomDatabaseProvider"
