from qgis.core import QgsWkbTypes
from qgis.PyQt.QtCore import QVariant

mapping_snowflake_qgis_geometry = {
    "LINESTRING": QgsWkbTypes.LineString,
    "MULTILINESTRING": QgsWkbTypes.MultiLineString,
    "MULTIPOINT": QgsWkbTypes.MultiPolygon,
    "MULTIPOLYGON": QgsWkbTypes.MultiPolygon,
    "POINT": QgsWkbTypes.Point,
    "POLYGON": QgsWkbTypes.Polygon,
    # ...
}

mapping_snowflake_qgis_type = {
    "BIGINT": QVariant.Int,
    "BOOLEAN": QVariant.Bool,
    "DATE": QVariant.Date,
    "TIME": QVariant.Time,
    "DOUBLE": QVariant.Double,
    "FLOAT": QVariant.Double,
    "INTEGER": QVariant.Int,
    "TIMESTAMP": QVariant.DateTime,
    "VARCHAR": QVariant.String,
    # Type used for custom sql when table is not created
    # Not difference betwenn float and integer so all the numeric field are NUMBER
    "NUMBER": QVariant.Double,
    "STRING": QVariant.String,
    "Date": QVariant.Date,
    "bool": QVariant.Bool,
    "JSON": QVariant.String,
    "TEXT": QVariant.String,
    "ARRAY": QVariant.List,
    "BINARY": QVariant.ByteArray,
    "GEOGRAPHY": QVariant.String,
    "GEOMETRY": QVariant.String,
    "OBJECT": QVariant.String,
    "TIMESTAMP_LTZ": QVariant.DateTime,
    "TIMESTAMP_NTZ": QVariant.DateTime,
    "TIMESTAMP_TZ": QVariant.DateTime,
    "VARIANT": QVariant.String,
}
