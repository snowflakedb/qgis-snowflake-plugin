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
    "BOOL": QVariant.Bool,
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


SNOWFLAKE_METADATA_TYPE_CODE_DICT = {
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
