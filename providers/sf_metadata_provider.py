import re
from typing import Dict
from qgis.core import QgsProviderMetadata

from .sf_vector_data_provider import SFVectorDataProvider


class SFMetadataProvider(QgsProviderMetadata):
    def __init__(self):
        super().__init__(
            SFVectorDataProvider.providerKey(),
            SFVectorDataProvider.description(),
            SFVectorDataProvider.createProvider,
        )

    def decodeUri(self, uri: str) -> Dict[str, str]:
        """Breaks a provider data source URI into its component paths
        (e.g. file path, layer name).

        :param str uri: uri to convert
        :returns: dict of components as strings
        """
        supported_keys = [
            "connection_name",
            "sql_query",
            "schema_name",
            "table_name",
            "srid",
            "geom_column",
            "geometry_type",
            "geo_column_type",
        ]
        matches = re.findall(
            f"({'|'.join(supported_keys)})=(.*?) *?(?={'|'.join(supported_keys)}=|$)",
            uri,
            flags=re.DOTALL,
        )
        params = {key: value for key, value in matches}
        return params

    def encodeUri(self, parts: Dict[str, str]) -> str:
        """Reassembles a provider data source URI from its component paths
        (e.g. file path, layer name).

        :param Dict[str, str] parts: parts as returned by decodeUri
        :returns: uri as string
        """

        connection_name = parts["connection_name"]
        sql_query = parts["sql_query"]
        schema_name = parts["schema_name"]
        table_name = parts["table_name"]
        srid = parts["srid"]
        geo_column = parts["geo_column"]
        geometry_type = parts["geometry_type"]
        geo_column_type = parts["geo_column_type"]
        uri = (
            f"connection_name={connection_name} sql={sql_query} "
            f"schema_name={schema_name} table_name={table_name} srid={srid} "
            f"geo_column={geo_column} geometry_type={geometry_type} geo_column_type={geo_column_type}"
        )
        return uri
