from ..helpers.utils import get_authentification_information, get_qsettings
from qgis.PyQt.QtCore import QSettings
from ..providers.sf_data_source_provider import SFDataProvider
from ..entities.sf_feature_iterator import SFFeatureIterator
import snowflake.connector


def get_schema_iterator(settings: QSettings, connection_name: str) -> SFFeatureIterator:
    """
    Retrieves an iterator over the schema names in the specified database.

    Args:
        settings (QSettings): The settings object containing configuration details.
        connection_name (str): The name of the database connection.

    Returns:
        SFFeatureIterator: An iterator over the schema names in the database.
    """
    auth_information = get_authentification_information(settings, connection_name)
    sf_data_provider = SFDataProvider(auth_information)

    query = f"""SELECT DISTINCT SCHEMA_NAME
FROM INFORMATION_SCHEMA.SCHEMATA
WHERE CATALOG_NAME = '{auth_information["database"]}'
ORDER BY SCHEMA_NAME"""

    sf_data_provider.load_data(query, connection_name)
    return sf_data_provider.get_feature_iterator()


def get_table_column_iterator(
    settings: QSettings, connection_name: str, table_name: str
) -> SFFeatureIterator:
    """
    Retrieves an iterator for the columns of a specified table in a database.

    Args:
        settings (QSettings): The settings object containing configuration details.
        connection_name (str): The name of the database connection.
        table_name (str): The name of the table to retrieve columns from.

    Returns:
        SFFeatureIterator: An iterator over the features (columns) of the specified table.

    Raises:
        Any exceptions raised by the underlying data provider or database query execution.
    """
    auth_information = get_authentification_information(settings, connection_name)
    schema_selected_query = f"""SELECT DISTINCT TABLE_NAME, COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_CATALOG ILIKE '{auth_information["database"]}'
AND TABLE_SCHEMA ILIKE '{table_name}'
AND DATA_TYPE in ('GEOGRAPHY', 'GEOMETRY')
ORDER BY TABLE_NAME, COLUMN_NAME"""
    sf_data_provider = SFDataProvider(auth_information)

    sf_data_provider.load_data(schema_selected_query, connection_name)
    return sf_data_provider.get_feature_iterator()


def get_column_iterator(
    settings: QSettings, connection_name: str, table_data_item
) -> SFFeatureIterator:
    """
    Retrieves an iterator over the columns of a specified table in a database.

    Args:
        settings (QSettings): The settings object containing configuration details.
        connection_name (str): The name of the database connection.
        table_data_item (SFDataItem): The data item representing the table whose columns are to be retrieved.

    Returns:
        SFFeatureIterator: An iterator over the features (columns) of the specified table.
    """
    auth_information = get_authentification_information(settings, connection_name)
    sf_data_provider = SFDataProvider(auth_information)

    schema_data_item = table_data_item.parent()

    query = f"""SELECT DISTINCT COLUMN_NAME, DATA_TYPE, NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_CATALOG = '{auth_information["database"]}'
AND TABLE_SCHEMA ILIKE '{schema_data_item.clean_name}'
AND TABLE_NAME ILIKE '{table_data_item.clean_name}'
ORDER BY COLUMN_NAME"""

    sf_data_provider.load_data(query, connection_name)
    return sf_data_provider.get_feature_iterator()


def get_table_iterator(settings: QSettings, connection_name: str, schema_name: str):
    """
    Retrieves an iterator over the table names in a specified schema within a database.

    Args:
        settings (QSettings): The settings object containing configuration details.
        connection_name (str): The name of the database connection.
        schema_name (str): The name of the schema to query for table names.

    Returns:
        Iterator: An iterator over the table names in the specified schema.
    """
    auth_information = get_authentification_information(settings, connection_name)
    sf_data_provider = SFDataProvider(auth_information)

    query = f"""SELECT DISTINCT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE table_catalog = '{auth_information["database"]}'
AND TABLE_SCHEMA = '{schema_name}'
ORDER BY TABLE_NAME"""

    sf_data_provider.load_data(query, connection_name)
    return sf_data_provider.get_feature_iterator()


def get_features_iterator(
    auth_information: dict, query: str, connection_name: str
) -> SFFeatureIterator:
    """
    Retrieves an iterator for features from a Salesforce data provider.

    Args:
        auth_information (dict): A dictionary containing authentication information.
        query (str): The query string to retrieve data.
        connection_name (str): The name of the connection to use.

    Returns:
        SFFeatureIterator: An iterator for the retrieved features.
    """
    sf_data_provider = SFDataProvider(auth_information)

    sf_data_provider.load_data(query, connection_name)
    return sf_data_provider.get_feature_iterator()


def get_columns_cursor(
    auth_information: dict,
    database_name: str,
    schema: str,
    table: str,
    connection_name: str,
) -> snowflake.connector.cursor.SnowflakeCursor:
    """
    Retrieves a cursor containing the distinct column names and data types
    from a specified table in a Snowflake database.

    Args:
        auth_information (dict): Authentication information required to connect to Snowflake.
        database_name (str): The name of the database containing the table.
        schema (str): The schema within the database containing the table.
        table (str): The name of the table to retrieve column information from.
        connection_name (str): The name of the connection to use for executing the query.

    Returns:
        snowflake.connector.cursor.SnowflakeCursor: A cursor containing the results of the query.
    """
    sf_data_provider = SFDataProvider(auth_information)
    query_select_columns = f"""
        SELECT DISTINCT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_CATALOG = '{database_name}'
        AND TABLE_SCHEMA ILIKE '{schema}'
        AND TABLE_NAME ILIKE '{table}'
        ORDER BY COLUMN_NAME
    """
    return sf_data_provider.execute_query(
        query=query_select_columns, connection_name=connection_name
    )


def get_cursor_description(
    auth_information: dict, query: str, connection_name: str
) -> list[snowflake.connector.cursor.ResultMetadata]:
    """
    Executes a query on a Snowflake database and retrieves the cursor description.

    Args:
        auth_information (dict): Authentication information required to connect to the Snowflake database.
        query (str): The SQL query to be executed.
        connection_name (str): The name of the connection to be used.

    Returns:
        list[snowflake.connector.cursor.ResultMetadata]: A list containing the metadata of the result set.
    """
    sf_data_provider = SFDataProvider(auth_information)
    cur_query = sf_data_provider.execute_query(query, connection_name)
    cur_description = cur_query.description
    cur_query.close()
    return cur_description


def __execute_query(
    settings: QSettings, connection_name: str, query: str
) -> snowflake.connector.cursor.SnowflakeCursor:
    """
    Executes a SQL query using the provided Snowflake connection settings.

    Args:
        settings (QSettings): The QSettings object containing the configuration settings.
        connection_name (str): The name of the Snowflake connection to use.
        query (str): The SQL query to be executed.

    Returns:
        snowflake.connector.cursor.SnowflakeCursor: The cursor object resulting from the executed query.
    """
    auth_information = get_authentification_information(
        settings=settings, connection_name=connection_name
    )
    sf_data_provider = SFDataProvider(auth_information)
    return sf_data_provider.execute_query(query=query, connection_name=connection_name)


def __get_cur_count(
    settings: QSettings,
    connection_name: str,
    query: str,
) -> int:
    """
    Executes a given SQL query and returns the number of rows affected.

    Args:
        settings (QSettings): The QSettings object containing database configuration.
        connection_name (str): The name of the database connection.
        query (str): The SQL query to be executed.

    Returns:
        int: The number of rows affected by the query.
    """
    cur = __execute_query(
        settings=settings, connection_name=connection_name, query=query
    )
    count = cur.rowcount
    cur.close()
    return count


def get_count_schemas(
    settings: QSettings, connection_name: str, data_base_name: str, schema_name: str
) -> int:
    """
    Retrieves the count of schemas in a specified database that match the given schema name.

    Args:
        settings (QSettings): The QSettings object containing configuration settings.
        connection_name (str): The name of the database connection.
        data_base_name (str): The name of the database to search within.
        schema_name (str): The name of the schema to search for.

    Returns:
        int: The count of schemas that match the specified schema name in the given database.
    """
    query_search_public_schema = f"""SELECT DISTINCT SCHEMA_NAME
FROM INFORMATION_SCHEMA.SCHEMATA
WHERE CATALOG_NAME ilike '{data_base_name}'
and SCHEMA_NAME ilike '{schema_name}'"""
    return __get_cur_count(settings, connection_name, query_search_public_schema)


def create_schema(settings: QSettings, connection_name: str, schema_name: str):
    """
    Creates a new schema in the specified database connection.

    Args:
        settings (QSettings): The QSettings object containing the database configuration.
        connection_name (str): The name of the database connection.
        schema_name (str): The name of the schema to be created.

    Returns:
        None
    """
    query_create_public_schema = f"""CREATE SCHEMA {schema_name}"""
    cur = __execute_query(
        settings=settings,
        connection_name=connection_name,
        query=query_create_public_schema,
    )
    cur.close()


def get_count_tables(
    connection_name: str, database_name: str, schema_name: str, table_name: str
) -> int:
    """
    Retrieves the count of tables matching the specified criteria from the database.

    Args:
        connection_name (str): The name of the database connection.
        database_name (str): The name of the database.
        schema_name (str): The name of the schema.
        table_name (str): The name of the table.

    Returns:
        int: The count of tables that match the specified criteria.
    """
    settings = get_qsettings()
    query_search_table = f"""SELECT DISTINCT TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG ilike '{database_name}'
and TABLE_SCHEMA ilike '{schema_name}'
and TABLE_NAME ilike '{table_name}'"""

    return __get_cur_count(settings, connection_name, query_search_table)


def create_table(connection_name: str, query: str):
    settings = get_qsettings()
    cur = __execute_query(
        settings=settings,
        connection_name=connection_name,
        query=query,
    )
    cur.close()
