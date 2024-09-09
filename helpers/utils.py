from datetime import datetime
from qgis.PyQt.QtCore import QSettings
import os
from qgis.PyQt.QtWidgets import QMessageBox


def get_authentification_information(settings: QSettings, connection_name: str) -> dict:
    """
    Retrieves authentication information from the given settings for the specified connection name.

    Parameters:
    - settings (QSettings): The QSettings object containing the authentication settings.
    - connection_name (str): The name of the connection for which to retrieve the authentication information.

    Returns:
    - dict: A dictionary containing the authentication information with the following keys:
        - "warehouse" (str): The name of the warehouse.
        - "account" (str): The name of the Snowflake account.
        - "database" (str): The name of the Snowflake database.
        - "username" (str): The username for the Snowflake connection.
        - "connection_type" (str): The type of the Snowflake connection.
        - "password" (str): The password for the Snowflake connection.
    """
    auth_info = {}
    settings.beginGroup(connection_name)
    auth_info["warehouse"] = settings.value("warehouse", defaultValue="")
    auth_info["account"] = settings.value("account", defaultValue="")
    auth_info["database"] = settings.value("database", defaultValue="")
    auth_info["username"] = settings.value("username", defaultValue="")
    auth_info["connection_type"] = settings.value("connection_type", defaultValue="")
    auth_info["password"] = settings.value("password", defaultValue="")
    settings.endGroup()

    return auth_info


def get_qsettings() -> QSettings:
    """
    Returns a QSettings object for the Snowflake QGIS plugin.

    Returns:
        QSettings: The QSettings object for the Snowflake QGIS plugin.
    """
    return QSettings(
        QSettings.IniFormat, QSettings.UserScope, "Snowflake", "SF_QGIS_PLUGIN"
    )


def write_to_log(string_to_write: str) -> None:
    """
    Writes the given string to a log file.

    Parameters:
        string_to_write (str): The string to be written to the log file.

    Returns:
        None
    """
    now = datetime.now()
    folder_path = "~/.sf_qgis_plugin"
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    with open(
        f"{folder_path}/log.log",
        "a",
    ) as file:
        # Write data to the file
        file.write(f"{now} => {string_to_write}\n")


def remove_connection(settings: QSettings, connection_name: str) -> None:
    """
    Remove a connection from the settings.

    Parameters:
    - settings (QSettings): The QSettings object to remove the connection from.
    - connection_name (str): The name of the connection to remove.

    Returns:
    - None
    """
    settings.beginGroup(connection_name)
    settings.remove("")
    settings.endGroup()
    settings.sync()


def on_handle_error(title: str, message: str) -> None:
    QMessageBox.critical(None, title, message, QMessageBox.Ok)


def check_package_installed(package_name) -> bool:
    import pkg_resources

    # Iterate over all installed distributions
    for package in pkg_resources.working_set:
        if package.key == package_name:
            return True
    return False


def check_install_snowflake_connector_package() -> None:
    if not check_package_installed("snowflake-connector-python"):
        import subprocess
        import platform
        from qgis.core import QgsApplication
        import os

        if platform.system() == "Windows":
            subprocess.run(
                ["pip3", "install", "snowflake-connector-python[secure-local-storage]"],
                check=True,
            )
            subprocess.call(["pip3", "install", "pyopenssl", "--upgrade"])
        else:
            prefixPath = QgsApplication.prefixPath()
            python3_path = os.path.join(prefixPath, "bin", "python3")
            subprocess.run(
                [
                    python3_path,
                    "-m",
                    "pip",
                    "install",
                    "snowflake-connector-python[secure-local-storage]",
                ],
                check=True,
            )
            subprocess.call(
                [python3_path, "-m", "pip", "install", "pyopenssl", "--upgrade"]
            )


def uninstall_snowflake_connector_package() -> None:
    import subprocess
    import platform
    from qgis.core import QgsApplication
    import os

    if platform.system() == "Windows":
        subprocess.run(
            [
                "pip3",
                "uninstall",
                "snowflake-connector-python[secure-local-storage]",
                "-y",
            ],
            check=True,
        )
    else:
        prefixPath = QgsApplication.prefixPath()
        python3_path = os.path.join(prefixPath, "bin", "python3")
        subprocess.run(
            [
                python3_path,
                "-m",
                "pip",
                "uninstall",
                "snowflake-connector-python[secure-local-storage]",
                "-y",
            ],
            check=True,
        )
