from qgis.PyQt import QtWidgets


def get_ok_cancel_message_box(title: str, text: str) -> int:
    message_box = QtWidgets.QMessageBox()
    message_box.setWindowTitle(title)
    message_box.setText(text)
    message_box.setStandardButtons(
        QtWidgets.QMessageBox.Ok | QtWidgets.QMessageBox.Cancel
    )

    return message_box.exec_()
