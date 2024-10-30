from qgis.PyQt import QtWidgets


def get_ok_cancel_message_box(title: str, text: str) -> int:
    message_box = QtWidgets.QMessageBox()

    message_box.setWindowTitle(title)
    message_box.setText(text)

    proceed_button = message_box.addButton("Proceed", QtWidgets.QMessageBox.AcceptRole)
    cancel_button = message_box.addButton("Cancel", QtWidgets.QMessageBox.RejectRole)

    message_box.exec_()

    if message_box.clickedButton() == proceed_button:
        print("Proceed button clicked.")
        return QtWidgets.QMessageBox.Ok
    elif message_box.clickedButton() == cancel_button:
        print("Cancel button clicked.")
        return QtWidgets.QMessageBox.Cancel
