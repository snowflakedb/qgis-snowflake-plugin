from qgis.core import QgsFeature, QgsFeatureIterator, QgsFields
import snowflake.connector


class SFFeatureIterator(QgsFeatureIterator):
    def __init__(
        self, cursor: snowflake.connector.cursor.SnowflakeCursor, fields: QgsFields
    ):
        """
        Initializes a new instance of the SfFeatureIterator class.

        Args:
            cursor (snowflake.connector.cursor.SnowflakeCursor): The Snowflake cursor object.
            fields (QgsFields): The fields of the feature.

        Returns:
            None
        """
        super().__init__()
        self.cursor = cursor
        self.fields = fields
        self.cursor_batch_rows = []

    def __iter__(self) -> "QgsFeatureIterator":
        """
        Returns an iterator object for iterating over features.

        :return: An iterator object for iterating over features.
        :rtype: QgsFeatureIterator
        """
        return self

    def __next__(self) -> QgsFeature:
        """
        Retrieves the next feature from the iterator.

        Returns:
            QgsFeature: The next feature from the iterator.

        Raises:
            StopIteration: If there are no more features to retrieve.
        """

        if not self.cursor_batch_rows:
            self.cursor_batch_rows = self.cursor.fetchmany(5000)

        row = self.cursor_batch_rows.pop(0) if self.cursor_batch_rows else None
        if row is None:
            raise StopIteration
        feature = QgsFeature(self.fields)
        feature.setAttributes(list(row))
        return feature

    def close(self) -> bool:
        """
        Closes the cursor.

        Returns:
            bool: True if the cursor was successfully closed, False otherwise.
        """
        try:
            if self.cursor:
                self.cursor.close()
            return True
        except Exception as _:
            return False
