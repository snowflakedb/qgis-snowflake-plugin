from qgis.core import QgsFeature, QgsFeatureIterator, QgsFields, QgsGeometry
import snowflake.connector


class SFFeatureIterator(QgsFeatureIterator):
    def __init__(
        self,
        cursor: snowflake.connector.cursor.SnowflakeCursor,
        fields: QgsFields,
        set_geometry: bool = False,
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
        self.set_geometry = set_geometry

    def __iter__(self) -> "QgsFeatureIterator":
        """
        Returns an iterator object for iterating over features.

        :return: An iterator object for iterating over features.
        :rtype: QgsFeatureIterator
        """
        self._index = 0
        return self

    def __next__(self) -> QgsFeature:
        """
        Retrieves the next feature from the iterator.

        Returns:
            QgsFeature: The next feature from the iterator.

        Raises:
            StopIteration: If there are no more features to retrieve.
        """
        row = self.cursor.fetchone()
        print(row)

        if row is None:
            raise StopIteration

        if self.set_geometry:
            row_column = row[0]
            print(row_column)
            while row_column is None:
                row = self.cursor.fetchone()
                row_column = row[0]

        feature = QgsFeature(self.fields)
        feature.setAttributes(list(row))
        if self.set_geometry:
            column_0 = feature.attribute(0)
            qgsGeometry = QgsGeometry()
            qgsGeometry.fromWkb(column_0)
            feature.setGeometry(qgsGeometry)
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
