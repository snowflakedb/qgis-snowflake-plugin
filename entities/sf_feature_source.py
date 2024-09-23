from qgis.core import (
    QgsFeatureSource,
    QgsAbstractFeatureSource,
    QgsFeature,
    QgsFeatureRequest,
    QgsFields,
    QgsGeometry,
    QgsPointXY,
    QgsProviderMetadata,
)

from ..entities.sf_feature_iterator import SFFeatureIterator

from ..providers.sf_vector_data_provider import SFVectorDataProvider


class SFFeatureSource(QgsAbstractFeatureSource):
    def __init__(self, provider: SFVectorDataProvider):
        super().__init__()
        self.provider = provider

    # def allFeatureIds(self):
    #     """Return a list of all feature IDs for features present in the source."""
    #     return [feature.id() for feature in self.provider._features]

    # def featureCount(self):
    #     """Return the number of features contained in the source, or -1 if unknown."""
    #     return self.provider.featureCount()

    # def fields(self):
    #     """Return the fields associated with features in the source."""
    #     return self.provider.fields()

    def getFeatures(self, request: QgsFeatureRequest = QgsFeatureRequest()):
        """Return an iterator for the features in the source."""
        # for feature in self.provider.getFeatures(request):
        #     yield feature
        # return self.provider.getFeatures(request)
        # Filter features based on the request if needed
        if (
            self.provider.connection_manager.get_connection(
                self.provider.connection_name
            )
            is None
        ):
            self.provider.connection_manager.connect(
                self.provider.connection_name, self.provider.auth_information
            )
        cursor = self.provider.connection_manager.execute_query(
            self.provider.connection_name, self.provider.query
        )
        print(f"getFeatures = {cursor}")
        print(f"getFeatures = {self.provider.query}")

        return SFFeatureIterator(cursor=cursor, fields=self.fields(), set_geometry=True)

    # def hasFeatures(self):
    #     """Determine if there are any features available in the source."""
    #     return len(self.provider.featureCount()) > 0

    # def hasSpatialIndex(self):
    #     """Return the presence of a valid spatial index on the source."""
    #     return (
    #         QgsFeatureSource.SpatialIndexUnknown
    #     )  # Adjust based on your implementation

    # def uniqueValues(self, fieldIndex, limit=-1):
    #     """Return the set of unique values contained within the specified fieldIndex."""
    #     unique_values = set()
    #     for feature in self.provider._features:
    #         unique_values.add(feature.attribute(fieldIndex))
    #         if limit > 0 and len(unique_values) >= limit:
    #             break
    #     return unique_values

    # def extent(self):
    #     """Return the extent of all geometries from the source."""
    #     print("extent in sf_feature_source")
    #     if not self.provider.features:
    #         return (
    #             QgsGeometry().boundingBox()
    #         )  # Return an empty bounding box if no features

    #     bbox = QgsGeometry.fromPointXY(
    #         QgsPointXY(self.provider.features[0].geometry().asPoint())
    #     ).boundingBox()
    #     for feature in self.provider._features:
    #         bbox.combineExtentWith(feature.geometry().boundingBox())
    #     return bbox
