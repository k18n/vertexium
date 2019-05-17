package org.vertexium.elasticsearch5;

import org.vertexium.Element;
import org.vertexium.ExtendedDataRowId;
import org.vertexium.PropertyDefinition;
import org.vertexium.mutation.ElementMutation;

import java.util.EnumSet;

public interface IndexSelectionStrategy {
    String[] getIndicesToQuery(Elasticsearch5SearchIndex es);

    String getIndexName(Elasticsearch5SearchIndex es, Element element);

    String[] getIndexNames(Elasticsearch5SearchIndex es, PropertyDefinition propertyDefinition);

    boolean isIncluded(Elasticsearch5SearchIndex es, String indexName);

    String[] getManagedIndexNames(Elasticsearch5SearchIndex es);

    String[] getIndicesToQuery(ElasticsearchSearchQueryBase query, EnumSet<ElasticsearchDocumentType> elementType);

    <T extends Element> String getExtendedDataIndexName(
        Elasticsearch5SearchIndex es,
        ElementMutation<T> elementMutation,
        String tableName,
        String rowId
    );

    String getExtendedDataIndexName(Elasticsearch5SearchIndex es, ExtendedDataRowId rowId);
}
