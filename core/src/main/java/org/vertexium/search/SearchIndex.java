package org.vertexium.search;

import org.vertexium.*;
import org.vertexium.mutation.AdditionalExtendedDataVisibilityAddMutation;
import org.vertexium.mutation.AdditionalExtendedDataVisibilityDeleteMutation;
import org.vertexium.mutation.ExistingElementMutation;
import org.vertexium.mutation.ExtendedDataMutation;
import org.vertexium.query.*;
import org.vertexium.util.FutureDeprecation;

import java.util.Collection;

public interface SearchIndex {

    @FutureDeprecation
    default void addElement(Graph graph, Element element, Authorizations authorizations) {
        addElement(graph, element, authorizations.getUser());
    }

    void addElement(Graph graph, Element element, User user);

    @FutureDeprecation
    default <TElement extends Element> void updateElement(Graph graph, ExistingElementMutation<TElement> mutation, Authorizations authorizations) {
        updateElement(graph, mutation, authorizations.getUser());
    }

    <TElement extends Element> void updateElement(Graph graph, ExistingElementMutation<TElement> mutation, User user);

    @FutureDeprecation
    default void deleteElement(Graph graph, Element element, Authorizations authorizations) {
        deleteElement(graph, element, authorizations.getUser());
    }

    void deleteElement(Graph graph, Element element, User user);

    @FutureDeprecation
    default void markElementHidden(Graph graph, Element element, Visibility visibility, Authorizations authorizations) {
        markElementHidden(graph, element, visibility, authorizations.getUser());
    }

    void markElementHidden(Graph graph, Element element, Visibility visibility, User user);

    @FutureDeprecation
    default void markElementVisible(Graph graph, ElementLocation elementLocation, Visibility visibility, Authorizations authorizations) {
        markElementVisible(graph, elementLocation, visibility, authorizations.getUser());
    }

    void markElementVisible(Graph graph, ElementLocation elementLocation, Visibility visibility, User user);

    @FutureDeprecation
    default void markPropertyHidden(Graph graph, ElementLocation elementLocation, Property property, Visibility visibility, Authorizations authorizations) {
        markPropertyHidden(graph, elementLocation, property, visibility, authorizations.getUser());
    }

    void markPropertyHidden(Graph graph, ElementLocation elementLocation, Property property, Visibility visibility, User user);

    @FutureDeprecation
    default void markPropertyVisible(Graph graph, ElementLocation elementLocation, Property property, Visibility visibility, Authorizations authorizations) {
        markPropertyVisible(graph, elementLocation, property, visibility, authorizations.getUser());
    }

    void markPropertyVisible(Graph graph, ElementLocation elementLocation, Property property, Visibility visibility, User user);

    /**
     * Default delete property simply calls deleteProperty in a loop. It is up to the SearchIndex implementation to decide
     * if a collective method can be made more efficient
     */
    @FutureDeprecation
    default void deleteProperties(Graph graph, Element element, Collection<PropertyDescriptor> propertyList, Authorizations authorizations) {
        deleteProperties(graph, element, propertyList, authorizations.getUser());
    }

    default void deleteProperties(Graph graph, Element element, Collection<PropertyDescriptor> propertyList, User user) {
        propertyList.forEach(p -> deleteProperty(graph, element, p, user));
    }

    @FutureDeprecation
    default void deleteProperty(Graph graph, Element element, PropertyDescriptor property, Authorizations authorizations) {
        deleteProperty(graph, element, property, authorizations.getUser());
    }

    void deleteProperty(Graph graph, Element element, PropertyDescriptor property, User user);

    @FutureDeprecation
    default void addElements(Graph graph, Iterable<? extends Element> elements, Authorizations authorizations) {
        addElements(graph, elements, authorizations.getUser());
    }

    void addElements(Graph graph, Iterable<? extends Element> elements, User user);

    @FutureDeprecation
    org.vertexium.query.GraphQuery queryGraph(Graph graph, String queryString, Authorizations authorizations);

    GraphQuery queryGraph(Graph graph, String queryString, User user);

    @FutureDeprecation
    org.vertexium.query.MultiVertexQuery queryGraph(Graph graph, String[] vertexIds, String queryString, Authorizations authorizations);

    MultiVertexQuery queryGraph(Graph graph, String[] vertexIds, String queryString, User user);

    @FutureDeprecation
    org.vertexium.query.VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, Authorizations authorizations);

    VertexQuery queryVertex(Graph graph, Vertex vertex, String queryString, User user);

    @FutureDeprecation
    org.vertexium.query.Query queryExtendedData(Graph graph, Element element, String tableName, String queryString, Authorizations authorizations);

    Query queryExtendedData(Graph graph, Element element, String tableName, String queryString, User user);

    @FutureDeprecation
    org.vertexium.query.SimilarToGraphQuery querySimilarTo(Graph graph, String[] fields, String text, Authorizations authorizations);

    SimilarToGraphQuery querySimilarTo(Graph graph, String[] fields, String text, User user);

    void flush(Graph graph);

    void shutdown();

    void clearCache();

    boolean isFieldBoostSupported();

    void truncate(Graph graph);

    void drop(Graph graph);

    SearchIndexSecurityGranularity getSearchIndexSecurityGranularity();

    boolean isQuerySimilarToTextSupported();

    boolean isFieldLevelSecuritySupported();

    @FutureDeprecation
    default <T extends Element> void alterElementVisibility(
        Graph graph,
        ExistingElementMutation<T> elementMutation,
        Visibility oldVisibility,
        Visibility newVisibility,
        Authorizations authorizations
    ) {
        alterElementVisibility(graph, elementMutation, oldVisibility, newVisibility, authorizations.getUser());
    }

    <T extends Element> void alterElementVisibility(
        Graph graph,
        ExistingElementMutation<T> elementMutation,
        Visibility oldVisibility,
        Visibility newVisibility,
        User user
    );

    @FutureDeprecation
    default void addElementExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        Iterable<ExtendedDataMutation> extendedDatas,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes,
        Authorizations authorizations
    ) {
        addElementExtendedData(
                graph,
                elementLocation,
                extendedDatas,
                additionalExtendedDataVisibilities,
                additionalExtendedDataVisibilityDeletes,
                authorizations.getUser()
        );
    }

    void addElementExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        Iterable<ExtendedDataMutation> extendedDatas,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes,
        User user
    );

    @FutureDeprecation
    default void addExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        Iterable<ExtendedDataRow> extendedDatas,
        Authorizations authorizations
    ) {
        addExtendedData(graph, elementLocation, extendedDatas, authorizations.getUser());
    }

    void addExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        Iterable<ExtendedDataRow> extendedDatas,
        User user
    );

    @FutureDeprecation
    default void deleteExtendedData(Graph graph, ExtendedDataRowId extendedDataRowId, Authorizations authorizations) {
        deleteExtendedData(graph, extendedDataRowId, authorizations.getUser());
    }

    void deleteExtendedData(Graph graph, ExtendedDataRowId extendedDataRowId, User user);

    @FutureDeprecation
    default void deleteExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        String tableName,
        String row,
        String columnName,
        String key,
        Visibility visibility,
        Authorizations authorizations
    ) {
        deleteExtendedData(graph, elementLocation, tableName, row, columnName, key, visibility, authorizations.getUser());
    }

    void deleteExtendedData(
        Graph graph,
        ElementLocation elementLocation,
        String tableName,
        String row,
        String columnName,
        String key,
        Visibility visibility,
        User user
    );

    @FutureDeprecation
    default void addAdditionalVisibility(Graph graph, Element element, String visibility, Object eventData, Authorizations authorizations) {
        addAdditionalVisibility(graph, element, visibility, eventData, authorizations.getUser());
    }

    void addAdditionalVisibility(Graph graph, Element element, String visibility, Object eventData, User user);

    @FutureDeprecation
    default void deleteAdditionalVisibility(Graph graph, Element element, String visibility, Object eventData, Authorizations authorizations) {
        deleteAdditionalVisibility(graph, element, visibility, eventData, authorizations.getUser());
    }

    void deleteAdditionalVisibility(Graph graph, Element element, String visibility, Object eventData, User user);
}
