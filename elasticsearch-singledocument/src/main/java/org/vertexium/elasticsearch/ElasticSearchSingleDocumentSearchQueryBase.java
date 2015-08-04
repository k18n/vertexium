package org.vertexium.elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.*;
import org.vertexium.Authorizations;
import org.vertexium.Graph;
import org.vertexium.GraphBaseWithSearchIndex;
import org.vertexium.PropertyDefinition;
import org.vertexium.elasticsearch.score.ScoringStrategy;
import org.vertexium.query.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ElasticSearchSingleDocumentSearchQueryBase extends ElasticSearchQueryBase implements
        GraphQueryWithHistogramAggregation,
        GraphQueryWithTermsAggregation,
        GraphQueryWithGeohashAggregation {
    private final List<HistogramQueryItem> histogramQueryItems = new ArrayList<>();
    private final List<TermsQueryItem> termsQueryItems = new ArrayList<>();
    private final List<GeohashQueryItem> geohashQueryItems = new ArrayList<>();

    public ElasticSearchSingleDocumentSearchQueryBase(
            TransportClient client,
            Graph graph,
            String queryString,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            Authorizations authorizations
    ) {
        super(client, graph, queryString, propertyDefinitions, scoringStrategy, indexSelectionStrategy, false, true, authorizations);
    }

    public ElasticSearchSingleDocumentSearchQueryBase(
            TransportClient client,
            Graph graph,
            String[] similarToFields,
            String similarToText,
            Map<String, PropertyDefinition> propertyDefinitions,
            ScoringStrategy scoringStrategy,
            IndexSelectionStrategy indexSelectionStrategy,
            Authorizations authorizations
    ) {
        super(client, graph, similarToFields, similarToText, propertyDefinitions, scoringStrategy, indexSelectionStrategy, false, true, authorizations);
    }

    @Override
    public GraphQueryWithHistogramAggregation addHistogramAggregation(String aggregationName, String fieldName, String interval) {
        histogramQueryItems.add(new HistogramQueryItem(aggregationName, fieldName, interval));
        return this;
    }

    @Override
    public GraphQueryWithTermsAggregation addTermsAggregation(String aggregationName, String fieldName) {
        termsQueryItems.add(new TermsQueryItem(aggregationName, fieldName));
        return this;
    }

    @Override
    public GraphQueryWithGeohashAggregation addGeohashAggregation(String aggregationName, String fieldName, int precision) {
        geohashQueryItems.add(new GeohashQueryItem(aggregationName, fieldName, precision));
        return this;
    }

    @Override
    protected SearchRequestBuilder getSearchRequestBuilder(List<FilterBuilder> filters, QueryBuilder queryBuilder, ElasticSearchElementType elementType) {
        SearchRequestBuilder searchRequestBuilder = super.getSearchRequestBuilder(filters, queryBuilder, elementType);
        addHistogramQueryToSearchRequestBuilder(searchRequestBuilder, histogramQueryItems);
        addTermsQueryToSearchRequestBuilder(searchRequestBuilder, termsQueryItems);
        addGeohashQueryToSearchRequestBuilder(searchRequestBuilder, geohashQueryItems);
        return searchRequestBuilder;
    }

    @Override
    protected QueryBuilder createQueryStringQuery(QueryStringQueryParameters queryParameters) {
        String queryString = queryParameters.getQueryString();
        if (queryString == null || queryString.equals("*")) {
            return QueryBuilders.matchAllQuery();
        }
        ElasticsearchSingleDocumentSearchIndex es = (ElasticsearchSingleDocumentSearchIndex) ((GraphBaseWithSearchIndex) getGraph()).getSearchIndex();
        Collection<String> fields = es.getQueryablePropertyNames(getGraph(), false, getParameters().getAuthorizations());
        QueryStringQueryBuilder qs = QueryBuilders.queryString(queryString);
        for (String field : fields) {
            qs = qs.field(field);
        }
        return qs;
    }

    @Override
    protected List<FilterBuilder> getFilters(ElasticSearchElementType elementType) {
        List<FilterBuilder> results = super.getFilters(elementType);
        if (getParameters() instanceof QueryStringQueryParameters) {
            String queryString = ((QueryStringQueryParameters) getParameters()).getQueryString();
            if (queryString == null || queryString.equals("*")) {
                ElasticsearchSingleDocumentSearchIndex es = (ElasticsearchSingleDocumentSearchIndex) ((GraphBaseWithSearchIndex) getGraph()).getSearchIndex();
                Collection<String> fields = es.getQueryablePropertyNames(getGraph(), true, getParameters().getAuthorizations());
                OrFilterBuilder atLeastOneFieldExistsFilter = new OrFilterBuilder();
                for (String field : fields) {
                    atLeastOneFieldExistsFilter.add(new ExistsFilterBuilder(field));
                }
                results.add(atLeastOneFieldExistsFilter);
            }
        }
        return results;
    }
}
