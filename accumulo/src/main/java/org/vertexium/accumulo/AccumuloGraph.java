package org.vertexium.accumulo;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.client.admin.TimeType;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.iterators.user.TimestampFilter;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.trace.DistributedTrace;
import org.apache.accumulo.core.trace.Span;
import org.apache.accumulo.core.trace.Trace;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.CreateMode;
import org.vertexium.Range;
import org.vertexium.*;
import org.vertexium.accumulo.iterator.*;
import org.vertexium.accumulo.iterator.model.EdgeInfo;
import org.vertexium.accumulo.iterator.model.IteratorFetchHints;
import org.vertexium.accumulo.iterator.util.ByteArrayWrapper;
import org.vertexium.accumulo.iterator.util.ByteSequenceUtils;
import org.vertexium.accumulo.keys.KeyHelper;
import org.vertexium.accumulo.util.*;
import org.vertexium.event.*;
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.historicalEvent.HistoricalEventId;
import org.vertexium.mutation.*;
import org.vertexium.property.MutableProperty;
import org.vertexium.property.StreamingPropertyValue;
import org.vertexium.property.StreamingPropertyValueRef;
import org.vertexium.search.IndexHint;
import org.vertexium.util.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.VertexiumObjectType.getTypeFromElement;
import static org.vertexium.util.IterableUtils.singleOrDefault;
import static org.vertexium.util.IterableUtils.toList;
import static org.vertexium.util.Preconditions.checkNotNull;
import static org.vertexium.util.StreamUtils.stream;
import static org.vertexium.util.StreamUtils.toIterable;

public class AccumuloGraph extends GraphBase implements Traceable {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(AccumuloGraph.class);
    static final AccumuloGraphLogger GRAPH_LOGGER = new AccumuloGraphLogger(QUERY_LOGGER);
    private static final String ROW_DELETING_ITERATOR_NAME = RowDeletingIterator.class.getSimpleName();
    private static final int ROW_DELETING_ITERATOR_PRIORITY = 7;
    private static final Object addIteratorLock = new Object();
    private static final Integer METADATA_ACCUMULO_GRAPH_VERSION = 2;
    private static final String METADATA_ACCUMULO_GRAPH_VERSION_KEY = "accumulo.graph.version";
    private static final String METADATA_SERIALIZER = "accumulo.graph.serializer";
    private static final String METADATA_STREAMING_PROPERTY_VALUE_DATA_WRITER = "accumulo.graph.streamingPropertyValueStorageStrategy";
    private static final Authorizations METADATA_AUTHORIZATIONS = new AccumuloAuthorizations();
    public static final int SINGLE_VERSION = 1;
    public static final Integer ALL_VERSIONS = null;
    private static final int ACCUMULO_DEFAULT_VERSIONING_ITERATOR_PRIORITY = 20;
    private static final String ACCUMULO_DEFAULT_VERSIONING_ITERATOR_NAME = "vers";
    private static final ColumnVisibility EMPTY_COLUMN_VISIBILITY = new ColumnVisibility();
    private static final String CLASSPATH_CONTEXT_NAME = "vertexium";
    private final Connector connector;
    private final VertexiumSerializer vertexiumSerializer;
    private final CuratorFramework curatorFramework;
    private final boolean historyInSeparateTable;
    private final StreamingPropertyValueStorageStrategy streamingPropertyValueStorageStrategy;
    private final MultiTableBatchWriter batchWriter;
    protected final ElementMutationBuilder elementMutationBuilder;
    private final Queue<GraphEvent> graphEventQueue = new LinkedList<>();
    private Integer accumuloGraphVersion;
    private boolean foundVertexiumSerializerMetadata;
    private boolean foundStreamingPropertyValueStorageStrategyMetadata;
    private final AccumuloNameSubstitutionStrategy nameSubstitutionStrategy;
    private final String verticesTableName;
    private final String historyVerticesTableName;
    private final String edgesTableName;
    private final String historyEdgesTableName;
    private final String extendedDataTableName;
    private final String dataTableName;
    private final String metadataTableName;
    private final int numberOfQueryThreads;
    private final AccumuloGraphMetadataStore graphMetadataStore;
    private boolean distributedTraceEnabled;

    protected AccumuloGraph(AccumuloGraphConfiguration config, Connector connector) {
        super(config);
        this.connector = connector;
        this.vertexiumSerializer = config.createSerializer(this);
        this.nameSubstitutionStrategy = AccumuloNameSubstitutionStrategy.create(config.createSubstitutionStrategy(this));
        this.streamingPropertyValueStorageStrategy = config.createStreamingPropertyValueStorageStrategy(this);
        this.elementMutationBuilder = new ElementMutationBuilder(streamingPropertyValueStorageStrategy, vertexiumSerializer) {
            @Override
            protected void saveVertexMutations(Mutation... m) {
                addMutations(VertexiumObjectType.VERTEX, m);
            }

            @Override
            protected void saveEdgeMutations(Mutation... m) {
                addMutations(VertexiumObjectType.EDGE, m);
            }

            @Override
            protected void saveExtendedDataMutation(ElementType elementType, Mutation m) {
                addMutations(VertexiumObjectType.EXTENDED_DATA, m);
            }

            @Override
            protected AccumuloNameSubstitutionStrategy getNameSubstitutionStrategy() {
                return AccumuloGraph.this.getNameSubstitutionStrategy();
            }

            @Override
            public void saveDataMutation(Mutation dataMutation) {
                _addMutations(getDataWriter(), dataMutation);
            }

            @Override
            @SuppressWarnings("unchecked")
            protected StreamingPropertyValueRef saveStreamingPropertyValue(String rowKey, Property property, StreamingPropertyValue propertyValue) {
                StreamingPropertyValueRef streamingPropertyValueRef = super.saveStreamingPropertyValue(rowKey, property, propertyValue);
                ((MutableProperty) property).setValue(streamingPropertyValueRef.toStreamingPropertyValue(AccumuloGraph.this, property.getTimestamp()));
                return streamingPropertyValueRef;
            }
        };
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        curatorFramework = CuratorFrameworkFactory.newClient(config.getZookeeperServers(), retryPolicy);
        curatorFramework.start();
        String zkPath = config.getZookeeperMetadataSyncPath();
        this.graphMetadataStore = new AccumuloGraphMetadataStore(curatorFramework, zkPath);
        this.verticesTableName = getVerticesTableName(getConfiguration().getTableNamePrefix());
        this.edgesTableName = getEdgesTableName(getConfiguration().getTableNamePrefix());
        this.extendedDataTableName = getExtendedDataTableName(getConfiguration().getTableNamePrefix());
        this.dataTableName = getDataTableName(getConfiguration().getTableNamePrefix());
        this.metadataTableName = getMetadataTableName(getConfiguration().getTableNamePrefix());
        this.numberOfQueryThreads = getConfiguration().getNumberOfQueryThreads();
        this.historyInSeparateTable = getConfiguration().isHistoryInSeparateTable();

        if (isHistoryInSeparateTable()) {
            this.historyVerticesTableName = getHistoryVerticesTableName(getConfiguration().getTableNamePrefix());
            this.historyEdgesTableName = getHistoryEdgesTableName(getConfiguration().getTableNamePrefix());
        } else {
            this.historyVerticesTableName = null;
            this.historyEdgesTableName = null;
        }

        BatchWriterConfig writerConfig = getConfiguration().createBatchWriterConfig();
        this.batchWriter = connector.createMultiTableBatchWriter(writerConfig);
    }

    public static AccumuloGraph create(AccumuloGraphConfiguration config) {
        if (config == null) {
            throw new IllegalArgumentException("config cannot be null");
        }
        Connector connector = config.createConnector();
        if (config.isHistoryInSeparateTable()) {
            ensureTableExists(connector, getVerticesTableName(config.getTableNamePrefix()), 1, config.getHdfsContextClasspath(), config.isCreateTables());
            ensureTableExists(connector, getEdgesTableName(config.getTableNamePrefix()), 1, config.getHdfsContextClasspath(), config.isCreateTables());

            ensureTableExists(connector, getHistoryVerticesTableName(config.getTableNamePrefix()), config.getMaxVersions(), config.getHdfsContextClasspath(), config.isCreateTables());
            ensureTableExists(connector, getHistoryEdgesTableName(config.getTableNamePrefix()), config.getMaxVersions(), config.getHdfsContextClasspath(), config.isCreateTables());
            ensureRowDeletingIteratorIsAttached(connector, getHistoryVerticesTableName(config.getTableNamePrefix()));
            ensureRowDeletingIteratorIsAttached(connector, getHistoryEdgesTableName(config.getTableNamePrefix()));
        } else {
            ensureTableExists(connector, getVerticesTableName(config.getTableNamePrefix()), config.getMaxVersions(), config.getHdfsContextClasspath(), config.isCreateTables());
            ensureTableExists(connector, getEdgesTableName(config.getTableNamePrefix()), config.getMaxVersions(), config.getHdfsContextClasspath(), config.isCreateTables());
        }
        ensureTableExists(connector, getExtendedDataTableName(config.getTableNamePrefix()), config.getExtendedDataMaxVersions(), config.getHdfsContextClasspath(), config.isCreateTables());
        ensureTableExists(connector, getDataTableName(config.getTableNamePrefix()), 1, config.getHdfsContextClasspath(), config.isCreateTables());
        ensureTableExists(connector, getMetadataTableName(config.getTableNamePrefix()), 1, config.getHdfsContextClasspath(), config.isCreateTables());
        ensureRowDeletingIteratorIsAttached(connector, getVerticesTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getEdgesTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getDataTableName(config.getTableNamePrefix()));
        ensureRowDeletingIteratorIsAttached(connector, getExtendedDataTableName(config.getTableNamePrefix()));
        AccumuloGraph graph = new AccumuloGraph(config, connector);
        graph.setup();
        return graph;
    }

    @Override
    protected void setup() {
        super.setup();
        if (accumuloGraphVersion == null) {
            setMetadata(METADATA_ACCUMULO_GRAPH_VERSION_KEY, METADATA_ACCUMULO_GRAPH_VERSION);
        } else if (!METADATA_ACCUMULO_GRAPH_VERSION.equals(accumuloGraphVersion)) {
            throw new VertexiumException("Invalid accumulo graph version. Expected " + METADATA_ACCUMULO_GRAPH_VERSION + " found " + accumuloGraphVersion);
        }
    }

    @Override
    protected void setupGraphMetadata() {
        foundVertexiumSerializerMetadata = false;
        super.setupGraphMetadata();
        if (!foundVertexiumSerializerMetadata) {
            setMetadata(METADATA_SERIALIZER, vertexiumSerializer.getClass().getName());
        }
        if (!foundStreamingPropertyValueStorageStrategyMetadata) {
            setMetadata(METADATA_STREAMING_PROPERTY_VALUE_DATA_WRITER, streamingPropertyValueStorageStrategy.getClass().getName());
        }
    }

    @Override
    protected void setupGraphMetadata(GraphMetadataEntry graphMetadataEntry) {
        super.setupGraphMetadata(graphMetadataEntry);
        if (graphMetadataEntry.getKey().equals(METADATA_ACCUMULO_GRAPH_VERSION_KEY)) {
            if (graphMetadataEntry.getValue() instanceof Integer) {
                accumuloGraphVersion = (Integer) graphMetadataEntry.getValue();
                LOGGER.info("%s=%s", METADATA_ACCUMULO_GRAPH_VERSION_KEY, accumuloGraphVersion);
            } else {
                throw new VertexiumException("Invalid accumulo version in metadata. " + graphMetadataEntry);
            }
        } else if (graphMetadataEntry.getKey().equals(METADATA_SERIALIZER)) {
            validateClassMetadataEntry(graphMetadataEntry, vertexiumSerializer.getClass());
            foundVertexiumSerializerMetadata = true;
        } else if (graphMetadataEntry.getKey().equals(METADATA_STREAMING_PROPERTY_VALUE_DATA_WRITER)) {
            validateClassMetadataEntry(graphMetadataEntry, streamingPropertyValueStorageStrategy.getClass());
            foundStreamingPropertyValueStorageStrategyMetadata = true;
        }
    }

    private void validateClassMetadataEntry(GraphMetadataEntry graphMetadataEntry, Class expectedClass) {
        if (!(graphMetadataEntry.getValue() instanceof String)) {
            throw new VertexiumException("Invalid " + graphMetadataEntry.getKey() + " expected string found " + graphMetadataEntry.getValue().getClass().getName());
        }
        String foundClassName = (String) graphMetadataEntry.getValue();
        if (!foundClassName.equals(expectedClass.getName())) {
            throw new VertexiumException("Invalid " + graphMetadataEntry.getKey() + " expected " + foundClassName + " found " + expectedClass.getName());
        }
    }

    protected static void ensureTableExists(Connector connector, String tableName, Integer maxVersions, String hdfsContextClasspath, boolean createTable) {
        try {
            if (!connector.tableOperations().exists(tableName)) {
                if (!createTable) {
                    throw new VertexiumException("Table '" + tableName + "' does not exist and 'graph." + GraphConfiguration.CREATE_TABLES + "' is set to false");
                }
                NewTableConfiguration ntc = new NewTableConfiguration()
                    .setTimeType(TimeType.MILLIS)
                    .withoutDefaultIterators();
                connector.tableOperations().create(tableName, ntc);

                if (maxVersions != null) {
                    // The following parameters match the Accumulo defaults for the VersioningIterator
                    IteratorSetting versioningSettings = new IteratorSetting(
                        ACCUMULO_DEFAULT_VERSIONING_ITERATOR_PRIORITY,
                        ACCUMULO_DEFAULT_VERSIONING_ITERATOR_NAME,
                        VersioningIterator.class
                    );
                    VersioningIterator.setMaxVersions(versioningSettings, maxVersions);
                    EnumSet<IteratorUtil.IteratorScope> scope = EnumSet.allOf(IteratorUtil.IteratorScope.class);
                    connector.tableOperations().attachIterator(tableName, versioningSettings, scope);
                }
            }

            if (hdfsContextClasspath != null) {
                connector.instanceOperations().setProperty("general.vfs.context.classpath." + CLASSPATH_CONTEXT_NAME + "-" + tableName, hdfsContextClasspath);
                connector.tableOperations().setProperty(tableName, "table.classpath.context", CLASSPATH_CONTEXT_NAME + "-" + tableName);
            }
        } catch (Exception e) {
            throw new VertexiumException("Unable to create table " + tableName, e);
        }
    }

    protected static void ensureRowDeletingIteratorIsAttached(Connector connector, String tableName) {
        try {
            synchronized (addIteratorLock) {
                IteratorSetting is = new IteratorSetting(ROW_DELETING_ITERATOR_PRIORITY, ROW_DELETING_ITERATOR_NAME, RowDeletingIterator.class);
                if (!connector.tableOperations().listIterators(tableName).containsKey(ROW_DELETING_ITERATOR_NAME)) {
                    try {
                        connector.tableOperations().attachIterator(tableName, is);
                    } catch (Exception ex) {
                        // If many processes are starting up at the same time (see YARN). It's possible that there will be a collision.
                        final int SLEEP_TIME = 5000;
                        LOGGER.warn("Failed to attach RowDeletingIterator. Retrying in %dms.", SLEEP_TIME);
                        Thread.sleep(SLEEP_TIME);
                        if (!connector.tableOperations().listIterators(tableName).containsKey(ROW_DELETING_ITERATOR_NAME)) {
                            connector.tableOperations().attachIterator(tableName, is);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new VertexiumException("Could not attach RowDeletingIterator", e);
        }
    }

    @SuppressWarnings("unchecked")
    public static AccumuloGraph create(Map config) {
        return create(new AccumuloGraphConfiguration(config));
    }

    @Override
    public AccumuloVertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility) {
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        final String finalVertexId = vertexId;
        return new AccumuloVertexBuilder(finalVertexId, visibility, elementMutationBuilder) {
            @Override
            public String save(User user) {
                return saveVertex(user).getId();
            }

            @Override
            public Vertex save(Authorizations authorizations) {
                return saveVertex(authorizations.getUser());
            }

            private Vertex saveVertex(User user) {
                Span trace = Trace.start("prepareVertex");
                trace.data("vertexId", finalVertexId);
                try {
                    // This has to occur before createVertex since it will mutate the properties
                    getElementMutationBuilder().saveVertexMutation(AccumuloGraph.this, this, timestampLong);

                    AccumuloVertex vertex = createVertex(user);

                    if (getIndexHint() != IndexHint.DO_NOT_INDEX) {
                        getSearchIndex().addElement(AccumuloGraph.this, vertex, user);
                        getSearchIndex().addElementExtendedData(
                            AccumuloGraph.this,
                            vertex,
                            getExtendedData(),
                            getAdditionalExtendedDataVisibilities(),
                            getAdditionalExtendedDataVisibilityDeletes(),
                            user
                        );

                        for (ExtendedDataDeleteMutation m : getExtendedDataDeletes()) {
                            getSearchIndex().deleteExtendedData(
                                AccumuloGraph.this,
                                vertex,
                                m.getTableName(),
                                m.getRow(),
                                m.getColumnName(),
                                m.getKey(),
                                m.getVisibility(),
                                user
                            );
                        }
                    }

                    if (hasEventListeners()) {
                        queueEvent(new AddVertexEvent(AccumuloGraph.this, vertex));
                        queueEvents(
                            vertex,
                            getProperties(),
                            getPropertyDeletes(),
                            getPropertySoftDeletes(),
                            getAdditionalVisibilities(),
                            getAdditionalVisibilityDeletes(),
                            getExtendedData(),
                            getExtendedDataDeletes(),
                            getAdditionalExtendedDataVisibilities(),
                            getAdditionalExtendedDataVisibilityDeletes()
                        );
                    }

                    return vertex;
                } finally {
                    trace.stop();
                }
            }

            @Override
            protected AccumuloVertex createVertex(User user) {
                Iterable<Visibility> hiddenVisibilities = null;
                return new AccumuloVertex(
                    AccumuloGraph.this,
                    getId(),
                    getVisibility(),
                    getProperties(),
                    getPropertyDeletes(),
                    getPropertySoftDeletes(),
                    hiddenVisibilities,
                    getAdditionalVisibilitiesAsStringSet(),
                    getExtendedDataTableNames(),
                    timestampLong,
                    FetchHints.ALL_INCLUDING_HIDDEN,
                    user
                );
            }
        };
    }

    private void queueEvents(
        Element element,
        Iterable<Property> properties,
        Iterable<PropertyDeleteMutation> propertyDeletes,
        Iterable<PropertySoftDeleteMutation> propertySoftDeletes,
        Iterable<AdditionalVisibilityAddMutation> additionalVisibilities,
        Iterable<AdditionalVisibilityDeleteMutation> additionalVisibilityDeletes,
        Iterable<ExtendedDataMutation> extendedData,
        Iterable<ExtendedDataDeleteMutation> extendedDataDeletes,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes
    ) {
        if (properties != null) {
            for (Property property : properties) {
                queueEvent(new AddPropertyEvent(this, element, property));
            }
        }
        if (propertyDeletes != null) {
            for (PropertyDeleteMutation propertyDeleteMutation : propertyDeletes) {
                queueEvent(new DeletePropertyEvent(this, element, propertyDeleteMutation));
            }
        }
        if (propertySoftDeletes != null) {
            for (PropertySoftDeleteMutation propertySoftDeleteMutation : propertySoftDeletes) {
                queueEvent(new SoftDeletePropertyEvent(this, element, propertySoftDeleteMutation));
            }
        }
        if (additionalVisibilities != null) {
            for (AdditionalVisibilityAddMutation additionalVisibilityAddMutation : additionalVisibilities) {
                queueEvent(new AddAdditionalVisibilityEvent(this, element, additionalVisibilityAddMutation));
            }
        }
        if (additionalVisibilities != null) {
            for (AdditionalVisibilityDeleteMutation additionalVisibilityDeleteMutation : additionalVisibilityDeletes) {
                queueEvent(new DeleteAdditionalVisibilityEvent(this, element, additionalVisibilityDeleteMutation));
            }
        }
        if (extendedData != null) {
            for (ExtendedDataMutation extendedDataMutation : extendedData) {
                queueEvent(new AddExtendedDataEvent(
                    this,
                    element,
                    extendedDataMutation.getTableName(),
                    extendedDataMutation.getRow(),
                    extendedDataMutation.getColumnName(),
                    extendedDataMutation.getKey(),
                    extendedDataMutation.getValue(),
                    extendedDataMutation.getVisibility()
                ));
            }
        }
        if (extendedDataDeletes != null) {
            for (ExtendedDataDeleteMutation extendedDataDeleteMutation : extendedDataDeletes) {
                queueEvent(new DeleteExtendedDataEvent(
                    this,
                    element,
                    extendedDataDeleteMutation.getTableName(),
                    extendedDataDeleteMutation.getRow(),
                    extendedDataDeleteMutation.getColumnName(),
                    extendedDataDeleteMutation.getKey()
                ));
            }
        }
        if (additionalExtendedDataVisibilities != null) {
            for (AdditionalExtendedDataVisibilityAddMutation additionalExtendedDataVisibility : additionalExtendedDataVisibilities) {
                queueEvent(new AddAdditionalExtendedDataVisibilityEvent(
                    this,
                    element,
                    additionalExtendedDataVisibility.getTableName(),
                    additionalExtendedDataVisibility.getRow(),
                    additionalExtendedDataVisibility.getAdditionalVisibility()
                ));
            }
        }
        if (additionalExtendedDataVisibilityDeletes != null) {
            for (AdditionalExtendedDataVisibilityDeleteMutation additionalExtendedDataVisibilityDelete : additionalExtendedDataVisibilityDeletes) {
                queueEvent(new DeleteAdditionalExtendedDataVisibilityEvent(
                    this,
                    element,
                    additionalExtendedDataVisibilityDelete.getTableName(),
                    additionalExtendedDataVisibilityDelete.getRow(),
                    additionalExtendedDataVisibilityDelete.getAdditionalVisibility()
                ));
            }
        }
    }

    private void queueEvent(GraphEvent graphEvent) {
        synchronized (this.graphEventQueue) {
            this.graphEventQueue.add(graphEvent);
        }
    }

    void savePropertiesAndAdditionalVisibilities(
        AccumuloElement element,
        Iterable<Property> properties,
        Iterable<PropertyDeleteMutation> propertyDeletes,
        Iterable<PropertySoftDeleteMutation> propertySoftDeletes,
        Iterable<AdditionalVisibilityAddMutation> additionalVisibilities,
        Iterable<AdditionalVisibilityDeleteMutation> additionalVisibilityDeletes
    ) {
        String elementRowKey = element.getId();
        Mutation m = new Mutation(elementRowKey);
        boolean hasProperty = false;
        for (PropertyDeleteMutation propertyDelete : propertyDeletes) {
            hasProperty = true;
            elementMutationBuilder.addPropertyDeleteToMutation(m, propertyDelete);
        }
        for (PropertySoftDeleteMutation propertySoftDelete : propertySoftDeletes) {
            hasProperty = true;
            elementMutationBuilder.addPropertySoftDeleteToMutation(m, propertySoftDelete);
        }
        for (Property property : properties) {
            hasProperty = true;
            elementMutationBuilder.addPropertyToMutation(this, m, elementRowKey, property);
        }
        if (hasProperty) {
            addMutations(element, m);
        }
        for (AdditionalVisibilityAddMutation additionalVisibility : additionalVisibilities) {
            elementMutationBuilder.addAdditionalVisibilityToMutation(m, additionalVisibility);
        }
        for (AdditionalVisibilityDeleteMutation additionalVisibilityDelete : additionalVisibilityDeletes) {
            elementMutationBuilder.addAdditionalVisibilityDeleteToMutation(m, additionalVisibilityDelete);
        }

        if (hasEventListeners()) {
            queueEvents(
                element,
                properties,
                propertyDeletes,
                propertySoftDeletes,
                additionalVisibilities,
                additionalVisibilityDeletes,
                null,
                null,
                null,
                null
            );
        }
    }

    void deleteProperty(AccumuloElement element, Property property, Authorizations authorizations) {
        if (!element.getFetchHints().isIncludePropertyAndMetadata(property.getName())) {
            throw new VertexiumMissingFetchHintException(element.getFetchHints(), "Property " + property.getName() + " needs to be included with metadata");
        }

        Mutation m = new Mutation(element.getId());
        elementMutationBuilder.addPropertyDeleteToMutation(m, property);
        addMutations(element, m);

        getSearchIndex().deleteProperty(
            this,
            element,
            PropertyDescriptor.fromProperty(property),
            authorizations
        );

        if (hasEventListeners()) {
            queueEvent(new DeletePropertyEvent(this, element, property));
        }
    }

    void softDeleteProperty(AccumuloElement element, Property property, Object data, Authorizations authorizations) {
        Mutation m = new Mutation(element.getId());
        elementMutationBuilder.addPropertySoftDeleteToMutation(m, property, IncreasingTime.currentTimeMillis(), data);
        addMutations(element, m);

        getSearchIndex().deleteProperty(
            this,
            element,
            PropertyDescriptor.fromProperty(property),
            authorizations
        );

        if (hasEventListeners()) {
            queueEvent(new SoftDeletePropertyEvent(this, element, property, data));
        }
    }

    protected void addMutations(Element element, Mutation... mutations) {
        addMutations(getTypeFromElement(element), mutations);
    }

    protected void addMutations(VertexiumObjectType objectType, Mutation... mutations) {
        _addMutations(getWriterFromElementType(objectType), mutations);
        if (isHistoryInSeparateTable() && objectType != VertexiumObjectType.EXTENDED_DATA) {
            _addMutations(getHistoryWriterFromElementType(objectType), mutations);
        }
    }

    protected void _addMutations(BatchWriter writer, Mutation... mutations) {
        try {
            for (Mutation mutation : mutations) {
                writer.addMutation(mutation);
            }
            if (getConfiguration().isAutoFlush()) {
                flush();
            }
        } catch (MutationsRejectedException ex) {
            throw new VertexiumException("Could not add mutation", ex);
        }
    }

    public BatchWriter getVerticesWriter() {
        return getWriterForTable(getVerticesTableName());
    }

    private BatchWriter getWriterForTable(String tableName) {
        try {
            return batchWriter.getBatchWriter(tableName);
        } catch (Exception e) {
            throw new VertexiumException("Unable to get writer for table " + tableName, e);
        }
    }

    public BatchWriter getHistoryVerticesWriter() {
        return getWriterForTable(getHistoryVerticesTableName());
    }

    public BatchWriter getEdgesWriter() {
        return getWriterForTable(getEdgesTableName());
    }

    public BatchWriter getHistoryEdgesWriter() {
        return getWriterForTable(getHistoryEdgesTableName());
    }

    public BatchWriter getExtendedDataWriter() {
        return getWriterForTable(getExtendedDataTableName());
    }

    public BatchWriter getDataWriter() {
        return getWriterForTable(getDataTableName());
    }

    public BatchWriter getWriterFromElementType(VertexiumObjectType objectType) {
        switch (objectType) {
            case VERTEX:
                return getVerticesWriter();
            case EDGE:
                return getEdgesWriter();
            case EXTENDED_DATA:
                return getExtendedDataWriter();
            default:
                throw new VertexiumException("Unexpected object type: " + objectType);
        }
    }

    public BatchWriter getHistoryWriterFromElementType(VertexiumObjectType objectType) {
        switch (objectType) {
            case VERTEX:
                return getHistoryVerticesWriter();
            case EDGE:
                return getHistoryEdgesWriter();
            default:
                throw new VertexiumException("Unexpected element type: " + objectType);
        }
    }

    protected BatchWriter getMetadataWriter() {
        return getWriterForTable(getMetadataTableName());
    }

    @Override
    public Stream<Vertex> getVertices(FetchHints fetchHints, Long endTime, User user) throws VertexiumException {
        Span trace = Trace.start("getVertices");
        return getVerticesInRange(trace, null, null, fetchHints, endTime, user);
    }

    void deleteElement(Element element, User user) {
        checkNotNull(element, "element cannot be null");
        VertexiumObjectType elementType = getTypeFromElement(element);

        Span trace = Trace.start("deleteElement");
        trace.data("elementType", elementType.name());
        trace.data("elementId", element.getId());
        try {
            GraphEvent deleteEvent;

            if (elementType == VertexiumObjectType.EDGE) {
                Edge edge = (Edge) element;

                ColumnVisibility visibility = visibilityToAccumuloVisibility(edge.getVisibility());

                Mutation outMutation = new Mutation(edge.getVertexId(Direction.OUT));
                outMutation.putDelete(AccumuloVertex.CF_OUT_EDGE, new Text(edge.getId()), visibility);

                Mutation inMutation = new Mutation(edge.getVertexId(Direction.IN));
                inMutation.putDelete(AccumuloVertex.CF_IN_EDGE, new Text(edge.getId()), visibility);

                addMutations(VertexiumObjectType.VERTEX, outMutation, inMutation);

                deleteEvent = new DeleteEdgeEvent(this, edge);
            } else if (elementType == VertexiumObjectType.VERTEX) {
                Vertex vertex = (Vertex) element;
                vertex.getEdges(Direction.BOTH, user).forEach(edge -> deleteElement(edge, user));
                deleteEvent = new DeleteVertexEvent(this, vertex);
            } else {
                throw new VertexiumException("Unsupported object type: " + elementType.name());
            }

            deleteAllExtendedDataForElement(element, user);
            addMutations(elementType, elementMutationBuilder.getDeleteRowMutation(element.getId()));
            getSearchIndex().deleteElement(this, element, user);
            if (hasEventListeners()) {
                queueEvent(deleteEvent);
            }
        } finally {
            trace.stop();
        }
    }

    private Mutation[] getDeleteExtendedDataMutations(ExtendedDataRowId rowId) {
        Mutation[] mutations = new Mutation[1];
        Text rowKey = KeyHelper.createExtendedDataRowKey(rowId);
        Mutation m = new Mutation(rowKey);
        m.put(AccumuloElement.DELETE_ROW_COLUMN_FAMILY, AccumuloElement.DELETE_ROW_COLUMN_QUALIFIER, RowDeletingIterator.DELETE_ROW_VALUE);
        mutations[0] = m;
        return mutations;
    }

    @Override
    public void softDeleteVertex(Vertex vertex, Long timestamp, Object eventData, Authorizations authorizations) {
        checkNotNull(vertex, "vertex cannot be null");
        Span trace = Trace.start("softDeleteVertex");
        trace.data("vertexId", vertex.getId());
        try {
            if (timestamp == null) {
                timestamp = IncreasingTime.currentTimeMillis();
            }

            getSearchIndex().deleteElement(this, vertex, authorizations);

            // Delete all edges that this vertex participates.
            for (Edge edge : vertex.getEdges(Direction.BOTH, authorizations)) {
                softDeleteEdge(edge, timestamp, eventData, authorizations);
            }

            addMutations(VertexiumObjectType.VERTEX, elementMutationBuilder.getSoftDeleteRowMutation(vertex.getId(), timestamp, eventData));

            if (hasEventListeners()) {
                queueEvent(new SoftDeleteVertexEvent(this, vertex, eventData));
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public void markVertexHidden(Vertex vertex, Visibility visibility, Object eventData, Authorizations authorizations) {
        checkNotNull(vertex, "vertex cannot be null");
        Span trace = Trace.start("markVertexHidden");
        trace.data("vertexId", vertex.getId());
        try {
            ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

            // Delete all edges that this vertex participates.
            for (Edge edge : vertex.getEdges(Direction.BOTH, authorizations)) {
                markEdgeHidden(edge, visibility, eventData, authorizations);
            }

            addMutations(VertexiumObjectType.VERTEX, elementMutationBuilder.getMarkHiddenRowMutation(vertex.getId(), columnVisibility, eventData));

            getSearchIndex().markElementHidden(this, vertex, visibility, authorizations);

            if (hasEventListeners()) {
                queueEvent(new MarkHiddenVertexEvent(this, vertex, eventData));
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public void markVertexVisible(Vertex vertex, Visibility visibility, Object eventData, Authorizations authorizations) {
        checkNotNull(vertex, "vertex cannot be null");
        Span trace = Trace.start("markVertexVisible");
        trace.data("vertexId", vertex.getId());
        try {
            ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

            // Delete all edges that this vertex participates.
            for (Edge edge : vertex.getEdges(Direction.BOTH, FetchHints.ALL_INCLUDING_HIDDEN, authorizations)) {
                markEdgeVisible(edge, visibility, authorizations);
            }

            addMutations(VertexiumObjectType.VERTEX, elementMutationBuilder.getMarkVisibleRowMutation(vertex.getId(), columnVisibility, eventData));

            getSearchIndex().markElementVisible(this, vertex, visibility, authorizations);

            if (hasEventListeners()) {
                queueEvent(new MarkVisibleVertexEvent(this, vertex, eventData));
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public AccumuloEdgeBuilderByVertexId prepareEdge(String edgeId, String outVertexId, String inVertexId, String label, Long timestamp, Visibility visibility) {
        checkNotNull(outVertexId, "outVertexId cannot be null");
        checkNotNull(inVertexId, "inVertexId cannot be null");
        checkNotNull(label, "label cannot be null");
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        final String finalEdgeId = edgeId;
        return new AccumuloEdgeBuilderByVertexId(finalEdgeId, outVertexId, inVertexId, label, visibility, elementMutationBuilder) {
            @Override
            public String save(User user) {
                return saveEdge(user).getId();
            }

            @Override
            public Edge save(Authorizations authorizations) {
                return saveEdge(authorizations.getUser());
            }

            private Edge saveEdge(User user) {
                Span trace = Trace.start("prepareEdge");
                trace.data("edgeId", finalEdgeId);
                try {
                    // This has to occur before createEdge since it will mutate the properties
                    elementMutationBuilder.saveEdgeMutation(AccumuloGraph.this, this, timestampLong);

                    AccumuloEdge edge = AccumuloGraph.this.createEdge(
                            AccumuloGraph.this,
                            this,
                            timestampLong,
                            FetchHints.ALL_INCLUDING_HIDDEN,
                            user
                    );
                    return savePreparedEdge(this, edge, null, user);
                } finally {
                    trace.stop();
                }
            }

            @Override
            protected AccumuloEdge createEdge(User user) {
                return AccumuloGraph.this.createEdge(
                    AccumuloGraph.this,
                    this,
                    timestampLong,
                    FetchHints.ALL_INCLUDING_HIDDEN,
                    user
                );
            }
        };
    }

    @Override
    public EdgeBuilder prepareEdge(String edgeId, Vertex outVertex, Vertex inVertex, String label, Long timestamp, Visibility visibility) {
        checkNotNull(outVertex, "outVertex cannot be null");
        checkNotNull(inVertex, "inVertex cannot be null");
        checkNotNull(label, "label cannot be null");
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        final long timestampLong = timestamp;

        final String finalEdgeId = edgeId;
        return new EdgeBuilder(finalEdgeId, outVertex, inVertex, label, visibility) {
            @Override
            public String save(User user) {
                return saveEdge(user).getId();
            }

            @Override
            public Edge save(Authorizations authorizations) {
                return saveEdge(authorizations.getUser());
            }

            private Edge saveEdge(User user) {
                Span trace = Trace.start("prepareEdge");
                trace.data("edgeId", finalEdgeId);
                try {
                    AddEdgeToVertexRunnable addEdgeToVertex = new AddEdgeToVertexRunnable() {
                        @Override
                        public void run(AccumuloEdge edge) {
                            if (getOutVertex() instanceof AccumuloVertex) {
                                ((AccumuloVertex) getOutVertex()).addOutEdge(edge);
                            }
                            if (getInVertex() instanceof AccumuloVertex) {
                                ((AccumuloVertex) getInVertex()).addInEdge(edge);
                            }
                        }
                    };

                    // This has to occur before createEdge since it will mutate the properties
                    elementMutationBuilder.saveEdgeMutation(AccumuloGraph.this, this, timestampLong);

                    AccumuloEdge edge = createEdge(
                        AccumuloGraph.this,
                        this,
                        timestampLong,
                        FetchHints.ALL_INCLUDING_HIDDEN,
                        user
                    );
                    return savePreparedEdge(this, edge, addEdgeToVertex, user);
                } finally {
                    trace.stop();
                }
            }
        };
    }

    private AccumuloEdge createEdge(
        AccumuloGraph accumuloGraph,
        EdgeBuilderBase edgeBuilder,
        long timestamp,
        FetchHints fetchHints,
        User user
    ) {
        Iterable<Visibility> hiddenVisibilities = null;
        return new AccumuloEdge(
            accumuloGraph,
            edgeBuilder.getId(),
            edgeBuilder.getVertexId(Direction.OUT),
            edgeBuilder.getVertexId(Direction.IN),
            edgeBuilder.getEdgeLabel(),
            edgeBuilder.getNewEdgeLabel(),
            edgeBuilder.getVisibility(),
            edgeBuilder.getProperties(),
            edgeBuilder.getPropertyDeletes(),
            edgeBuilder.getPropertySoftDeletes(),
            hiddenVisibilities,
            edgeBuilder.getAdditionalVisibilitiesAsStringSet(),
            edgeBuilder.getExtendedDataTableNames(),
            timestamp,
            fetchHints,
            user
        );
    }

    private Edge savePreparedEdge(
        EdgeBuilderBase edgeBuilder,
        AccumuloEdge edge,
        AddEdgeToVertexRunnable addEdgeToVertex,
        User user
    ) {
        if (addEdgeToVertex != null) {
            addEdgeToVertex.run(edge);
        }

        if (edgeBuilder.getIndexHint() != IndexHint.DO_NOT_INDEX) {
            getSearchIndex().addElement(AccumuloGraph.this, edge, user);
            getSearchIndex().addElementExtendedData(
                AccumuloGraph.this,
                edge,
                edgeBuilder.getExtendedData(),
                edgeBuilder.getAdditionalExtendedDataVisibilities(),
                edgeBuilder.getAdditionalExtendedDataVisibilityDeletes(),
                user
            );
            for (ExtendedDataDeleteMutation m : edgeBuilder.getExtendedDataDeletes()) {
                getSearchIndex().deleteExtendedData(
                    AccumuloGraph.this,
                    edge,
                    m.getTableName(),
                    m.getRow(),
                    m.getColumnName(),
                    m.getKey(),
                    m.getVisibility(),
                    user
                );
            }
        }

        if (hasEventListeners()) {
            queueEvent(new AddEdgeEvent(this, edge));
            queueEvents(
                edge,
                edgeBuilder.getProperties(),
                edgeBuilder.getPropertyDeletes(),
                edgeBuilder.getPropertySoftDeletes(),
                edgeBuilder.getAdditionalVisibilities(),
                edgeBuilder.getAdditionalVisibilityDeletes(),
                edgeBuilder.getExtendedData(),
                edgeBuilder.getExtendedDataDeletes(),
                edgeBuilder.getAdditionalExtendedDataVisibilities(),
                edgeBuilder.getAdditionalExtendedDataVisibilityDeletes()
            );
        }

        return edge;
    }

    public AccumuloNameSubstitutionStrategy getNameSubstitutionStrategy() {
        return nameSubstitutionStrategy;
    }

    @Override
    public Stream<HistoricalEvent> getHistoricalEvents(
        Iterable<ElementId> elementIds,
        HistoricalEventId after,
        HistoricalEventsFetchHints fetchHints,
        User user
    ) {
        Span trace = Trace.start("getHistoricalEvents");
        if (Trace.isTracing()) {
            trace.data("fetchHints", fetchHints.toString());
        }

        try {
            Map<ElementType, List<ElementId>> elementIdsByType = stream(elementIds)
                .collect(Collectors.groupingBy(ElementId::getElementType));
            return fetchHints.applyToResults(elementIdsByType.entrySet().stream()
                .flatMap(entry -> {
                    Set<String> ids = entry.getValue().stream().map(ElementId::getElementId).collect(Collectors.toSet());
                    return getHistoricalEvents(entry.getKey(), ids, after, fetchHints, user);
                }), after);
        } finally {
            trace.stop();
        }
    }

    private Stream<HistoricalEvent> getHistoricalEvents(
        ElementType elementType,
        Set<String> elementIds,
        HistoricalEventId after,
        HistoricalEventsFetchHints fetchHints,
        User user
    ) {
        FetchHints elementFetchHints = new FetchHintsBuilder()
            .setIncludeAllProperties(true)
            .setIncludeAllPropertyMetadata(true)
            .setIncludeHidden(true)
            .setIncludeAllEdgeRefs(true)
            .build();
        Collection<org.apache.accumulo.core.data.Range> ranges = elementIds.stream()
            .map(RangeUtils::createRangeFromString)
            .collect(Collectors.toList());
        ScannerBase scanner = createElementScanner(
            elementFetchHints,
            elementType,
            ALL_VERSIONS,
            null,
            null,
            ranges,
            false,
            user
        );
        IteratorSetting historicalEventsIteratorSettings = new IteratorSetting(
            100000,
            HistoricalEventsIterator.class.getSimpleName(),
            HistoricalEventsIterator.class
        );
        HistoricalEventsIterator.setFetchHints(historicalEventsIteratorSettings, HistoricalEventsIteratorConverter.convertToIteratorHistoricalEventsFetchHints(fetchHints));
        HistoricalEventsIterator.setElementType(historicalEventsIteratorSettings, HistoricalEventsIteratorConverter.convertToIteratorElementType(elementType));
        HistoricalEventsIterator.setAfter(historicalEventsIteratorSettings, HistoricalEventsIteratorConverter.convertToIteratorHistoricalEventId(after));
        scanner.addScanIterator(historicalEventsIteratorSettings);

        return ScannerStreamUtils.stream(scanner)
            .flatMap(r -> {
                try {
                    String elementId = r.getKey().getRow().toString();
                    return HistoricalEventsIterator.decode(r.getValue(), elementId).stream()
                        .map(iterEvent -> HistoricalEventsIteratorConverter.convert(
                            this,
                            elementType,
                            elementId,
                            iterEvent,
                            fetchHints
                        ));
                } catch (IOException ex) {
                    throw new VertexiumException("Could not decode row", ex);
                }
            });
    }

    @Override
    public List<InputStream> getStreamingPropertyValueInputStreams(List<StreamingPropertyValue> streamingPropertyValues) {
        if (streamingPropertyValues.size() == 0) {
            return Collections.emptyList();
        }
        return streamingPropertyValueStorageStrategy.getInputStreams(streamingPropertyValues);
    }

    @Override
    public Stream<ExtendedDataRow> getExtendedData(Iterable<ExtendedDataRowId> ids, FetchHints fetchHints, User user) {
        List<org.apache.accumulo.core.data.Range> ranges = extendedDataRowIdToRange(ids);
        Span trace = Trace.start("getExtendedData");
        return getExtendedDataRowsInRange(trace, ranges, fetchHints, user);
    }

    @Override
    public Stream<ExtendedDataRow> getExtendedData(
        ElementType elementType,
        String elementId,
        String tableName,
        FetchHints fetchHints,
        User user
    ) {
        try {
            Span trace = Trace.start("getExtendedData");
            trace.data("elementType", elementType == null ? null : elementType.name());
            trace.data("elementId", elementId);
            trace.data("tableName", tableName);
            org.apache.accumulo.core.data.Range range = org.apache.accumulo.core.data.Range.prefix(KeyHelper.createExtendedDataRowKey(elementType, elementId, tableName, null));
            return getExtendedDataRowsInRange(trace, Lists.newArrayList(range), fetchHints, user);
        } catch (IllegalStateException ex) {
            throw new VertexiumException("Failed to get extended data: " + elementType + ":" + elementId + ":" + tableName, ex);
        } catch (RuntimeException ex) {
            if (ex.getCause() instanceof AccumuloSecurityException) {
                throw new SecurityVertexiumException("Could not get extended data " + elementType + ":" + elementId + ":" + tableName + " with user: " + user, user, ex.getCause());
            }
            throw ex;
        }
    }

    @Override
    public Stream<ExtendedDataRow> getExtendedDataInRange(ElementType elementType, Range elementIdRange, User user) {
        Range extendedDataRowKeyRange = KeyHelper.createExtendedDataRowKeyRange(elementType, elementIdRange);
        return getExtendedDataInRange(extendedDataRowKeyRange, user);
    }

    public Stream<ExtendedDataRow> getExtendedDataInRange(Range extendedDataRowKeyRange, User user) {
        Span trace = Trace.start("getExtendedDataInRange");
        trace.data("rangeInclusiveStart", extendedDataRowKeyRange.getInclusiveStart());
        trace.data("rangeExclusiveStart", extendedDataRowKeyRange.getExclusiveEnd());

        org.apache.accumulo.core.data.Range range = vertexiumRangeToAccumuloRange(extendedDataRowKeyRange);
        return getExtendedDataRowsInRange(trace, Collections.singletonList(range), FetchHints.ALL, user);
    }

    private List<org.apache.accumulo.core.data.Range> extendedDataRowIdToRange(Iterable<ExtendedDataRowId> ids) {
        return stream(ids)
            .map(id -> org.apache.accumulo.core.data.Range.prefix(KeyHelper.createExtendedDataRowKey(id)))
            .collect(Collectors.toList());
    }

    void saveExtendedDataMutations(
        Element element,
        ElementType elementType,
        IndexHint indexHint,
        Iterable<ExtendedDataMutation> extendedData,
        Iterable<ExtendedDataDeleteMutation> extendedDataDeletes,
        Iterable<AdditionalExtendedDataVisibilityAddMutation> additionalExtendedDataVisibilities,
        Iterable<AdditionalExtendedDataVisibilityDeleteMutation> additionalExtendedDataVisibilityDeletes,
        User user
    ) {
        if (extendedData == null) {
            return;
        }

        String elementId = element.getId();
        elementMutationBuilder.saveExtendedDataMarkers(elementId, elementType, extendedData);
        elementMutationBuilder.saveExtendedData(
            this,
            elementId,
            elementType,
            extendedData,
            extendedDataDeletes,
            additionalExtendedDataVisibilities,
            additionalExtendedDataVisibilityDeletes
        );

        if (indexHint != IndexHint.DO_NOT_INDEX) {
            getSearchIndex().addElementExtendedData(
                this,
                element,
                extendedData,
                additionalExtendedDataVisibilities,
                additionalExtendedDataVisibilityDeletes,
                user
            );
            for (ExtendedDataDeleteMutation m : extendedDataDeletes) {
                getSearchIndex().deleteExtendedData(
                    this,
                    element,
                    m.getTableName(),
                    m.getRow(),
                    m.getColumnName(),
                    m.getKey(),
                    m.getVisibility(),
                    user
                );
            }
        }

        if (hasEventListeners()) {
            queueEvents(
                element,
                null,
                null,
                null,
                null,
                null,
                extendedData,
                extendedDataDeletes,
                additionalExtendedDataVisibilities,
                additionalExtendedDataVisibilityDeletes
            );
        }
    }

    private static abstract class AddEdgeToVertexRunnable {
        public abstract void run(AccumuloEdge edge);
    }

    @Override
    public Stream<Edge> getEdges(FetchHints fetchHints, Long endTime, User user) {
        Span trace = Trace.start("getEdges");
        return getEdgesInRange(trace, null, null, fetchHints, endTime, user);
    }

    @Override
    public void deleteEdge(Edge edge, Authorizations authorizations) {
        deleteElement(edge, authorizations.getUser());
    }

    @Override
    public void deleteExtendedDataRow(ExtendedDataRowId rowId, Authorizations authorizations) {
        checkNotNull(rowId);
        Span trace = Trace.start("deleteExtendedDataRow");
        trace.data("rowId", rowId.toString());
        try {
            getSearchIndex().deleteExtendedData(this, rowId, authorizations);

            addMutations(VertexiumObjectType.EXTENDED_DATA, getDeleteExtendedDataMutations(rowId));

            if (hasEventListeners()) {
                queueEvent(new DeleteExtendedDataRowEvent(this, rowId));
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public void softDeleteEdge(Edge edge, Long timestamp, Object eventData, Authorizations authorizations) {
        checkNotNull(edge);
        Span trace = Trace.start("softDeleteEdge");
        trace.data("edgeId", edge.getId());
        try {
            if (timestamp == null) {
                timestamp = IncreasingTime.currentTimeMillis();
            }

            getSearchIndex().deleteElement(this, edge, authorizations);

            ColumnVisibility visibility = visibilityToAccumuloVisibility(edge.getVisibility());

            Value value = elementMutationBuilder.toSoftDeleteDataToValue(eventData);

            Mutation outMutation = new Mutation(edge.getVertexId(Direction.OUT));
            outMutation.put(AccumuloVertex.CF_OUT_EDGE_SOFT_DELETE, new Text(edge.getId()), visibility, timestamp, value);

            Mutation inMutation = new Mutation(edge.getVertexId(Direction.IN));
            inMutation.put(AccumuloVertex.CF_IN_EDGE_SOFT_DELETE, new Text(edge.getId()), visibility, timestamp, value);

            addMutations(VertexiumObjectType.VERTEX, outMutation, inMutation);

            // Soft deletes everything else related to edge.
            addMutations(VertexiumObjectType.EDGE, elementMutationBuilder.getSoftDeleteRowMutation(edge.getId(), timestamp, eventData));

            if (hasEventListeners()) {
                queueEvent(new SoftDeleteEdgeEvent(this, edge, eventData));
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public void markEdgeHidden(Edge edge, Visibility visibility, Object eventData, Authorizations authorizations) {
        checkNotNull(edge);
        Span trace = Trace.start("markEdgeHidden");
        trace.data("edgeId", edge.getId());
        try {
            Vertex out = edge.getVertex(Direction.OUT, authorizations);
            if (out == null) {
                throw new VertexiumException(String.format("Unable to mark edge hidden %s, can't find out vertex %s", edge.getId(), edge.getVertexId(Direction.OUT)));
            }
            Vertex in = edge.getVertex(Direction.IN, authorizations);
            if (in == null) {
                throw new VertexiumException(String.format("Unable to mark edge hidden %s, can't find in vertex %s", edge.getId(), edge.getVertexId(Direction.IN)));
            }

            ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);
            addMutations(
                VertexiumObjectType.VERTEX,
                elementMutationBuilder.getMarkHiddenOutEdgeMutation(out, edge, columnVisibility, eventData),
                elementMutationBuilder.getMarkHiddenInEdgeMutation(in, edge, columnVisibility, eventData)
            );

            // Delete everything else related to edge.
            addMutations(VertexiumObjectType.EDGE, elementMutationBuilder.getMarkHiddenRowMutation(edge.getId(), columnVisibility, eventData));

            if (out instanceof AccumuloVertex) {
                ((AccumuloVertex) out).removeOutEdge(edge);
            }
            if (in instanceof AccumuloVertex) {
                ((AccumuloVertex) in).removeInEdge(edge);
            }

            getSearchIndex().markElementHidden(this, edge, visibility, authorizations);

            if (hasEventListeners()) {
                queueEvent(new MarkHiddenEdgeEvent(this, edge, eventData));
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public void markEdgeVisible(Edge edge, Visibility visibility, Object eventData, Authorizations authorizations) {
        checkNotNull(edge);
        Span trace = Trace.start("markEdgeVisible");
        trace.data("edgeId", edge.getId());
        try {
            Vertex out = edge.getVertex(Direction.OUT, FetchHints.ALL_INCLUDING_HIDDEN, authorizations);
            if (out == null) {
                throw new VertexiumException(String.format("Unable to mark edge visible %s, can't find out vertex %s", edge.getId(), edge.getVertexId(Direction.OUT)));
            }
            Vertex in = edge.getVertex(Direction.IN, FetchHints.ALL_INCLUDING_HIDDEN, authorizations);
            if (in == null) {
                throw new VertexiumException(String.format("Unable to mark edge visible %s, can't find in vertex %s", edge.getId(), edge.getVertexId(Direction.IN)));
            }

            ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

            addMutations(
                VertexiumObjectType.VERTEX,
                elementMutationBuilder.getMarkVisibleOutEdgeMutation(out, edge, columnVisibility, eventData),
                elementMutationBuilder.getMarkHiddenInEdgeMutation(in, edge, columnVisibility, eventData)
            );

            // Delete everything else related to edge.
            addMutations(VertexiumObjectType.EDGE, elementMutationBuilder.getMarkVisibleRowMutation(edge.getId(), columnVisibility, eventData));

            if (out instanceof AccumuloVertex) {
                ((AccumuloVertex) out).addOutEdge(edge);
            }
            if (in instanceof AccumuloVertex) {
                ((AccumuloVertex) in).addInEdge(edge);
            }

            getSearchIndex().markElementVisible(this, edge, visibility, authorizations);

            if (hasEventListeners()) {
                queueEvent(new MarkVisibleEdgeEvent(this, edge, eventData));
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public Authorizations createAuthorizations(String... auths) {
        return new AccumuloAuthorizations(auths);
    }

    public void markPropertyHidden(
        AccumuloElement element,
        Property property,
        Long timestamp,
        Visibility visibility,
        Object data,
        @SuppressWarnings("UnusedParameters") Authorizations authorizations
    ) {
        checkNotNull(element);
        Span trace = Trace.start("markPropertyHidden");
        trace.data("elementId", element.getId());
        trace.data("propertyName", property.getName());
        trace.data("propertyKey", property.getKey());
        try {
            if (timestamp == null) {
                timestamp = IncreasingTime.currentTimeMillis();
            }

            ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

            if (element instanceof Vertex) {
                addMutations(VertexiumObjectType.VERTEX, elementMutationBuilder.getMarkHiddenPropertyMutation(element.getId(), property, timestamp, columnVisibility, data));
            } else if (element instanceof Edge) {
                addMutations(VertexiumObjectType.EDGE, elementMutationBuilder.getMarkHiddenPropertyMutation(element.getId(), property, timestamp, columnVisibility, data));
            }

            getSearchIndex().markPropertyHidden(this, element, property, visibility, authorizations);

            if (hasEventListeners()) {
                fireGraphEvent(new MarkHiddenPropertyEvent(this, element, property, visibility, data));
            }
        } finally {
            trace.stop();
        }
    }

    @SuppressWarnings("unused")
    public void markPropertyVisible(
        AccumuloElement element,
        Property property,
        Long timestamp,
        Visibility visibility,
        Object data,
        Authorizations authorizations
    ) {
        checkNotNull(element);
        Span trace = Trace.start("markPropertyVisible");
        trace.data("elementId", element.getId());
        trace.data("propertyName", property.getName());
        trace.data("propertyKey", property.getKey());
        try {
            if (timestamp == null) {
                timestamp = IncreasingTime.currentTimeMillis();
            }

            ColumnVisibility columnVisibility = visibilityToAccumuloVisibility(visibility);

            Mutation markVisiblePropertyMutation = elementMutationBuilder.getMarkVisiblePropertyMutation(element.getId(), property, timestamp, columnVisibility, data);
            if (element instanceof Vertex) {
                addMutations(VertexiumObjectType.VERTEX, markVisiblePropertyMutation);
            } else if (element instanceof Edge) {
                addMutations(VertexiumObjectType.EDGE, markVisiblePropertyMutation);
            }

            getSearchIndex().markPropertyVisible(this, element, property, visibility, authorizations);

            if (hasEventListeners()) {
                fireGraphEvent(new MarkVisiblePropertyEvent(this, element, property, visibility, data));
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public void flushGraph() {
        flushWriter(this.batchWriter);
    }

    @Override
    public void flush() {
        if (hasEventListeners()) {
            synchronized (this.graphEventQueue) {
                flushWritersAndSuper();
                flushGraphEventQueue();
            }
        } else {
            flushWritersAndSuper();
        }
    }

    private void flushWritersAndSuper() {
        flushWriter(this.batchWriter);
        super.flush();
    }

    private void flushGraphEventQueue() {
        GraphEvent graphEvent;
        while ((graphEvent = this.graphEventQueue.poll()) != null) {
            fireGraphEvent(graphEvent);
        }
    }

    private static void flushWriter(MultiTableBatchWriter writer) {
        if (writer == null) {
            return;
        }

        try {
            if (!writer.isClosed()) {
                writer.flush();
            }
        } catch (MutationsRejectedException e) {
            throw new VertexiumException("Unable to flush writer", e);
        }
    }

    @Override
    public void shutdown() {
        try {
            flush();
            super.shutdown();
            streamingPropertyValueStorageStrategy.close();
            this.graphMetadataStore.close();
            this.curatorFramework.close();
            this.batchWriter.close();
        } catch (Exception ex) {
            throw new VertexiumException(ex);
        }
    }

    public VertexiumSerializer getVertexiumSerializer() {
        return vertexiumSerializer;
    }

    @Override
    public AccumuloGraphConfiguration getConfiguration() {
        return (AccumuloGraphConfiguration) super.getConfiguration();
    }

    @Override
    public Vertex getVertex(String vertexId, FetchHints fetchHints, Long endTime, User user) throws VertexiumException {
        try {
            if (vertexId == null) {
                return null;
            }

            Span trace = Trace.start("getVertex");
            trace.data("vertexId", vertexId);
            traceDataFetchHints(trace, fetchHints);
            return singleOrDefault(getVerticesInRange(trace, new org.apache.accumulo.core.data.Range(vertexId), fetchHints, endTime, user), null);
        } catch (IllegalStateException ex) {
            throw new VertexiumException("Failed to find vertex with id: " + vertexId, ex);
        } catch (RuntimeException ex) {
            if (ex.getCause() instanceof AccumuloSecurityException) {
                throw new SecurityVertexiumException("Could not get vertex " + vertexId + " with user: " + user, user, ex.getCause());
            }
            throw ex;
        }
    }

    @Override
    public Stream<Vertex> getVerticesWithPrefix(String vertexIdPrefix, FetchHints fetchHints, Long endTime, User user) {
        Span trace = Trace.start("getVerticesWithPrefix");
        trace.data("vertexIdPrefix", vertexIdPrefix);
        traceDataFetchHints(trace, fetchHints);
        org.apache.accumulo.core.data.Range range = org.apache.accumulo.core.data.Range.prefix(vertexIdPrefix);
        return getVerticesInRange(trace, range, fetchHints, endTime, user);
    }

    @Override
    public Stream<Vertex> getVerticesInRange(Range idRange, FetchHints fetchHints, Long endTime, User user) {
        Span trace = Trace.start("getVerticesInRange");
        trace.data("rangeInclusiveStart", idRange.getInclusiveStart());
        trace.data("rangeExclusiveStart", idRange.getExclusiveEnd());
        traceDataFetchHints(trace, fetchHints);
        org.apache.accumulo.core.data.Range range = vertexiumRangeToAccumuloRange(idRange);
        return getVerticesInRange(trace, range, fetchHints, endTime, user);
    }

    private Stream<Vertex> getVerticesInRange(
        Span trace,
        String startId,
        String endId,
        FetchHints fetchHints,
        Long timestamp,
        User user
    ) throws VertexiumException {
        trace.data("startId", startId);
        trace.data("endId", endId);
        if (Trace.isTracing() && timestamp != null) {
            trace.data("timestamp", Long.toString(timestamp));
        }
        traceDataFetchHints(trace, fetchHints);

        final Key startKey;
        if (startId == null) {
            startKey = null;
        } else {
            startKey = new Key(startId);
        }

        final Key endKey;
        if (endId == null) {
            endKey = null;
        } else {
            endKey = new Key(endId).followingKey(PartialKey.ROW);
        }

        org.apache.accumulo.core.data.Range range = new org.apache.accumulo.core.data.Range(startKey, endKey);
        return getVerticesInRange(trace, range, fetchHints, timestamp, user);
    }

    protected ScannerBase createVertexScanner(
        FetchHints fetchHints,
        Integer maxVersions,
        Long startTime,
        Long endTime,
        org.apache.accumulo.core.data.Range range,
        User user
    ) throws VertexiumException {
        return createElementScanner(fetchHints, ElementType.VERTEX, maxVersions, startTime, endTime, Lists.newArrayList(range), user);
    }

    protected ScannerBase createEdgeScanner(
        FetchHints fetchHints,
        Integer maxVersions,
        Long startTime,
        Long endTime,
        org.apache.accumulo.core.data.Range range,
        User user
    ) throws VertexiumException {
        return createElementScanner(fetchHints, ElementType.EDGE, maxVersions, startTime, endTime, Lists.newArrayList(range), user);
    }

    private ScannerBase createElementScanner(
        FetchHints fetchHints,
        ElementType elementType,
        Integer maxVersions,
        Long startTime,
        Long endTime,
        Collection<org.apache.accumulo.core.data.Range> ranges,
        User user
    ) throws VertexiumException {
        return createElementScanner(fetchHints, elementType, maxVersions, startTime, endTime, ranges, true, user);
    }

    ScannerBase createElementScanner(
        FetchHints fetchHints,
        ElementType elementType,
        Integer maxVersions,
        Long startTime,
        Long endTime,
        Collection<org.apache.accumulo.core.data.Range> ranges,
        boolean useVertexiumElementIterators,
        User user
    ) throws VertexiumException {
        try {
            String tableName;
            if (isHistoryInSeparateTable() && (startTime != null || endTime != null || maxVersions == null || maxVersions > 1)) {
                tableName = getHistoryTableNameFromElementType(elementType);
            } else {
                tableName = getTableNameFromElementType(elementType);
            }
            ScannerBase scanner;
            if (ranges == null || ranges.size() == 1) {
                org.apache.accumulo.core.data.Range range = ranges == null ? null : ranges.iterator().next();
                scanner = createScanner(tableName, range, user);
            } else {
                scanner = createBatchScanner(tableName, ranges, user);
            }

            if (startTime != null || endTime != null) {
                IteratorSetting iteratorSetting = new IteratorSetting(
                    80,
                    TimestampFilter.class.getSimpleName(),
                    TimestampFilter.class
                );
                if (startTime != null) {
                    TimestampFilter.setStart(iteratorSetting, startTime, true);
                }
                if (endTime != null) {
                    TimestampFilter.setEnd(iteratorSetting, endTime, true);
                }
                scanner.addScanIterator(iteratorSetting);
            }

            if (maxVersions != null) {
                IteratorSetting versioningIteratorSettings = new IteratorSetting(
                    90,
                    VersioningIterator.class.getSimpleName(),
                    VersioningIterator.class
                );
                VersioningIterator.setMaxVersions(versioningIteratorSettings, maxVersions);
                scanner.addScanIterator(versioningIteratorSettings);
            }

            if (useVertexiumElementIterators) {
                if (elementType == ElementType.VERTEX) {
                    IteratorSetting vertexIteratorSettings = new IteratorSetting(
                        1000,
                        VertexIterator.class.getSimpleName(),
                        VertexIterator.class
                    );
                    VertexIterator.setFetchHints(vertexIteratorSettings, toIteratorFetchHints(fetchHints));
                    VertexIterator.setAuthorizations(vertexIteratorSettings, user.getAuthorizations());
                    scanner.addScanIterator(vertexIteratorSettings);
                } else if (elementType == ElementType.EDGE) {
                    IteratorSetting edgeIteratorSettings = new IteratorSetting(
                        1000,
                        EdgeIterator.class.getSimpleName(),
                        EdgeIterator.class
                    );
                    EdgeIterator.setFetchHints(edgeIteratorSettings, toIteratorFetchHints(fetchHints));
                    EdgeIterator.setAuthorizations(edgeIteratorSettings, user.getAuthorizations());
                    scanner.addScanIterator(edgeIteratorSettings);
                } else {
                    throw new VertexiumException("Unexpected element type: " + elementType);
                }
            }

            applyFetchHints(scanner, fetchHints, elementType);
            GRAPH_LOGGER.logStartIterator(tableName, scanner);
            return scanner;
        } catch (TableNotFoundException e) {
            throw new VertexiumException(e);
        }
    }

    public IteratorFetchHints toIteratorFetchHints(FetchHints fetchHints) {
        return new IteratorFetchHints(
            fetchHints.isIncludeAllProperties(),
            deflateByteSequences(fetchHints.getPropertyNamesToInclude()),
            fetchHints.isIncludeAllPropertyMetadata(),
            deflateByteSequences(fetchHints.getMetadataKeysToInclude()),
            fetchHints.isIncludeHidden(),
            fetchHints.isIncludeAllEdgeRefs(),
            fetchHints.isIncludeOutEdgeRefs(),
            fetchHints.isIncludeInEdgeRefs(),
            fetchHints.isIgnoreAdditionalVisibilities(),
            deflate(fetchHints.getEdgeLabelsOfEdgeRefsToInclude()),
            fetchHints.isIncludeEdgeLabelsAndCounts(),
            fetchHints.isIncludeExtendedDataTableNames()
        );
    }

    private ImmutableSet<ByteSequence> deflateByteSequences(ImmutableSet<String> strings) {
        if (strings == null) {
            return null;
        }
        return ImmutableSet.copyOf(
            strings.stream()
                .map(s -> new ArrayByteSequence(getNameSubstitutionStrategy().deflate(s)))
                .collect(Collectors.toSet())
        );
    }

    private ImmutableSet<String> deflate(ImmutableSet<String> strings) {
        if (strings == null) {
            return null;
        }
        return ImmutableSet.copyOf(
            strings.stream()
                .map(s -> getNameSubstitutionStrategy().deflate(s))
                .collect(Collectors.toSet())
        );
    }

    protected ScannerBase createVertexScanner(
        FetchHints fetchHints,
        Integer maxVersions,
        Long startTime,
        Long endTime,
        Collection<org.apache.accumulo.core.data.Range> ranges,
        User user
    ) throws VertexiumException {
        return createElementScanner(
            fetchHints,
            ElementType.VERTEX,
            maxVersions,
            startTime,
            endTime,
            ranges,
            user
        );
    }

    protected ScannerBase createEdgeScanner(
        FetchHints fetchHints,
        Integer maxVersions,
        Long startTime,
        Long endTime,
        Collection<org.apache.accumulo.core.data.Range> ranges,
        User user
    ) throws VertexiumException {
        return createElementScanner(
            fetchHints,
            ElementType.EDGE,
            maxVersions,
            startTime,
            endTime,
            ranges,
            user
        );
    }

    public ScannerBase createBatchScanner(
        String tableName,
        Collection<org.apache.accumulo.core.data.Range> ranges,
        User user
    ) throws TableNotFoundException {
        org.apache.accumulo.core.security.Authorizations accumuloAuthorizations = toAccumuloAuthorizations(user);
        return createBatchScanner(tableName, ranges, accumuloAuthorizations);
    }

    public ScannerBase createBatchScanner(
        String tableName,
        Collection<org.apache.accumulo.core.data.Range> ranges,
        org.apache.accumulo.core.security.Authorizations accumuloAuthorizations
    ) throws TableNotFoundException {
        VisalloTabletServerBatchReader scanner = new VisalloTabletServerBatchReader(
            connector,
            tableName,
            accumuloAuthorizations,
            numberOfQueryThreads
        );
        scanner.setRanges(ranges);
        return scanner;
    }

    private Scanner createScanner(
        String tableName,
        org.apache.accumulo.core.data.Range range,
        User user
    ) throws TableNotFoundException {
        org.apache.accumulo.core.security.Authorizations accumuloAuthorizations = toAccumuloAuthorizations(user);
        return createScanner(tableName, range, accumuloAuthorizations);
    }

    private Scanner createScanner(
        String tableName,
        org.apache.accumulo.core.data.Range range,
        org.apache.accumulo.core.security.Authorizations accumuloAuthorizations
    ) throws TableNotFoundException {
        Scanner scanner = connector.createScanner(tableName, accumuloAuthorizations);
        if (range != null) {
            scanner.setRange(range);
        }
        return scanner;
    }

    private void applyFetchHints(ScannerBase scanner, FetchHints fetchHints, ElementType elementType) {
        scanner.clearColumns();

        Iterable<Text> columnFamiliesToFetch = getColumnFamiliesToFetch(elementType, fetchHints);
        for (Text columnFamilyToFetch : columnFamiliesToFetch) {
            scanner.fetchColumnFamily(columnFamilyToFetch);
        }
    }

    public static Iterable<Text> getColumnFamiliesToFetch(ElementType elementType, FetchHints fetchHints) {
        List<Text> columnFamiliesToFetch = new ArrayList<>();

        columnFamiliesToFetch.add(AccumuloElement.CF_HIDDEN);
        columnFamiliesToFetch.add(AccumuloElement.CF_SOFT_DELETE);
        columnFamiliesToFetch.add(AccumuloElement.DELETE_ROW_COLUMN_FAMILY);
        columnFamiliesToFetch.add(AccumuloElement.CF_ADDITIONAL_VISIBILITY);

        if (elementType == ElementType.VERTEX) {
            columnFamiliesToFetch.add(AccumuloVertex.CF_SIGNAL);
        } else if (elementType == ElementType.EDGE) {
            columnFamiliesToFetch.add(AccumuloEdge.CF_SIGNAL);
            columnFamiliesToFetch.add(AccumuloEdge.CF_IN_VERTEX);
            columnFamiliesToFetch.add(AccumuloEdge.CF_OUT_VERTEX);
        } else {
            throw new VertexiumException("Unhandled element type: " + elementType);
        }

        if (fetchHints.isIncludeAllEdgeRefs()
            || fetchHints.isIncludeInEdgeRefs()
            || fetchHints.isIncludeEdgeLabelsAndCounts()
            || fetchHints.hasEdgeLabelsOfEdgeRefsToInclude()) {
            columnFamiliesToFetch.add(AccumuloVertex.CF_IN_EDGE);
            columnFamiliesToFetch.add(AccumuloVertex.CF_IN_EDGE_HIDDEN);
            columnFamiliesToFetch.add(AccumuloVertex.CF_IN_EDGE_SOFT_DELETE);
        }
        if (fetchHints.isIncludeAllEdgeRefs()
            || fetchHints.isIncludeOutEdgeRefs()
            || fetchHints.isIncludeEdgeLabelsAndCounts()
            || fetchHints.hasEdgeLabelsOfEdgeRefsToInclude()) {
            columnFamiliesToFetch.add(AccumuloVertex.CF_OUT_EDGE);
            columnFamiliesToFetch.add(AccumuloVertex.CF_OUT_EDGE_HIDDEN);
            columnFamiliesToFetch.add(AccumuloVertex.CF_OUT_EDGE_SOFT_DELETE);
        }
        if (fetchHints.isIncludeProperties()) {
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY);
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_HIDDEN);
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_SOFT_DELETE);
        }
        if (fetchHints.isIncludePropertyMetadata()) {
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_METADATA);
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_HIDDEN);
            columnFamiliesToFetch.add(AccumuloElement.CF_PROPERTY_SOFT_DELETE);
        }
        if (fetchHints.isIncludeExtendedDataTableNames()) {
            columnFamiliesToFetch.add(AccumuloElement.CF_EXTENDED_DATA);
        }

        return columnFamiliesToFetch;
    }

    public String getTableNameFromElementType(ElementType elementType) {
        switch (elementType) {
            case VERTEX:
                return getVerticesTableName();
            case EDGE:
                return getEdgesTableName();
            default:
                throw new VertexiumException("Unexpected element type: " + elementType);
        }
    }

    public String getHistoryTableNameFromElementType(ElementType elementType) {
        switch (elementType) {
            case VERTEX:
                return getHistoryVerticesTableName();
            case EDGE:
                return getHistoryEdgesTableName();
            default:
                throw new VertexiumException("Unexpected element type: " + elementType);
        }
    }

    public org.apache.accumulo.core.security.Authorizations toAccumuloAuthorizations(User user) {
        if (user == null) {
            throw new NullPointerException("user is required");
        }
        return new org.apache.accumulo.core.security.Authorizations(user.getAuthorizations());
    }

    @Override
    public Edge getEdge(String edgeId, FetchHints fetchHints, Long endTime, User user) {
        Span trace = Trace.start("getEdge");
        trace.data("edgeId", edgeId);
        try {
            return singleOrDefault(getEdgesInRange(trace, edgeId, edgeId, fetchHints, endTime, user), null);
        } catch (IllegalStateException ex) {
            throw new VertexiumException("Failed to find edge with id: " + edgeId, ex);
        } catch (RuntimeException ex) {
            if (ex.getCause() instanceof AccumuloSecurityException) {
                throw new SecurityVertexiumException("Could not get edge " + edgeId + " with user: " + user, user, ex.getCause());
            }
            throw ex;
        }
    }

    public static ColumnVisibility visibilityToAccumuloVisibility(Visibility visibility) {
        return new ColumnVisibility(visibility.getVisibilityString());
    }

    public static ColumnVisibility visibilityToAccumuloVisibility(String visibilityString) {
        return new ColumnVisibility(visibilityString);
    }

    public static ColumnVisibility visibilityToAccumuloVisibility(ByteSequence visibilityBytes) {
        return new ColumnVisibility(ByteSequenceUtils.getBytes(visibilityBytes));
    }

    public static Visibility accumuloVisibilityToVisibility(ColumnVisibility columnVisibility) {
        if (columnVisibility.equals(EMPTY_COLUMN_VISIBILITY)) {
            return Visibility.EMPTY;
        }
        String columnVisibilityString = columnVisibility.toString();
        return accumuloVisibilityToVisibility(columnVisibilityString);
    }

    public static Visibility accumuloVisibilityToVisibility(Text columnVisibility) {
        return accumuloVisibilityToVisibility(columnVisibility.toString());
    }

    public static Visibility accumuloVisibilityToVisibility(String columnVisibilityString) {
        if (columnVisibilityString.startsWith("[") && columnVisibilityString.endsWith("]")) {
            if (columnVisibilityString.length() == 2) {
                return Visibility.EMPTY;
            }
            columnVisibilityString = columnVisibilityString.substring(1, columnVisibilityString.length() - 1);
        }
        if (columnVisibilityString.length() == 0) {
            return Visibility.EMPTY;
        }
        return new Visibility(columnVisibilityString);
    }

    public static String getVerticesTableName(String tableNamePrefix) {
        return tableNamePrefix + "_v";
    }

    public static String getHistoryVerticesTableName(String tableNamePrefix) {
        return tableNamePrefix + "_vh";
    }

    public static String getEdgesTableName(String tableNamePrefix) {
        return tableNamePrefix.concat("_e");
    }

    public static String getHistoryEdgesTableName(String tableNamePrefix) {
        return tableNamePrefix.concat("_eh");
    }

    public static String getExtendedDataTableName(String tableNamePrefix) {
        return tableNamePrefix + "_extdata";
    }

    public static String getDataTableName(String tableNamePrefix) {
        return tableNamePrefix.concat("_d");
    }

    public static String getMetadataTableName(String tableNamePrefix) {
        return tableNamePrefix.concat("_m");
    }

    public String getVerticesTableName() {
        return verticesTableName;
    }

    public String getHistoryVerticesTableName() {
        return historyVerticesTableName;
    }

    public String getEdgesTableName() {
        return edgesTableName;
    }

    public String getHistoryEdgesTableName() {
        return historyEdgesTableName;
    }

    public String getExtendedDataTableName() {
        return extendedDataTableName;
    }

    public String getDataTableName() {
        return dataTableName;
    }

    public String getMetadataTableName() {
        return metadataTableName;
    }

    public StreamingPropertyValueStorageStrategy getStreamingPropertyValueStorageStrategy() {
        return streamingPropertyValueStorageStrategy;
    }

    public AccumuloGraphLogger getGraphLogger() {
        return GRAPH_LOGGER;
    }

    public Connector getConnector() {
        return connector;
    }

    public Iterable<Range> listVerticesTableSplits() {
        return listTableSplits(getVerticesTableName());
    }

    public Iterable<Range> listHistoryVerticesTableSplits() {
        return listTableSplits(getHistoryVerticesTableName());
    }

    public Iterable<Range> listEdgesTableSplits() {
        return listTableSplits(getEdgesTableName());
    }

    public Iterable<Range> listHistoryEdgesTableSplits() {
        return listTableSplits(getHistoryEdgesTableName());
    }

    public Iterable<Range> listDataTableSplits() {
        return listTableSplits(getDataTableName());
    }

    public Iterable<Range> listExtendedDataTableSplits() {
        return listTableSplits(getExtendedDataTableName());
    }

    private Iterable<Range> listTableSplits(String tableName) {
        try {
            return splitsIterableToRangeIterable(getConnector().tableOperations().listSplits(tableName));
        } catch (Exception ex) {
            throw new VertexiumException("Could not get splits for: " + tableName, ex);
        }
    }

    private Iterable<Range> splitsIterableToRangeIterable(final Iterable<Text> splits) {
        String inclusiveStart = null;
        List<Range> ranges = new ArrayList<>();
        for (Text split : splits) {
            String exclusiveEnd = new Key(split).getRow().toString();
            ranges.add(new Range(inclusiveStart, exclusiveEnd));
            inclusiveStart = exclusiveEnd;
        }
        ranges.add(new Range(inclusiveStart, null));
        return ranges;
    }

    void alterElementVisibility(AccumuloElement element, Visibility newVisibility, Object data) {
        String elementRowKey = element.getId();
        Span trace = Trace.start("alterElementVisibility");
        trace.data("elementRowKey", elementRowKey);
        try {
            if (element instanceof Edge) {
                Edge edge = (Edge) element;

                String vertexOutRowKey = edge.getVertexId(Direction.OUT);
                Mutation vertexOutMutation = new Mutation(vertexOutRowKey);
                if (elementMutationBuilder.alterEdgeVertexOutVertex(vertexOutMutation, edge, newVisibility)) {
                    addMutations(VertexiumObjectType.VERTEX, vertexOutMutation);
                }

                String vertexInRowKey = edge.getVertexId(Direction.IN);
                Mutation vertexInMutation = new Mutation(vertexInRowKey);
                if (elementMutationBuilder.alterEdgeVertexInVertex(vertexInMutation, edge, newVisibility)) {
                    addMutations(VertexiumObjectType.VERTEX, vertexInMutation);
                }
            }

            Mutation m = new Mutation(elementRowKey);
            if (elementMutationBuilder.alterElementVisibility(m, element, newVisibility, data)) {
                addMutations(element, m);
            }
            element.setVisibility(newVisibility);
        } finally {
            trace.stop();
        }
    }

    void alterElementPropertyVisibilities(AccumuloElement element, Iterable<AlterPropertyVisibility> alterPropertyVisibilities) {
        Iterator<AlterPropertyVisibility> it = alterPropertyVisibilities.iterator();
        if (!it.hasNext()) {
            return;
        }

        String elementRowKey = element.getId();

        Mutation m = new Mutation(elementRowKey);

        List<PropertyDescriptor> propertyList = Lists.newArrayList();
        while (it.hasNext()) {
            AlterPropertyVisibility apv = it.next();
            MutableProperty property = (MutableProperty) element.getProperty(
                apv.getKey(),
                apv.getName(),
                apv.getExistingVisibility()
            );
            if (property == null) {
                throw new VertexiumException("Could not find property " + apv.getKey() + ":" + apv.getName());
            }
            if (property.getVisibility().equals(apv.getVisibility())) {
                continue;
            }
            if (apv.getExistingVisibility() == null) {
                apv.setExistingVisibility(property.getVisibility());
            }
            elementMutationBuilder.addPropertySoftDeleteToMutation(m, property, apv.getTimestamp() - 1, apv.getData());
            property.setVisibility(apv.getVisibility());
            property.setTimestamp(apv.getTimestamp());
            elementMutationBuilder.addPropertyToMutation(this, m, elementRowKey, property);

            // Keep track of properties that need to be removed from indices
            propertyList.add(PropertyDescriptor.from(apv.getKey(), apv.getName(), apv.getExistingVisibility()));
        }


        if (!propertyList.isEmpty()) {
            addMutations(element, m);
        }
    }

    void alterPropertyMetadatas(AccumuloElement element, Iterable<SetPropertyMetadata> setPropertyMetadatas) {
        Iterator<SetPropertyMetadata> it = setPropertyMetadatas.iterator();
        if (!it.hasNext()) {
            return;
        }

        String elementRowKey = element.getId();
        Mutation m = new Mutation(elementRowKey);
        while (it.hasNext()) {
            SetPropertyMetadata apm  = it.next();
            Property property = element.getProperty(apm.getPropertyKey(), apm.getPropertyName(), apm.getPropertyVisibility());
            if (property == null) {
                throw new VertexiumException(String.format("Could not find property %s:%s(%s)", apm.getPropertyKey(), apm.getPropertyName(), apm.getPropertyVisibility()));
            }
            if (property.getFetchHints().isIncludePropertyAndMetadata(property.getName())) {
                property.getMetadata().add(apm.getMetadataName(), apm.getNewValue(), apm.getMetadataVisibility());
            }
            elementMutationBuilder.addPropertyMetadataItemToMutation(
                m,
                property,
                apm.getMetadataName(),
                apm.getNewValue(),
                apm.getMetadataVisibility()
            );
        }

        addMutations(element, m);
    }

    @Override
    public boolean isVisibilityValid(Visibility visibility, User user) {
        return user.canRead(visibility);
    }

    private boolean isHistoryInSeparateTable() {
        return historyInSeparateTable;
    }

    @Override
    public void truncate() {
        try {
            this.connector.tableOperations().deleteRows(getDataTableName(), null, null);
            this.connector.tableOperations().deleteRows(getEdgesTableName(), null, null);
            this.connector.tableOperations().deleteRows(getVerticesTableName(), null, null);
            this.connector.tableOperations().deleteRows(getExtendedDataTableName(), null, null);
            this.connector.tableOperations().deleteRows(getMetadataTableName(), null, null);
            if (isHistoryInSeparateTable()) {
                this.connector.tableOperations().deleteRows(getHistoryEdgesTableName(), null, null);
                this.connector.tableOperations().deleteRows(getHistoryVerticesTableName(), null, null);
            }
            getSearchIndex().truncate(this);
        } catch (Exception ex) {
            throw new VertexiumException("Could not delete rows", ex);
        }
    }

    @Override
    public void drop() {
        try {
            dropTableIfExists(getDataTableName());
            dropTableIfExists(getEdgesTableName());
            dropTableIfExists(getVerticesTableName());
            dropTableIfExists(getMetadataTableName());
            if (isHistoryInSeparateTable()) {
                dropTableIfExists(getHistoryEdgesTableName());
                dropTableIfExists(getHistoryVerticesTableName());
            }
            getSearchIndex().drop(this);
        } catch (Exception ex) {
            throw new VertexiumException("Could not drop tables", ex);
        }
    }

    private void dropTableIfExists(String tableName) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        if (this.connector.tableOperations().exists(tableName)) {
            this.connector.tableOperations().delete(tableName);
        }
    }

    @Override
    public Stream<String> findRelatedEdgeIds(Iterable<String> vertexIds, Long endTime, User user) {
        Set<String> vertexIdsSet = IterableUtils.toSet(vertexIds);
        Span trace = Trace.start("findRelatedEdges");
        try {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("findRelatedEdges:\n  %s", IterableUtils.join(vertexIdsSet, "\n  "));
            }

            if (vertexIdsSet.size() == 0) {
                return Stream.empty();
            }

            List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
            for (String vertexId : vertexIdsSet) {
                ranges.add(RangeUtils.createRangeFromString(vertexId));
            }

            Long startTime = null;
            int maxVersions = 1;
            FetchHints fetchHints = FetchHints.builder()
                .setIncludeOutEdgeRefs(true)
                .build();
            ScannerBase scanner = createElementScanner(
                fetchHints,
                ElementType.VERTEX,
                maxVersions,
                startTime,
                endTime,
                ranges,
                false,
                user
            );

            IteratorSetting edgeRefFilterSettings = new IteratorSetting(
                1000,
                EdgeRefFilter.class.getSimpleName(),
                EdgeRefFilter.class
            );
            EdgeRefFilter.setVertexIds(edgeRefFilterSettings, vertexIdsSet);
            scanner.addScanIterator(edgeRefFilterSettings);

            IteratorSetting vertexEdgeIdIteratorSettings = new IteratorSetting(
                1001,
                VertexEdgeIdIterator.class.getSimpleName(),
                VertexEdgeIdIterator.class
            );
            scanner.addScanIterator(vertexEdgeIdIteratorSettings);

            final long timerStartTime = System.currentTimeMillis();
            try {
                Iterator<Map.Entry<Key, Value>> it = scanner.iterator();
                List<String> edgeIds = new ArrayList<>();
                while (it.hasNext()) {
                    Map.Entry<Key, Value> c = it.next();
                    for (ByteArrayWrapper edgeId : VertexEdgeIdIterator.decodeValue(c.getValue())) {
                        edgeIds.add(new Text(edgeId.getData()).toString());
                    }
                }
                return edgeIds.stream();
            } finally {
                scanner.close();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public Stream<RelatedEdge> findRelatedEdgeSummary(Iterable<String> vertexIds, Long endTime, User user) {
        Set<String> vertexIdsSet = IterableUtils.toSet(vertexIds);
        Span trace = Trace.start("findRelatedEdgeSummary");
        try {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("findRelatedEdgeSummary:\n  %s", IterableUtils.join(vertexIdsSet, "\n  "));
            }

            if (vertexIdsSet.size() == 0) {
                return Stream.empty();
            }

            List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
            for (String vertexId : vertexIdsSet) {
                ranges.add(RangeUtils.createRangeFromString(vertexId));
            }

            Long startTime = null;
            int maxVersions = 1;
            FetchHints fetchHints = FetchHints.builder()
                .setIncludeOutEdgeRefs(true)
                .build();
            ScannerBase scanner = createElementScanner(
                fetchHints,
                ElementType.VERTEX,
                maxVersions,
                startTime,
                endTime,
                ranges,
                false,
                user
            );

            IteratorSetting edgeRefFilterSettings = new IteratorSetting(
                1000,
                EdgeRefFilter.class.getSimpleName(),
                EdgeRefFilter.class
            );
            EdgeRefFilter.setVertexIds(edgeRefFilterSettings, vertexIdsSet);
            scanner.addScanIterator(edgeRefFilterSettings);

            final long timerStartTime = System.currentTimeMillis();
            try {
                List<RelatedEdge> results = new ArrayList<>();
                Map<String, Long> edgeAddTimestamps = new HashMap<>();
                Map<String, Long> edgeHideOrDeleteTimestamps = new HashMap<>();
                for (Map.Entry<Key, Value> row : scanner) {
                    Text columnFamily = row.getKey().getColumnFamily();
                    Long timestamp = row.getKey().getTimestamp();
                    if (!columnFamily.equals(AccumuloVertex.CF_OUT_EDGE)) {
                        if (columnFamily.equals(AccumuloVertex.CF_OUT_EDGE_SOFT_DELETE) || columnFamily.equals(AccumuloVertex.CF_OUT_EDGE_HIDDEN)) {
                            String edgeId = row.getKey().getColumnQualifier().toString();
                            edgeHideOrDeleteTimestamps.merge(edgeId, timestamp, Math::max);
                        }
                        continue;
                    }

                    org.vertexium.accumulo.iterator.model.EdgeInfo edgeInfo
                        = new EdgeInfo(row.getValue().get(), row.getKey().getTimestamp());
                    String edgeId = row.getKey().getColumnQualifier().toString();
                    String outVertexId = row.getKey().getRow().toString();
                    String inVertexId = edgeInfo.getVertexId();
                    String label = getNameSubstitutionStrategy().inflate(edgeInfo.getLabel());

                    edgeAddTimestamps.merge(edgeId, timestamp, Math::max);

                    results.add(new RelatedEdgeImpl(edgeId, label, outVertexId, inVertexId));
                }
                return results.stream().filter(relatedEdge -> {
                    Long edgeAddedTime = edgeAddTimestamps.get(relatedEdge.getEdgeId());
                    Long edgeDeletedOrHiddenTime = edgeHideOrDeleteTimestamps.get(relatedEdge.getEdgeId());
                    return edgeDeletedOrHiddenTime == null || edgeAddedTime > edgeDeletedOrHiddenTime;
                });
            } finally {
                scanner.close();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        } finally {
            trace.stop();
        }
    }

    @Override
    public Stream<Path> findPaths(FindPathOptions options, User user) {
        ProgressCallback progressCallback = options.getProgressCallback();
        if (progressCallback == null) {
            progressCallback = new ProgressCallback() {
                @Override
                public void progress(double progressPercent, Step step, Integer edgeIndex, Integer vertexCount) {
                    LOGGER.debug("findPaths progress %d%%: %s", (int) (progressPercent * 100.0), step.formatMessage(edgeIndex, vertexCount));
                }
            };
        }

        return new AccumuloFindPathStrategy(this, options, progressCallback, user).findPaths();
    }

    @Override
    public Iterable<String> filterEdgeIdsByAuthorization(Iterable<String> edgeIds, String authorizationToMatch, EnumSet<ElementFilter> filters, Authorizations authorizations) {
        return toIterable(filterElementIdsByAuthorization(
            ElementType.EDGE,
            edgeIds,
            authorizationToMatch,
            filters,
            authorizations.getUser()
        ));
    }

    @Override
    public Iterable<String> filterVertexIdsByAuthorization(Iterable<String> vertexIds, String authorizationToMatch, EnumSet<ElementFilter> filters, Authorizations authorizations) {
        return toIterable(filterElementIdsByAuthorization(
            ElementType.VERTEX,
            vertexIds,
            authorizationToMatch,
            filters,
            authorizations.getUser()
        ));
    }

    private Stream<String> filterElementIdsByAuthorization(ElementType elementType, Iterable<String> elementIds, String authorizationToMatch, EnumSet<ElementFilter> filters, User user) {
        Set<String> elementIdsSet = IterableUtils.toSet(elementIds);
        Span trace = Trace.start("filterElementIdsByAuthorization");
        try {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("filterElementIdsByAuthorization:\n  %s", IterableUtils.join(elementIdsSet, "\n  "));
            }

            if (elementIdsSet.size() == 0) {
                return Stream.empty();
            }

            List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
            for (String elementId : elementIdsSet) {
                ranges.add(RangeUtils.createRangeFromString(elementId));
            }

            Long startTime = null;
            Long endTime = null;
            int maxVersions = 1;
            ScannerBase scanner = createElementScanner(
                FetchHints.ALL_INCLUDING_HIDDEN,
                elementType,
                maxVersions,
                startTime,
                endTime,
                ranges,
                false,
                user
            );

            IteratorSetting hasAuthorizationFilterSettings = new IteratorSetting(
                1000,
                HasAuthorizationFilter.class.getSimpleName(),
                HasAuthorizationFilter.class
            );
            HasAuthorizationFilter.setAuthorizationToMatch(hasAuthorizationFilterSettings, authorizationToMatch);
            HasAuthorizationFilter.setFilters(hasAuthorizationFilterSettings, filters);
            scanner.addScanIterator(hasAuthorizationFilterSettings);

            final long timerStartTime = System.currentTimeMillis();
            try {
                Set<String> results = new HashSet<>();
                for (Map.Entry<Key, Value> row : scanner) {
                    results.add(row.getKey().getRow().toString());
                }
                return results.stream();
            } finally {
                scanner.close();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        } finally {
            trace.stop();
        }
    }

    public Iterable<GraphMetadataEntry> getMetadataInRange(org.apache.accumulo.core.data.Range range) {
        final long timerStartTime = System.currentTimeMillis();

        return new LookAheadIterable<Map.Entry<Key, Value>, GraphMetadataEntry>() {
            public Scanner scanner;

            @Override
            protected boolean isIncluded(Map.Entry<Key, Value> src, GraphMetadataEntry graphMetadataEntry) {
                return true;
            }

            @Override
            protected GraphMetadataEntry convert(Map.Entry<Key, Value> entry) {
                String key = entry.getKey().getRow().toString();
                return new GraphMetadataEntry(key, entry.getValue().get());
            }

            @Override
            protected Iterator<Map.Entry<Key, Value>> createIterator() {
                try {
                    scanner = createScanner(getMetadataTableName(), range, METADATA_AUTHORIZATIONS.getUser());
                    GRAPH_LOGGER.logStartIterator(getMetadataTableName(), scanner);

                    IteratorSetting versioningIteratorSettings = new IteratorSetting(
                        90,
                        VersioningIterator.class.getSimpleName(),
                        VersioningIterator.class
                    );
                    VersioningIterator.setMaxVersions(versioningIteratorSettings, 1);
                    scanner.addScanIterator(versioningIteratorSettings);

                    return scanner.iterator();
                } catch (TableNotFoundException ex) {
                    throw new VertexiumException("Could not create metadata scanner", ex);
                }
            }

            @Override
            public void close() {
                super.close();
                this.scanner.close();
                GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
            }
        };
    }

    @Override
    protected GraphMetadataStore getGraphMetadataStore() {
        return graphMetadataStore;
    }

    private Stream<ExtendedDataRow> getExtendedDataRowsInRange(
        Span trace,
        List<org.apache.accumulo.core.data.Range> ranges,
        FetchHints fetchHints,
        User user
    ) {
        final long timerStartTime = System.currentTimeMillis();

        ScannerBase scanner = createExtendedDataRowScanner(ranges, user);
        return new DelegatingStream<>(stream(scanner.iterator()))
                .onClose(() -> {
                    scanner.close();
                    if (trace != null) {
                        trace.stop();
                    }
                    GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
                }).map(rawRow -> {
                    try {
                        SortedMap<Key, Value> row = WholeRowIterator.decodeRow(rawRow.getKey(), rawRow.getValue());
                        ExtendedDataRowId extendedDataRowId = KeyHelper.parseExtendedDataRowId(rawRow.getKey().getRow());
                        return (ExtendedDataRow) AccumuloExtendedDataRow.create(
                                extendedDataRowId,
                                row,
                                fetchHints,
                                vertexiumSerializer
                        );
                    } catch (IOException e) {
                        throw new VertexiumException("Could not decode row", e);
                    }
                }).filter(row -> {
                    if (!fetchHints.isIgnoreAdditionalVisibilities()) {
                        Set<String> additionalVisibilities = row.getAdditionalVisibilities();
                        if (additionalVisibilities.size() == 0) {
                            return true;
                        }
                        for (String additionalVisibility : additionalVisibilities) {
                            if (!user.canRead(new Visibility(additionalVisibility))) {
                                return false;
                            }
                        }
                        return true;
                    }
                    return true;
                });
    }

    private ScannerBase createExtendedDataRowScanner(List<org.apache.accumulo.core.data.Range> ranges, User user) {
        try {
            String tableName = getExtendedDataTableName();
            ScannerBase scanner;
            if (ranges == null || ranges.size() == 1) {
                org.apache.accumulo.core.data.Range range = ranges == null ? null : ranges.iterator().next();
                scanner = createScanner(tableName, range, user);
            } else {
                scanner = createBatchScanner(tableName, ranges, user);
            }

            IteratorSetting versioningIteratorSettings = new IteratorSetting(
                90, // versioning needs to happen before combining into one row
                VersioningIterator.class.getSimpleName(),
                VersioningIterator.class
            );
            VersioningIterator.setMaxVersions(versioningIteratorSettings, 1);
            scanner.addScanIterator(versioningIteratorSettings);

            IteratorSetting rowIteratorSettings = new IteratorSetting(
                100,
                WholeRowIterator.class.getSimpleName(),
                WholeRowIterator.class
            );
            scanner.addScanIterator(rowIteratorSettings);

            GRAPH_LOGGER.logStartIterator(tableName, scanner);
            return scanner;
        } catch (TableNotFoundException e) {
            throw new VertexiumException(e);
        }
    }

    protected Stream<Vertex> getVerticesInRange(
        Span trace,
        org.apache.accumulo.core.data.Range range,
        FetchHints fetchHints,
        Long endTime,
        User user
    ) {
        final long timerStartTime = System.currentTimeMillis();

        ScannerBase scanner = createVertexScanner(fetchHints, SINGLE_VERSION, null, endTime, range, user);
        return new DelegatingStream<>(stream(scanner.iterator()))
                .onClose(() -> {
                    scanner.close();
                    if (trace != null) {
                        trace.stop();
                    }
                    GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
                }).map(row -> createVertexFromVertexIteratorValue(row.getKey(), row.getValue(), fetchHints, user));
    }

    private Vertex createVertexFromVertexIteratorValue(Key key, Value value, FetchHints fetchHints, User user) {
        return AccumuloVertex.createFromIteratorValue(this, key, value, fetchHints, user);
    }

    private Edge createEdgeFromEdgeIteratorValue(Key key, Value value, FetchHints fetchHints, User user) {
        return AccumuloEdge.createFromIteratorValue(this, key, value, fetchHints, user);
    }

    @Override
    public Stream<Vertex> getVertices(Iterable<String> ids, FetchHints fetchHints, Long endTime, User user) {
        final List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
        int idCount = 0;
        for (String id : ids) {
            ranges.add(RangeUtils.createRangeFromString(id));
            idCount++;
        }
        if (ranges.size() == 0) {
            return Stream.empty();
        }

        final Span trace = Trace.start("getVertices");
        trace.data("idCount", Integer.toString(idCount));
        traceDataFetchHints(trace, fetchHints);
        final long timerStartTime = System.currentTimeMillis();

        ScannerBase scanner = createVertexScanner(fetchHints, 1, null, endTime, ranges, user);
        return new DelegatingStream<>(StreamUtils.stream(scanner.iterator()))
                .onClose(() -> {
                    scanner.close();
                    trace.stop();
                    GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
                })
                .map(row -> createVertexFromVertexIteratorValue(row.getKey(), row.getValue(), fetchHints, user));
    }

    @Override
    public Stream<Edge> getEdges(Iterable<String> ids, FetchHints fetchHints, Long endTime, User user) {
        final List<org.apache.accumulo.core.data.Range> ranges = new ArrayList<>();
        int idCount = 0;
        for (String id : ids) {
            ranges.add(RangeUtils.createRangeFromString(id));
            idCount++;
        }
        if (ranges.size() == 0) {
            return Stream.empty();
        }

        final Span trace = Trace.start("getEdges");
        trace.data("idCount", Integer.toString(idCount));
        traceDataFetchHints(trace, fetchHints);
        final long timerStartTime = System.currentTimeMillis();

        ScannerBase scanner = createEdgeScanner(fetchHints, 1, null, endTime, ranges, user);
        return new DelegatingStream<>(stream(scanner.iterator()))
                .onClose(() -> {
                    scanner.close();
                    trace.stop();
                    GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
                }).map(row -> createEdgeFromEdgeIteratorValue(row.getKey(), row.getValue(), fetchHints, user));
    }

    @Override
    public Stream<Edge> getEdgesInRange(Range idRange, FetchHints fetchHints, Long endTime, User user) {
        Span trace = Trace.start("getEdgesInRange");
        trace.data("rangeInclusiveStart", idRange.getInclusiveStart());
        trace.data("rangeExclusiveStart", idRange.getExclusiveEnd());
        traceDataFetchHints(trace, fetchHints);
        org.apache.accumulo.core.data.Range range = vertexiumRangeToAccumuloRange(idRange);
        return getEdgesInRange(trace, range, fetchHints, endTime, user);
    }

    private org.apache.accumulo.core.data.Range vertexiumRangeToAccumuloRange(Range range) {
        Key inclusiveStartRow = range.getInclusiveStart() == null ? null : new Key(range.getInclusiveStart());
        Key exclusiveEndRow = range.getExclusiveEnd() == null ? null : new Key(range.getExclusiveEnd());
        boolean startKeyInclusive = true;
        boolean endKeyInclusive = false;
        return new org.apache.accumulo.core.data.Range(
            inclusiveStartRow, startKeyInclusive,
            exclusiveEndRow, endKeyInclusive
        );
    }

    protected Stream<Edge> getEdgesInRange(
        Span trace,
        String startId,
        String endId,
        FetchHints fetchHints,
        Long timestamp,
        User user
    ) throws VertexiumException {
        trace.data("startId", startId);
        trace.data("endId", endId);
        if (Trace.isTracing() && timestamp != null) {
            trace.data("timestamp", Long.toString(timestamp));
        }
        traceDataFetchHints(trace, fetchHints);

        final Key startKey;
        if (startId == null) {
            startKey = null;
        } else {
            startKey = new Key(startId);
        }

        final Key endKey;
        if (endId == null) {
            endKey = null;
        } else {
            endKey = new Key(endId).followingKey(PartialKey.ROW);
        }

        org.apache.accumulo.core.data.Range range = new org.apache.accumulo.core.data.Range(startKey, endKey);
        return getEdgesInRange(trace, range, fetchHints, timestamp, user);
    }

    protected Stream<Edge> getEdgesInRange(
        Span trace,
        org.apache.accumulo.core.data.Range range,
        FetchHints fetchHints,
        Long endTime,
        User user
    ) throws VertexiumException {
        traceDataFetchHints(trace, fetchHints);

        final long timerStartTime = System.currentTimeMillis();

        ScannerBase scanner = createEdgeScanner(fetchHints, SINGLE_VERSION, null, endTime, range, user);
        return new DelegatingStream<>(stream(scanner.iterator()))
                .onClose(() -> {
                    scanner.close();
                    if (trace != null) {
                        trace.stop();
                    }
                    GRAPH_LOGGER.logEndIterator(System.currentTimeMillis() - timerStartTime);
                }).map(row -> createEdgeFromEdgeIteratorValue(row.getKey(), row.getValue(), fetchHints, user));
    }

    private long getRowCountFromTable(String tableName, Text signalColumn, User user) {
        try {
            LOGGER.debug("BEGIN getRowCountFromTable(%s)", tableName);
            Scanner scanner = createScanner(tableName, null, user);
            try {
                scanner.fetchColumnFamily(signalColumn);

                IteratorSetting countingIterator = new IteratorSetting(
                    100,
                    CountingIterator.class.getSimpleName(),
                    CountingIterator.class
                );
                scanner.addScanIterator(countingIterator);

                GRAPH_LOGGER.logStartIterator(tableName, scanner);

                long count = 0;
                for (Map.Entry<Key, Value> entry : scanner) {
                    Long countForKey = LongCombiner.FIXED_LEN_ENCODER.decode(entry.getValue().get());
                    LOGGER.debug("getRowCountFromTable(%s): %s: %d", tableName, entry.getKey().getRow(), countForKey);
                    count += countForKey;
                }
                LOGGER.debug("getRowCountFromTable(%s): TOTAL: %d", tableName, count);
                return count;
            } finally {
                scanner.close();
            }
        } catch (TableNotFoundException ex) {
            throw new VertexiumException("Could not get count from table: " + tableName, ex);
        }
    }

    public void traceOn(String description) {
        traceOn(description, new HashMap<>());
    }

    @Override
    public void traceOn(String description, Map<String, String> data) {
        if (!distributedTraceEnabled) {
            try {
                ClientConfiguration conf = getConfiguration().getClientConfiguration();
                DistributedTrace.enable(null, AccumuloGraph.class.getSimpleName(), conf);
                distributedTraceEnabled = true;
            } catch (Exception e) {
                throw new VertexiumException("Could not enable DistributedTrace", e);
            }
        }
        if (Trace.isTracing()) {
            throw new VertexiumException("Trace already running");
        }
        Span span = Trace.on(description);
        for (Map.Entry<String, String> dataEntry : data.entrySet()) {
            span.data(dataEntry.getKey(), dataEntry.getValue());
        }

        LOGGER.info("Started trace '%s'", description);
    }

    public void traceOff() {
        if (!Trace.isTracing()) {
            throw new VertexiumException("No trace currently running");
        }
        Trace.off();
    }

    private void traceDataFetchHints(Span trace, FetchHints fetchHints) {
        if (Trace.isTracing()) {
            trace.data("fetchHints", fetchHints.toString());
        }
    }

    @Override
    protected Class<?> getValueType(Object value) {
        if (value instanceof StreamingPropertyValueTableRef) {
            return ((StreamingPropertyValueTableRef) value).getValueType();
        }
        return super.getValueType(value);
    }

    private class AccumuloGraphMetadataStore extends GraphMetadataStore {
        private final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(AccumuloGraphMetadataStore.class);
        private final String ZK_PATH_REPLACEMENT = "[^a-zA-Z]+";
        private final Pattern ZK_PATH_REPLACEMENT_PATTERN = Pattern.compile(ZK_PATH_REPLACEMENT);
        private final String ZK_DEFINE_PROPERTY = METADATA_DEFINE_PROPERTY_PREFIX.replaceAll(ZK_PATH_REPLACEMENT, "");
        private final CuratorFramework curatorFramework;
        private final String zkPath;
        private final TreeCache treeCache;
        private final Map<String, GraphMetadataEntry> entries = new HashMap<>();
        private final StampedLock stampedLock = new StampedLock();

        public AccumuloGraphMetadataStore(CuratorFramework curatorFramework, String zkPath) {
            this.zkPath = zkPath;
            this.curatorFramework = curatorFramework;
            this.treeCache = new TreeCache(curatorFramework, zkPath);
            this.treeCache.getListenable().addListener((client, event) -> {
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("treeCache event, clearing cache %s", event);
                }
                writeValues(entries::clear);
                getSearchIndex().clearCache();
                invalidatePropertyDefinitions(event);
            });
            try {
                this.treeCache.start();
            } catch (Exception e) {
                throw new VertexiumException("Could not start metadata sync", e);
            }
        }

        public void close() {
            this.treeCache.close();
        }

        @Override
        public Iterable<GraphMetadataEntry> getMetadata() {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("getMetadata");
            }
            return readValues(() -> toList(entries.values()));
        }

        private void ensureMetadataLoaded() {
            if (entries.size() > 0) {
                return;
            }

            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("metadata is stale... loading");
            }

            Iterable<GraphMetadataEntry> metadata = getMetadataInRange(null);
            for (GraphMetadataEntry graphMetadataEntry : metadata) {
                entries.put(graphMetadataEntry.getKey(), graphMetadataEntry);
            }
        }

        @Override
        public void setMetadata(String key, Object value) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("setMetadata: %s = %s", key, value);
            }
            try {
                Mutation m = new Mutation(key);
                byte[] valueBytes = JavaSerializableUtils.objectToBytes(value);
                m.put(AccumuloElement.METADATA_COLUMN_FAMILY, AccumuloElement.METADATA_COLUMN_QUALIFIER, new Value(valueBytes));
                BatchWriter writer = getMetadataWriter();
                writer.addMutation(m);
                flush();
            } catch (MutationsRejectedException ex) {
                throw new VertexiumException("Could not add metadata " + key, ex);
            }

            writeValues(() -> {
                entries.clear();
                try {
                    signalMetadataChange(key);
                } catch (Exception e) {
                    LOGGER.error("Could not notify other nodes via ZooKeeper", e);
                }
            });
        }

        private void invalidatePropertyDefinitions(TreeCacheEvent event) {
            if (event == null || event.getData() == null) {
                return;
            }

            String path = event.getData().getPath();
            byte[] bytes = event.getData().getData();
            if (path == null || bytes == null) {
                return;
            }

            if (!path.startsWith(zkPath + "/" + ZK_DEFINE_PROPERTY)) {
                return;
            }

            String key = new String(bytes, StandardCharsets.UTF_8);
            if (key == null) {
                return;
            }

            String propertyName = key.substring(METADATA_DEFINE_PROPERTY_PREFIX.length());
            LOGGER.debug("invalidating property definition: %s", propertyName);
            invalidatePropertyDefinition(propertyName);
        }

        private void signalMetadataChange(String key) throws Exception {
            String path = zkPath + "/" + ZK_PATH_REPLACEMENT_PATTERN.matcher(key).replaceAll("_");
            LOGGER.debug("signaling change to metadata via path: %s", path);
            byte[] data = key.getBytes(StandardCharsets.UTF_8);
            this.curatorFramework.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(path, data);
        }

        @Override
        public Object getMetadata(String key) {
            return readValues(() -> {
                GraphMetadataEntry e = entries.get(key);
                return e != null ? e.getValue() : null;
            });
        }

        private <T> T readValues(Supplier<T> reader) {
            T result = null;
            long stamp = stampedLock.tryOptimisticRead();
            if (entries.size() > 0) {
                result = reader.get();
            } else {
                stamp = 0;
            }
            if (!stampedLock.validate(stamp)) {
                stamp = stampedLock.writeLock();
                try {
                    ensureMetadataLoaded();
                    result = reader.get();
                } finally {
                    stampedLock.unlockWrite(stamp);
                }
            }
            return result;
        }

        private void writeValues(Runnable writer) {
            long stamp = stampedLock.writeLock();
            try {
                writer.run();
            } finally {
                stampedLock.unlockWrite(stamp);
            }
        }
    }
}
