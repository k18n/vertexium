package org.vertexium.inmemory;

import com.google.common.collect.ImmutableSet;
import org.vertexium.*;
import org.vertexium.event.GraphEvent;
import org.vertexium.historicalEvent.HistoricalEvent;
import org.vertexium.id.IdGenerator;
import org.vertexium.inmemory.mutations.EdgeSetupMutation;
import org.vertexium.search.SearchIndex;
import org.vertexium.util.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.vertexium.util.Preconditions.checkNotNull;
import static org.vertexium.util.StreamUtils.stream;

public class InMemoryGraph extends GraphBase {
    private static final VertexiumLogger LOGGER = VertexiumLoggerFactory.getLogger(InMemoryGraph.class);
    protected static final InMemoryGraphConfiguration DEFAULT_CONFIGURATION =
        new InMemoryGraphConfiguration(new HashMap<>());
    private final Set<String> validAuthorizations = new HashSet<>();
    private final InMemoryVertexTable vertices;
    private final InMemoryEdgeTable edges;
    private final InMemoryExtendedDataTable extendedDataTable;
    private final GraphMetadataStore graphMetadataStore;
    private final InMemoryElementMutationBuilder elementMutationBuilder;

    protected InMemoryGraph(InMemoryGraphConfiguration configuration) {
        this(
            configuration,
            new InMemoryVertexTable(),
            new InMemoryEdgeTable(),
            new MapInMemoryExtendedDataTable()
        );
    }

    protected InMemoryGraph(InMemoryGraphConfiguration configuration, IdGenerator idGenerator, SearchIndex searchIndex) {
        this(
            configuration,
            idGenerator,
            searchIndex,
            new InMemoryVertexTable(),
            new InMemoryEdgeTable(),
            new MapInMemoryExtendedDataTable()
        );
    }

    protected InMemoryGraph(
        InMemoryGraphConfiguration configuration,
        InMemoryVertexTable vertices,
        InMemoryEdgeTable edges,
        InMemoryExtendedDataTable extendedDataTable
    ) {
        super(configuration);
        this.vertices = vertices;
        this.edges = edges;
        this.extendedDataTable = extendedDataTable;
        this.graphMetadataStore = newGraphMetadataStore();
        this.elementMutationBuilder = createInMemoryElementMutationBuilder();
    }

    protected InMemoryGraph(
        InMemoryGraphConfiguration configuration,
        IdGenerator idGenerator,
        SearchIndex searchIndex,
        InMemoryVertexTable vertices,
        InMemoryEdgeTable edges,
        InMemoryExtendedDataTable extendedDataTable
    ) {
        super(configuration, idGenerator, searchIndex);
        this.vertices = vertices;
        this.edges = edges;
        this.extendedDataTable = extendedDataTable;
        this.graphMetadataStore = newGraphMetadataStore();
        this.elementMutationBuilder = createInMemoryElementMutationBuilder();
    }

    private InMemoryElementMutationBuilder createInMemoryElementMutationBuilder() {
        return new InMemoryElementMutationBuilder(
            this,
            this.vertices,
            this.edges,
            this.extendedDataTable,
            (GraphEvent event) -> {
                if (hasEventListeners()) {
                    fireGraphEvent(event);
                }
            }
        );
    }

    protected GraphMetadataStore newGraphMetadataStore() {
        return new InMemoryGraphMetadataStore();
    }

    @SuppressWarnings("unused")
    public static InMemoryGraph create() {
        return create(DEFAULT_CONFIGURATION);
    }

    public static InMemoryGraph create(InMemoryGraphConfiguration config) {
        InMemoryGraph graph = new InMemoryGraph(config);
        graph.setup();
        return graph;
    }

    @SuppressWarnings("unused")
    public static InMemoryGraph create(Map<String, Object> config) {
        return create(new InMemoryGraphConfiguration(config));
    }

    @SuppressWarnings("unused")
    public static InMemoryGraph create(InMemoryGraphConfiguration config, IdGenerator idGenerator, SearchIndex searchIndex) {
        InMemoryGraph graph = new InMemoryGraph(config, idGenerator, searchIndex);
        graph.setup();
        return graph;
    }

    @Override
    public VertexBuilder prepareVertex(String vertexId, Long timestamp, Visibility visibility) {
        checkNotNull(visibility, "visibility is required");
        if (vertexId == null) {
            vertexId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();
        }
        long finalTimestamp = timestamp;

        return new VertexBuilder(vertexId, visibility) {
            @Override
            public Vertex save(Authorizations authorizations) {
                return saveVertex(authorizations.getUser());
            }

            @Override
            public String save(User user) {
                return saveVertex(user).getId();
            }

            private Vertex saveVertex(User user) {
                return elementMutationBuilder.saveVertexBuilder(this, finalTimestamp, user);
            }
        };
    }

    void addValidAuthorizations(String[] authorizations) {
        Collections.addAll(this.validAuthorizations, authorizations);
    }

    @Override
    public Stream<Vertex> getVertices(FetchHints fetchHints, Long endTime, User user) {
        validateAuthorizations(user);
        return this.vertices.getAll(InMemoryGraph.this, fetchHints, endTime, user)
            .map(v -> v);
    }

    protected void validateAuthorizations(User user) {
        for (String auth : user.getAuthorizations()) {
            if (!this.validAuthorizations.contains(auth)) {
                throw new SecurityVertexiumException("Invalid authorizations", user);
            }
        }
    }

    @Override
    public EdgeBuilderByVertexId prepareEdge(
        String edgeId,
        String outVertexId,
        String inVertexId,
        String label,
        Long timestamp,
        Visibility visibility
    ) {
        checkNotNull(outVertexId, "outVertexId cannot be null");
        checkNotNull(inVertexId, "inVertexId cannot be null");
        checkNotNull(label, "label cannot be null");
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();

            // The timestamps will be incremented below, this will ensure future mutations will be in the future
            IncreasingTime.advanceTime(10);
        }
        long finalTimestamp = timestamp;

        return new EdgeBuilderByVertexId(edgeId, outVertexId, inVertexId, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                addValidAuthorizations(authorizations.getAuthorizations());
                return elementMutationBuilder.savePreparedEdge(
                    this,
                    getVertexId(Direction.OUT),
                    getVertexId(Direction.IN),
                    finalTimestamp,
                    authorizations.getUser()
                );
            }

            @Override
            public String save(User user) {
                addValidAuthorizations(user.getAuthorizations());
                Edge e = elementMutationBuilder.savePreparedEdge(
                    this,
                    getVertexId(Direction.OUT),
                    getVertexId(Direction.IN),
                    finalTimestamp,
                    user
                );
                return e.getId();
            }
        };
    }

    @Override
    public EdgeBuilder prepareEdge(
        String edgeId,
        Vertex outVertex,
        Vertex inVertex,
        String label,
        Long timestamp,
        Visibility visibility
    ) {
        checkNotNull(outVertex, "outVertex cannot be null");
        checkNotNull(inVertex, "inVertex cannot be null");
        checkNotNull(label, "label cannot be null");
        if (edgeId == null) {
            edgeId = getIdGenerator().nextId();
        }
        if (timestamp == null) {
            timestamp = IncreasingTime.currentTimeMillis();

            // The timestamps will be incremented below, this will ensure future mutations will be in the future
            IncreasingTime.advanceTime(10);
        }
        long finalTimestamp = timestamp;

        return new EdgeBuilder(edgeId, outVertex, inVertex, label, visibility) {
            @Override
            public Edge save(Authorizations authorizations) {
                addValidAuthorizations(authorizations.getAuthorizations());
                return elementMutationBuilder.savePreparedEdge(
                    this,
                    getOutVertex().getId(),
                    getInVertex().getId(),
                    finalTimestamp,
                    authorizations.getUser()
                );
            }

            @Override
            public String save(User user) {
                addValidAuthorizations(user.getAuthorizations());
                Edge e = elementMutationBuilder.savePreparedEdge(
                    this,
                    getOutVertex().getId(),
                    getInVertex().getId(),
                    finalTimestamp,
                    user
                );
                return e.getId();
            }
        };
    }

    @Override
    public Stream<Edge> getEdges(FetchHints fetchHints, Long endTime, User user) {
        validateAuthorizations(user);
        return this.edges.getAll(InMemoryGraph.this, fetchHints, endTime, user)
            .map(e -> e);
    }

    @Override
    protected GraphMetadataStore getGraphMetadataStore() {
        return graphMetadataStore;
    }

    @Override
    public Authorizations createAuthorizations(String... auths) {
        addValidAuthorizations(auths);
        return new InMemoryAuthorizations(auths);
    }

    private Stream<InMemoryTableEdge> getInMemoryTableEdges() {
        return edges.getAllTableElements();
    }

    private Stream<InMemoryTableEdge> getInMemoryTableEdgesForVertex(String vertexId, FetchHints fetchHints, User user) {
        return getInMemoryTableEdges()
            .filter(inMemoryTableElement -> {
                EdgeSetupMutation edgeSetupMutation = inMemoryTableElement.findLastMutation(EdgeSetupMutation.class);
                String inVertexId = edgeSetupMutation.getInVertexId();
                checkNotNull(inVertexId, "inVertexId was null");
                String outVertexId = edgeSetupMutation.getOutVertexId();
                checkNotNull(outVertexId, "outVertexId was null");

                return (inVertexId.equals(vertexId) || outVertexId.equals(vertexId)) &&
                    InMemoryGraph.this.isIncluded(inMemoryTableElement, fetchHints, user);
            });
    }

    protected Stream<Edge> getEdgesFromVertex(
        String vertexId,
        FetchHints fetchHints,
        Long endTime,
        User user
    ) {
        return getInMemoryTableEdgesForVertex(vertexId, fetchHints, user)
            .map(inMemoryTableElement -> (Edge) inMemoryTableElement.createElement(InMemoryGraph.this, fetchHints, endTime, user))
            .filter(Objects::nonNull); // edge deleted or outside of time range
    }

    protected boolean isIncluded(
        InMemoryTableElement element,
        FetchHints fetchHints,
        User user
    ) {
        boolean includeHidden = fetchHints.isIncludeHidden();

        if (!element.canRead(fetchHints, user)) {
            return false;
        }

        if (!includeHidden) {
            if (element.isHidden(user)) {
                return false;
            }
        }

        return true;
    }

    protected boolean isIncludedInTimeSpan(
        InMemoryTableElement element,
        FetchHints fetchHints,
        Long endTime,
        User user
    ) {
        boolean includeHidden = fetchHints.isIncludeHidden();

        if (!element.canRead(fetchHints, user)) {
            return false;
        }
        if (!includeHidden && element.isHidden(user)) {
            return false;
        }

        if (element.isDeleted(endTime, user)) {
            return false;
        }

        if (endTime != null && element.getFirstTimestamp() > endTime) {
            return false;
        }

        return true;
    }

    @Override
    public boolean isVisibilityValid(Visibility visibility, Authorizations authorizations) {
        return authorizations.canRead(visibility);
    }

    @Override
    public boolean isVisibilityValid(Visibility visibility, User user) {
        return user.canRead(visibility);
    }

    @Override
    public void truncate() {
        this.vertices.clear();
        this.edges.clear();
        getSearchIndex().truncate(this);
    }

    @Override
    public void drop() {
        this.vertices.clear();
        this.edges.clear();
        getSearchIndex().drop(this);
    }

    public ImmutableSet<String> getExtendedDataTableNames(
        ElementType elementType,
        String elementId,
        FetchHints fetchHints,
        User user
    ) {
        return extendedDataTable.getTableNames(elementType, elementId, fetchHints, user);
    }

    public Iterable<? extends ExtendedDataRow> getExtendedDataTable(
        ElementType elementType,
        String elementId,
        String tableName,
        FetchHints fetchHints,
        User user
    ) {
        return extendedDataTable.getTable(elementType, elementId, tableName, fetchHints, user);
    }

    @Override
    public void flushGraph() {
        // no need to do anything here
    }

    Stream<HistoricalEvent> getHistoricalVertexEdgeEvents(
        String vertexId,
        HistoricalEventsFetchHints historicalEventsFetchHints,
        User user
    ) {
        FetchHints elementFetchHints = new FetchHintsBuilder()
            .setIncludeAllProperties(true)
            .setIncludeAllPropertyMetadata(true)
            .setIncludeHidden(true)
            .setIncludeAllEdgeRefs(true)
            .build();
        return getInMemoryTableEdgesForVertex(vertexId, elementFetchHints, user)
            .flatMap(inMemoryTableElement -> inMemoryTableElement.getHistoricalEventsForVertex(vertexId, historicalEventsFetchHints));
    }

    @Override
    public Vertex getVertex(String vertexId, FetchHints fetchHints, Long endTime, User user) {
        return getVertices(fetchHints, endTime, user)
            .filter(v -> v.getId().equals(vertexId))
            .findFirst()
            .orElse(null);
    }

    @Override
    public Edge getEdge(String edgeId, FetchHints fetchHints, Long endTime, User user) {
        return getEdges(fetchHints, endTime, user)
            .filter(e -> e.getId().equals(edgeId))
            .findFirst()
            .orElse(null);
    }

    @Override
    public Stream<Vertex> getVerticesWithPrefix(String vertexIdPrefix, FetchHints fetchHints, Long endTime, User user) {
        return getVertices(fetchHints, endTime, user)
            .filter(v -> v.getId().startsWith(vertexIdPrefix));
    }

    @Override
    public Stream<Vertex> getVerticesInRange(Range idRange, FetchHints fetchHints, Long endTime, User user) {
        return getVertices(fetchHints, endTime, user)
            .filter(v -> idRange.isInRange(v.getId()));
    }

    @Override
    public Stream<Edge> getEdgesInRange(Range idRange, FetchHints fetchHints, Long endTime, User user) {
        return getEdges(fetchHints, endTime, user)
            .filter(e -> idRange.isInRange(e.getId()));
    }

    @Override
    public Stream<Vertex> getVertices(Iterable<String> ids, FetchHints fetchHints, Long endTime, User user) {
        return stream(ids)
            .distinct()
            .map(id -> getVertex(id, fetchHints, endTime, user))
            .filter(Objects::nonNull);
    }

    @Override
    public Stream<Edge> getEdges(Iterable<String> ids, FetchHints fetchHints, Long endTime, User user) {
        return stream(ids)
            .distinct()
            .map(id -> getEdge(id, fetchHints, endTime, user))
            .filter(Objects::nonNull);
    }

    @Override
    @Deprecated
    public Iterable<String> filterEdgeIdsByAuthorization(
        Iterable<String> edgeIds,
        final String authorizationToMatch,
        final EnumSet<ElementFilter> filters,
        Authorizations authorizations
    ) {
        FilterIterable<Edge> edges = new FilterIterable<Edge>(getEdges(edgeIds, FetchHints.ALL_INCLUDING_HIDDEN, authorizations)) {
            @Override
            protected boolean isIncluded(Edge edge) {
                if (filters.contains(ElementFilter.ELEMENT)) {
                    if (edge.getVisibility().hasAuthorization(authorizationToMatch)) {
                        return true;
                    }
                }
                return isIncludedByAuthorizations(edge, filters, authorizationToMatch);
            }
        };
        return new ConvertingIterable<Edge, String>(edges) {
            @Override
            protected String convert(Edge edge) {
                return edge.getId();
            }
        };
    }

    @Override
    @Deprecated
    public Iterable<String> filterVertexIdsByAuthorization(
        Iterable<String> vertexIds,
        final String authorizationToMatch,
        final EnumSet<ElementFilter> filters,
        Authorizations authorizations
    ) {
        FilterIterable<Vertex> vertices = new FilterIterable<Vertex>(getVertices(vertexIds, FetchHints.ALL_INCLUDING_HIDDEN, authorizations)) {
            @Override
            protected boolean isIncluded(Vertex vertex) {
                if (filters.contains(ElementFilter.ELEMENT)) {
                    if (vertex.getVisibility().hasAuthorization(authorizationToMatch)) {
                        return true;
                    }
                }
                return isIncludedByAuthorizations(vertex, filters, authorizationToMatch);
            }
        };
        return new ConvertingIterable<Vertex, String>(vertices) {
            @Override
            protected String convert(Vertex vertex) {
                return vertex.getId();
            }
        };
    }

    private boolean isIncludedByAuthorizations(Element element, EnumSet<ElementFilter> filters, String authorizationToMatch) {
        if (filters.contains(ElementFilter.PROPERTY) || filters.contains(ElementFilter.PROPERTY_METADATA)) {
            for (Property property : element.getProperties()) {
                if (filters.contains(ElementFilter.PROPERTY)) {
                    if (property.getVisibility().hasAuthorization(authorizationToMatch)) {
                        return true;
                    }
                }
                if (filters.contains(ElementFilter.PROPERTY_METADATA)) {
                    for (Metadata.Entry entry : property.getMetadata().entrySet()) {
                        if (entry.getVisibility().hasAuthorization(authorizationToMatch)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
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

        FetchHints fetchHints = FetchHints.EDGE_REFS;
        Vertex sourceVertex = getVertex(options.getSourceVertexId(), fetchHints, user);
        if (sourceVertex == null) {
            throw new IllegalArgumentException("Could not find vertex with id: " + options.getSourceVertexId());
        }
        Vertex destVertex = getVertex(options.getDestVertexId(), fetchHints, user);
        if (destVertex == null) {
            throw new IllegalArgumentException("Could not find vertex with id: " + options.getDestVertexId());
        }

        progressCallback.progress(0, ProgressCallback.Step.FINDING_PATH);

        Set<String> seenVertices = new HashSet<>();
        seenVertices.add(sourceVertex.getId());

        Path startPath = new Path(sourceVertex.getId());

        List<Path> foundPaths = new ArrayList<>();
        if (options.getMaxHops() == 2) {
            findPathsSetIntersection(
                options,
                foundPaths,
                sourceVertex,
                destVertex,
                progressCallback,
                user
            );
        } else {
            findPathsRecursive(
                options,
                foundPaths,
                sourceVertex,
                destVertex,
                options.getMaxHops(),
                seenVertices,
                startPath,
                progressCallback,
                user
            );
        }

        progressCallback.progress(1, ProgressCallback.Step.COMPLETE);
        return foundPaths.stream();
    }

    private void findPathsSetIntersection(
        FindPathOptions options,
        List<Path> foundPaths,
        Vertex sourceVertex,
        Vertex destVertex,
        ProgressCallback progressCallback,
        User user
    ) {
        String sourceVertexId = sourceVertex.getId();
        String destVertexId = destVertex.getId();

        progressCallback.progress(0.1, ProgressCallback.Step.SEARCHING_SOURCE_VERTEX_EDGES);
        Set<String> sourceVertexConnectedVertexIds = filterFindPathEdgeInfo(options, sourceVertex.getEdgeInfos(Direction.BOTH, options.getLabels(), user));
        Map<String, Boolean> sourceVerticesExist = doVerticesExist(sourceVertexConnectedVertexIds, user);
        sourceVertexConnectedVertexIds = stream(sourceVerticesExist.keySet())
            .filter(key -> sourceVerticesExist.getOrDefault(key, false))
            .collect(Collectors.toSet());

        progressCallback.progress(0.3, ProgressCallback.Step.SEARCHING_DESTINATION_VERTEX_EDGES);
        Set<String> destVertexConnectedVertexIds = filterFindPathEdgeInfo(options, destVertex.getEdgeInfos(Direction.BOTH, options.getLabels(), user));
        Map<String, Boolean> destVerticesExist = doVerticesExist(destVertexConnectedVertexIds, user);
        destVertexConnectedVertexIds = stream(destVerticesExist.keySet())
            .filter(key -> destVerticesExist.getOrDefault(key, false))
            .collect(Collectors.toSet());

        if (sourceVertexConnectedVertexIds.contains(destVertexId)) {
            foundPaths.add(new Path(sourceVertexId, destVertexId));
            if (options.isGetAnyPath()) {
                return;
            }
        }

        progressCallback.progress(0.6, ProgressCallback.Step.MERGING_EDGES);
        sourceVertexConnectedVertexIds.retainAll(destVertexConnectedVertexIds);

        progressCallback.progress(0.9, ProgressCallback.Step.ADDING_PATHS);
        for (String connectedVertexId : sourceVertexConnectedVertexIds) {
            foundPaths.add(new Path(sourceVertexId, connectedVertexId, destVertexId));
        }
    }

    private void findPathsRecursive(
        FindPathOptions options,
        List<Path> foundPaths,
        Vertex sourceVertex,
        Vertex destVertex,
        int hops,
        Set<String> seenVertices,
        Path currentPath,
        ProgressCallback progressCallback,
        User user
    ) {
        // if this is our first source vertex report progress back to the progress callback
        boolean firstLevelRecursion = hops == options.getMaxHops();

        if (options.isGetAnyPath() && foundPaths.size() == 1) {
            return;
        }

        seenVertices.add(sourceVertex.getId());
        if (sourceVertex.getId().equals(destVertex.getId())) {
            foundPaths.add(currentPath);
        } else if (hops > 0) {
            Iterable<Vertex> vertices = filterFindPathEdgePairs(options, sourceVertex.getEdgeVertexPairs(Direction.BOTH, options.getLabels(), user));
            int vertexCount = 0;
            if (firstLevelRecursion) {
                vertices = IterableUtils.toList(vertices);
                vertexCount = ((List<Vertex>) vertices).size();
            }
            int i = 0;
            for (Vertex child : vertices) {
                if (firstLevelRecursion) {
                    // this will never get to 100% since i starts at 0. which is good. 100% signifies done and we still have work to do.
                    double progressPercent = (double) i / (double) vertexCount;
                    progressCallback.progress(progressPercent, ProgressCallback.Step.SEARCHING_EDGES, i + 1, vertexCount);
                }
                if (!seenVertices.contains(child.getId())) {
                    findPathsRecursive(options, foundPaths, child, destVertex, hops - 1, seenVertices, new Path(currentPath, child.getId()), progressCallback, user);
                }
                i++;
            }
        }
        seenVertices.remove(sourceVertex.getId());
    }

    private Set<String> filterFindPathEdgeInfo(FindPathOptions options, Stream<EdgeInfo> edgeInfos) {
        return edgeInfos
            .filter(edgeInfo -> {
                if (options.getExcludedLabels() != null) {
                    return !ArrayUtils.contains(options.getExcludedLabels(), edgeInfo.getLabel());
                }
                return true;
            })
            .map(EdgeInfo::getVertexId)
            .collect(Collectors.toSet());
    }

    private Iterable<Vertex> filterFindPathEdgePairs(FindPathOptions options, Stream<EdgeVertexPair> edgeVertexPairs) {
        return edgeVertexPairs
            .filter(edgePair -> {
                if (options.getExcludedLabels() != null) {
                    return !ArrayUtils.contains(options.getExcludedLabels(), edgePair.getEdge().getLabel());
                }
                return true;
            })
            .map(EdgeVertexPair::getVertex)
            .collect(Collectors.toList());
    }

    @Override
    public Stream<String> findRelatedEdgeIds(Iterable<String> vertexIds, Long endTime, User user) {
        FetchHints fetchHints = new FetchHintsBuilder()
            .setIncludeOutEdgeRefs(true)
            .build();
        return findRelatedEdgeIdsForVertices(
            getVertices(vertexIds, fetchHints, endTime, user).collect(Collectors.toList()),
            user
        );
    }

    @Override
    public Stream<RelatedEdge> findRelatedEdgeSummary(Iterable<String> vertexIds, Long endTime, User user) {
        FetchHints fetchHints = new FetchHintsBuilder()
            .setIncludeOutEdgeRefs(true)
            .build();
        return findRelatedEdgeSummaryForVertices(
            getVertices(vertexIds, fetchHints, endTime, user).collect(Collectors.toList()),
            user
        );
    }

    public InMemoryElementMutationBuilder getElementMutationBuilder() {
        return elementMutationBuilder;
    }
}
