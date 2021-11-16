package com.linkedin.entity.client;

import com.linkedin.common.client.BaseClient;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.AspectsDoGetTimeseriesAspectValuesRequestBuilder;
import com.linkedin.entity.AspectsDoIngestProposalRequestBuilder;
import com.linkedin.entity.AspectsGetRequestBuilder;
import com.linkedin.entity.AspectsRequestBuilders;
import com.linkedin.entity.EntitiesBatchGetRequestBuilder;
import com.linkedin.entity.EntitiesDoAutocompleteRequestBuilder;
import com.linkedin.entity.EntitiesDoBatchGetTotalEntityCountRequestBuilder;
import com.linkedin.entity.EntitiesDoBatchIngestRequestBuilder;
import com.linkedin.entity.EntitiesDoBrowseRequestBuilder;
import com.linkedin.entity.EntitiesDoDeleteRequestBuilder;
import com.linkedin.entity.EntitiesDoFilterRequestBuilder;
import com.linkedin.entity.EntitiesDoGetBrowsePathsRequestBuilder;
import com.linkedin.entity.EntitiesDoGetTotalEntityCountRequestBuilder;
import com.linkedin.entity.EntitiesDoIngestRequestBuilder;
import com.linkedin.entity.EntitiesDoSearchAcrossEntitiesRequestBuilder;
import com.linkedin.entity.EntitiesDoListUrnsRequestBuilder;
import com.linkedin.entity.EntitiesDoSearchRequestBuilder;
import com.linkedin.entity.EntitiesDoListRequestBuilder;
import com.linkedin.entity.EntitiesDoSetWritableRequestBuilder;
import com.linkedin.entity.EntitiesRequestBuilders;
import com.linkedin.entity.Entity;
import com.linkedin.entity.EntityArray;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Client;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.HttpStatus;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.search.utils.QueryUtils.newFilter;

@Slf4j
public class RestliEntityClient extends BaseClient implements EntityClient {

    private static final EntitiesRequestBuilders ENTITIES_REQUEST_BUILDERS = new EntitiesRequestBuilders();
    private static final AspectsRequestBuilders ASPECTS_REQUEST_BUILDERS = new AspectsRequestBuilders();

    public RestliEntityClient(@Nonnull final Client restliClient) {
        super(restliClient);
    }

    @Nonnull
    public Entity get(@Nonnull final Urn urn, @Nonnull final String actor) throws RemoteInvocationException {
        return sendClientRequest(
            ENTITIES_REQUEST_BUILDERS.get().id(urn.toString()),
            actor)
            .getEntity();
    }

    @Nonnull
    public Map<Urn, Entity> batchGet(@Nonnull final Set<Urn> urns, @Nonnull final String actor) throws RemoteInvocationException {

        final Integer batchSize = 25;
        final AtomicInteger index = new AtomicInteger(0);

        final Collection<List<Urn>> entityUrnBatches = urns.stream()
                .collect(Collectors.groupingBy(x -> index.getAndIncrement() / batchSize))
                .values();

        final Map<Urn, Entity> response = new HashMap<>();

        for (List<Urn> urnsInBatch : entityUrnBatches) {
            EntitiesBatchGetRequestBuilder batchGetRequestBuilder =
                    ENTITIES_REQUEST_BUILDERS.batchGet()
                            .ids(urnsInBatch.stream().map(Urn::toString).collect(Collectors.toSet()));
            final Map<Urn, Entity> batchResponse = sendClientRequest(batchGetRequestBuilder, actor).getEntity().getResults()
                    .entrySet().stream().collect(Collectors.toMap(
                            entry -> {
                                try {
                                    return Urn.createFromString(entry.getKey());
                                } catch (URISyntaxException e) {
                                   throw new RuntimeException(String.format("Failed to create Urn from key string %s", entry.getKey()));
                                }
                            },
                            entry -> entry.getValue().getEntity())
                    );
            response.putAll(batchResponse);
        }
        return response;
    }

    /**
     * Gets browse snapshot of a given path
     *
     * @param query search query
     * @param field field of the dataset
     * @param requestFilters autocomplete filters
     * @param limit max number of autocomplete results
     * @throws RemoteInvocationException
     */
    @Nonnull
    public AutoCompleteResult autoComplete(
        @Nonnull String entityType,
        @Nonnull String query,
        @Nonnull Map<String, String> requestFilters,
        @Nonnull int limit,
        @Nullable String field,
        @Nonnull String actor) throws RemoteInvocationException {
        EntitiesDoAutocompleteRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS
            .actionAutocomplete()
            .entityParam(entityType)
            .queryParam(query)
            .fieldParam(field)
            .filterParam(newFilter(requestFilters))
            .limitParam(limit);
        return sendClientRequest(requestBuilder, actor).getEntity();
    }

    /**
     * Gets browse snapshot of a given path
     *
     * @param query search query
     * @param requestFilters autocomplete filters
     * @param limit max number of autocomplete results
     * @throws RemoteInvocationException
     */
    @Nonnull
    public AutoCompleteResult autoComplete(
        @Nonnull String entityType,
        @Nonnull String query,
        @Nonnull Map<String, String> requestFilters,
        @Nonnull int limit,
        @Nonnull String actor) throws RemoteInvocationException {
        EntitiesDoAutocompleteRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS
            .actionAutocomplete()
            .entityParam(entityType)
            .queryParam(query)
            .filterParam(newFilter(requestFilters))
            .limitParam(limit);
        return sendClientRequest(requestBuilder, actor).getEntity();
    }

    /**
     * Gets browse snapshot of a given path
     *
     * @param entityType entity type being browse
     * @param path path being browsed
     * @param requestFilters browse filters
     * @param start start offset of first dataset
     * @param limit max number of datasets
     * @throws RemoteInvocationException
     */
    @Nonnull
    public BrowseResult browse(
        @Nonnull String entityType,
        @Nonnull String path,
        @Nullable Map<String, String> requestFilters,
        int start,
        int limit,
        @Nonnull String actor) throws RemoteInvocationException {
        EntitiesDoBrowseRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS
            .actionBrowse()
            .pathParam(path)
            .entityParam(entityType)
            .startParam(start)
            .limitParam(limit);
        if (requestFilters != null) {
            requestBuilder.filterParam(newFilter(requestFilters));
        }
        return sendClientRequest(requestBuilder, actor).getEntity();
    }

    public void update(@Nonnull final Entity entity, @Nonnull final String actor) throws RemoteInvocationException {
        EntitiesDoIngestRequestBuilder requestBuilder =
            ENTITIES_REQUEST_BUILDERS.actionIngest().entityParam(entity);

        sendClientRequest(requestBuilder, actor);
    }

    public void updateWithSystemMetadata(
        @Nonnull final Entity entity,
        @Nullable final SystemMetadata systemMetadata,
        @Nonnull final String actor) throws RemoteInvocationException {
        if (systemMetadata == null) {
            update(entity, actor);
            return;
        }

        EntitiesDoIngestRequestBuilder requestBuilder =
            ENTITIES_REQUEST_BUILDERS.actionIngest().entityParam(entity).systemMetadataParam(systemMetadata);

        sendClientRequest(requestBuilder, actor);
    }

    public void batchUpdate(@Nonnull final Set<Entity> entities, final String actor) throws RemoteInvocationException {
        EntitiesDoBatchIngestRequestBuilder requestBuilder =
            ENTITIES_REQUEST_BUILDERS.actionBatchIngest().entitiesParam(new EntityArray(entities));

        sendClientRequest(requestBuilder, actor);
    }

    /**
     * Searches for entities matching to a given query and filters
     *
     * @param input search query
     * @param requestFilters search filters
     * @param start start offset for search results
     * @param count max number of search results requested
     * @return a set of search results
     * @throws RemoteInvocationException
     */
    @Nonnull
    public SearchResult search(
        @Nonnull String entity,
        @Nonnull String input,
        @Nullable Map<String, String> requestFilters,
        int start,
        int count,
        @Nonnull String actor)
        throws RemoteInvocationException {

        final EntitiesDoSearchRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS.actionSearch()
            .entityParam(entity)
            .inputParam(input)
            .filterParam(newFilter(requestFilters))
            .startParam(start)
            .countParam(count);

        return sendClientRequest(requestBuilder, actor).getEntity();
    }

    /**
     * Filters for entities matching to a given query and filters
     *
     * @param requestFilters search filters
     * @param start start offset for search results
     * @param count max number of search results requested
     * @return a set of list results
     * @throws RemoteInvocationException
     */
    @Nonnull
    public ListResult list(
        @Nonnull String entity,
        @Nullable Map<String, String> requestFilters,
        int start,
        int count,
        @Nonnull String actor)
        throws RemoteInvocationException {
        final EntitiesDoListRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS.actionList()
            .entityParam(entity)
            .filterParam(newFilter(requestFilters))
            .startParam(start)
            .countParam(count);

        return sendClientRequest(requestBuilder, actor).getEntity();
    }

    /**
     * Searches for datasets matching to a given query and filters
     *
     * @param input search query
     * @param filter search filters
     * @param start start offset for search results
     * @param count max number of search results requested
     * @return Snapshot key
     * @throws RemoteInvocationException
     */
    @Nonnull
    public SearchResult search(
        @Nonnull String entity,
        @Nonnull String input,
        @Nullable Filter filter,
        int start,
        int count,
        @Nonnull String actor)
        throws RemoteInvocationException {

        final EntitiesDoSearchRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS.actionSearch()
            .entityParam(entity)
            .inputParam(input)
            .startParam(start)
            .countParam(count);

        if (filter != null) {
            requestBuilder.filterParam(filter);
        }

        return sendClientRequest(requestBuilder, actor).getEntity();
    }

    /**
     * Searches for entities matching to a given query and filters across multiple entity types
     *
     * @param entities entity types to search (if empty, searches all entities)
     * @param input search query
     * @param filter search filters
     * @param start start offset for search results
     * @param count max number of search results requested
     * @return Snapshot key
     * @throws RemoteInvocationException
     */
    @Nonnull
    public SearchResult searchAcrossEntities(
        @Nullable List<String> entities,
        @Nonnull String input,
        @Nullable Filter filter,
        int start,
        int count,
        @Nonnull String actor) throws RemoteInvocationException {

        final EntitiesDoSearchAcrossEntitiesRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS.actionSearchAcrossEntities()
            .inputParam(input)
            .startParam(start)
            .countParam(count);

        if (entities != null) {
            requestBuilder.entitiesParam(new StringArray(entities));
        }
        if (filter != null) {
            requestBuilder.filterParam(filter);
        }

        return sendClientRequest(requestBuilder, actor).getEntity();
    }

    /**
     * Gets browse path(s) given dataset urn
     *
     * @param urn urn for the entity
     * @return list of paths given urn
     * @throws RemoteInvocationException
     */
    @Nonnull
    public StringArray getBrowsePaths(@Nonnull Urn urn, @Nonnull String actor) throws RemoteInvocationException {
        EntitiesDoGetBrowsePathsRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS
            .actionGetBrowsePaths()
            .urnParam(urn);
        return sendClientRequest(requestBuilder, actor).getEntity();
    }

    public void setWritable(boolean canWrite, @Nonnull String actor) throws RemoteInvocationException {
        EntitiesDoSetWritableRequestBuilder requestBuilder =
            ENTITIES_REQUEST_BUILDERS.actionSetWritable().valueParam(canWrite);
        sendClientRequest(requestBuilder, actor);
    }

    @Nonnull
    public long getTotalEntityCount(@Nonnull String entityName, @Nonnull String actor) throws RemoteInvocationException {
        EntitiesDoGetTotalEntityCountRequestBuilder requestBuilder =
            ENTITIES_REQUEST_BUILDERS.actionGetTotalEntityCount().entityParam(entityName);
        return sendClientRequest(requestBuilder, actor).getEntity();
    }

    @Nonnull
    public Map<String, Long> batchGetTotalEntityCount(@Nonnull List<String> entityName, @Nonnull String actor) throws RemoteInvocationException {
        EntitiesDoBatchGetTotalEntityCountRequestBuilder requestBuilder =
            ENTITIES_REQUEST_BUILDERS.actionBatchGetTotalEntityCount().entitiesParam(new StringArray(entityName));
        return sendClientRequest(requestBuilder, actor).getEntity();
    }

    /**
     * List all urns existing for a particular Entity type.
     */
    public ListUrnsResult listUrns(@Nonnull final String entityName, final int start, final int count, @Nonnull final String actor)
        throws RemoteInvocationException {
        EntitiesDoListUrnsRequestBuilder requestBuilder =
            ENTITIES_REQUEST_BUILDERS.actionListUrns()
                .entityParam(entityName)
                .startParam(start)
                .countParam(count);
        return sendClientRequest(requestBuilder, actor).getEntity();
    }

    /**
     * Hard delete an entity with a particular urn.
     */
    public void deleteEntity(@Nonnull final Urn urn, @Nonnull final String actor) throws RemoteInvocationException {
        EntitiesDoDeleteRequestBuilder requestBuilder = ENTITIES_REQUEST_BUILDERS.actionDelete()
                .urnParam(urn.toString());
        sendClientRequest(requestBuilder, actor);
    }

    @Nonnull
    @Override
    public SearchResult filter(@Nonnull String entity, @Nonnull Filter filter, @Nullable SortCriterion sortCriterion,
        int start, int count, @Nonnull String actor) throws RemoteInvocationException {
        EntitiesDoFilterRequestBuilder requestBuilder =
            ENTITIES_REQUEST_BUILDERS.actionFilter()
                .entityParam(entity)
                .filterParam(filter)
                .startParam(start)
                .countParam(count);
        if (sortCriterion != null) {
            requestBuilder.sortParam(sortCriterion);
        }
        return sendClientRequest(requestBuilder, actor).getEntity();
    }

    /**
     * Gets aspect at version for an entity
     *
     * @param urn urn for the entity
     * @return list of paths given urn
     * @throws RemoteInvocationException on remote request error.
     */
    @Nonnull
    public VersionedAspect getAspect(
        @Nonnull String urn,
        @Nonnull String aspect,
        @Nonnull Long version,
        @Nonnull String actor)
        throws RemoteInvocationException {

        AspectsGetRequestBuilder requestBuilder =
            ASPECTS_REQUEST_BUILDERS.get().id(urn).aspectParam(aspect).versionParam(version);

        return sendClientRequest(requestBuilder, actor).getEntity();
    }

    /**
     * Gets aspect at version for an entity, or null if one doesn't exist.
     *
     * @param urn urn for the entity
     * @return list of paths given urn
     * @throws RemoteInvocationException on remote request error.
     */
    @Nullable
    public VersionedAspect getAspectOrNull(
        @Nonnull String urn,
        @Nonnull String aspect,
        @Nonnull Long version,
        @Nonnull String actor)
        throws RemoteInvocationException {

        AspectsGetRequestBuilder requestBuilder =
            ASPECTS_REQUEST_BUILDERS.get().id(urn).aspectParam(aspect).versionParam(version);
        try {
            return sendClientRequest(requestBuilder, actor).getEntity();
        } catch (RestLiResponseException e) {
            if (e.getStatus() == HttpStatus.S_404_NOT_FOUND.getCode()) {
                // Then the aspect was not found. Return null.
                return null;
            }
            throw e;
        }
    }

    /**
     * Retrieve instances of a particular aspect.
     *
     * @param urn urn for the entity.
     * @param entity the name of the entity.
     * @param aspect the name of the aspect.
     * @param startTimeMillis the earliest desired event time of the aspect value in milliseconds.
     * @param endTimeMillis the latest desired event time of the aspect value in milliseconds.
     * @param limit the maximum number of desired aspect values.
     * @param actor the actor associated with the request [internal]
     * @return  the list of EnvelopedAspect values satisfying the input parameters.
     * @throws RemoteInvocationException on remote request error.
     */
    @Nonnull
    public List<EnvelopedAspect> getTimeseriesAspectValues(
        @Nonnull String urn,
        @Nonnull String entity,
        @Nonnull String aspect,
        @Nullable Long startTimeMillis,
        @Nullable Long endTimeMillis,
        @Nullable Integer limit,
        @Nullable String actor
    )
        throws RemoteInvocationException {

        AspectsDoGetTimeseriesAspectValuesRequestBuilder requestBuilder =
            ASPECTS_REQUEST_BUILDERS.actionGetTimeseriesAspectValues()
                .urnParam(urn)
                .entityParam(entity)
                .aspectParam(aspect);

        if (startTimeMillis != null) {
            requestBuilder.startTimeMillisParam(startTimeMillis);
        }

        if (endTimeMillis != null) {
            requestBuilder.endTimeMillisParam(endTimeMillis);
        }

        if (limit != null) {
            requestBuilder.limitParam(limit);
        }
        return sendClientRequest(requestBuilder, actor).getEntity().getValues();
    }

    /**
     * Ingest a MetadataChangeProposal event.
     * @return
     */
    public String ingestProposal(@Nonnull final MetadataChangeProposal metadataChangeProposal, @Nonnull final String actor)
        throws RemoteInvocationException {
        final AspectsDoIngestProposalRequestBuilder requestBuilder = ASPECTS_REQUEST_BUILDERS.actionIngestProposal()
            .proposalParam(metadataChangeProposal);
        return sendClientRequest(requestBuilder, actor).getEntity();
    }

    public <T extends RecordTemplate> Optional<T> getVersionedAspect(
        @Nonnull String urn,
        @Nonnull String aspect,
        @Nonnull Long version,
        @Nonnull String actor,
        @Nonnull Class<T> aspectClass)
        throws RemoteInvocationException {

        AspectsGetRequestBuilder requestBuilder =
            ASPECTS_REQUEST_BUILDERS.get().id(urn).aspectParam(aspect).versionParam(version);

        try {
            VersionedAspect entity = sendClientRequest(requestBuilder, actor).getEntity();
            if (entity.hasAspect()) {
                DataMap rawAspect = ((DataMap) entity.data().get("aspect"));
                if (rawAspect.containsKey(aspectClass.getCanonicalName())) {
                    DataMap aspectDataMap = rawAspect.getDataMap(aspectClass.getCanonicalName());
                    return Optional.of(RecordUtils.toRecordTemplate(aspectClass, aspectDataMap));
                }
            }
        } catch (RestLiResponseException e) {
            if (e.getStatus() == 404) {
                log.debug("Could not find aspect {} for entity {}", aspect, urn);
                return Optional.empty();
            } else {
                // re-throw other exceptions
                throw e;
            }
        }

        return Optional.empty();
    }

}
