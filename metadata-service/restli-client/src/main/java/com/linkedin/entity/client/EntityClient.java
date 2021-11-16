package com.linkedin.entity.client;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.r2.RemoteInvocationException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface EntityClient {

  @Nonnull
  public Entity get(@Nonnull final Urn urn, @Nonnull final String actor) throws RemoteInvocationException;

  @Nonnull
  public Map<Urn, Entity> batchGet(@Nonnull final Set<Urn> urns, @Nonnull final String actor) throws RemoteInvocationException;
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
      @Nonnull String actor) throws RemoteInvocationException;

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
      @Nonnull String actor) throws RemoteInvocationException;

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
      @Nonnull String actor) throws RemoteInvocationException;

  public void update(@Nonnull final Entity entity, @Nonnull final String actor)
      throws RemoteInvocationException;

  public void updateWithSystemMetadata(
      @Nonnull final Entity entity,
      @Nullable final SystemMetadata systemMetadata,
      @Nonnull final String actor) throws RemoteInvocationException;

  public void batchUpdate(@Nonnull final Set<Entity> entities, final String actor)
      throws RemoteInvocationException;

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
      throws RemoteInvocationException;

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
      throws RemoteInvocationException;

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
      throws RemoteInvocationException;

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
      @Nonnull String actor) throws RemoteInvocationException;

  /**
   * Gets browse path(s) given dataset urn
   *
   * @param urn urn for the entity
   * @return list of paths given urn
   * @throws RemoteInvocationException
   */
  @Nonnull
  public StringArray getBrowsePaths(@Nonnull Urn urn, @Nonnull String actor) throws RemoteInvocationException;

  public void setWritable(boolean canWrite, @Nonnull String actor) throws RemoteInvocationException;

  @Nonnull
  public long getTotalEntityCount(@Nonnull String entityName, @Nonnull String actor) throws RemoteInvocationException;


  @Nonnull
  public Map<String, Long> batchGetTotalEntityCount(@Nonnull List<String> entityName, @Nonnull String actor)
      throws RemoteInvocationException;

  /**
   * List all urns existing for a particular Entity type.
   */
  public ListUrnsResult listUrns(@Nonnull final String entityName, final int start, final int count, @Nonnull final String actor)
      throws RemoteInvocationException;

  /**
   * Hard delete an entity with a particular urn.
   */
  public void deleteEntity(@Nonnull final Urn urn, @Nonnull final String actor) throws RemoteInvocationException;

  /**
   * Filters entities based on a particular Filter and Sort criterion
   *
   * @param entity filter entity
   * @param filter search filters
   * @param sortCriterion sort criterion
   * @param start start offset for search results
   * @param count max number of search results requested
   * @return a set of {@link SearchResult}s
   * @throws RemoteInvocationException
   */
  @Nonnull
  public SearchResult filter(
      @Nonnull String entity,
      @Nonnull Filter filter,
      @Nullable SortCriterion sortCriterion,
      int start,
      int count,
      @Nonnull String actor)
      throws RemoteInvocationException;

  public VersionedAspect getAspect(
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version,
      @Nonnull String actor)
      throws RemoteInvocationException;

  public VersionedAspect getAspectOrNull(
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version,
      @Nonnull String actor) throws RemoteInvocationException;

  public List<EnvelopedAspect> getTimeseriesAspectValues(
      @Nonnull String urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nullable Long startTimeMillis,
      @Nullable Long endTimeMillis,
      @Nullable Integer limit,
      @Nullable String actor
  ) throws RemoteInvocationException;

  public String ingestProposal(
      @Nonnull final MetadataChangeProposal metadataChangeProposal,
      @Nonnull final String actor
  ) throws RemoteInvocationException;

  public <T extends RecordTemplate> Optional<T> getVersionedAspect(
      @Nonnull String urn,
      @Nonnull String aspect,
      @Nonnull Long version,
      @Nonnull String actor,
      @Nonnull Class<T> aspectClass) throws RemoteInvocationException;
}
