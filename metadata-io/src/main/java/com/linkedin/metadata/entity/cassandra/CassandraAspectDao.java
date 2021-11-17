package com.linkedin.metadata.entity.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.paging.OffsetPager;
import com.datastax.oss.driver.api.core.paging.OffsetPager.Page;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.dao.exception.ModelConversionException;
import com.linkedin.metadata.dao.exception.RetryLimitReached;
import com.linkedin.metadata.dao.retention.IndefiniteRetention;
import com.linkedin.metadata.dao.retention.Retention;
import com.linkedin.metadata.dao.retention.TimeBasedRetention;
import com.linkedin.metadata.dao.retention.VersionBasedRetention;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.ListResultMetadata;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.time.Clock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;

import lombok.extern.slf4j.Slf4j;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.deleteFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.update;
import static com.linkedin.metadata.Constants.*;

@Slf4j
public class CassandraAspectDao implements AspectDao {
  protected final CqlSession _cqlSession;
  private static final IndefiniteRetention INDEFINITE_RETENTION = new IndefiniteRetention();
  private final Map<String, Retention> _aspectRetentionMap = new HashMap<>();
  private final Clock _clock = Clock.systemUTC();

  public CassandraAspectDao(@Nonnull final Map<String, String> sessionConfig) {

    int port = Integer.parseInt(sessionConfig.get("port"));
    List<InetSocketAddress> addresses = Arrays.stream(sessionConfig.get("hosts").split(","))
      .map(host -> new InetSocketAddress(host, port))
      .collect(Collectors.toList());

    String dc = sessionConfig.get("datacenter");
    String ks = sessionConfig.get("keyspace");
    String username = sessionConfig.get("username");
    String password = sessionConfig.get("password");

    CqlSessionBuilder csb = CqlSession.builder()
      .addContactPoints(addresses)
      .withLocalDatacenter(dc)
      .withKeyspace(ks)
      .withAuthCredentials(username, password);

    if (sessionConfig.containsKey("useSsl") && sessionConfig.get("useSsl").equals("true")) {
      try {
        csb = csb.withSslContext(SSLContext.getDefault());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    _cqlSession = csb.build();
  }

  public CassandraAspect getLatestAspect(String urn, String aspectName) {
    return getAspect(urn, aspectName, 0L);
  }

  public long getMaxVersion(@Nonnull final String urn, @Nonnull final String aspectName) {
    SimpleStatement ss = selectFrom(CassandraAspect.TABLE_NAME)
      .all()
      .whereColumn(CassandraAspect.URN_COLUMN)
      .isEqualTo(literal(urn))
      .whereColumn(CassandraAspect.ASPECT_COLUMN)
      .isEqualTo(literal(aspectName))
      .build();

    ResultSet rs = _cqlSession.execute(ss);
    List<Row> rows = rs.all();

    return getMaxVersion(rows);
  }

  private long getMaxVersion(List<Row> rows) {
    long maxVersion = -1;
    for (Row r : rows) {
      if (r.getLong(CassandraAspect.VERSION_COLUMN) > maxVersion) {
        maxVersion = r.getLong(CassandraAspect.VERSION_COLUMN);
      }
    }
    return maxVersion;
  }

  private Insert createCondInsertStatement(CassandraAspect cassandraAspect) {

    String entity;

    try {
      entity = (new Urn(cassandraAspect.getUrn())).getEntityType();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    return insertInto(CassandraAspect.TABLE_NAME)
            .value(CassandraAspect.URN_COLUMN, literal(cassandraAspect.getUrn()))
            .value(CassandraAspect.ASPECT_COLUMN, literal(cassandraAspect.getAspect()))
            .value(CassandraAspect.VERSION_COLUMN, literal(cassandraAspect.getVersion()))
            .value(CassandraAspect.SYSTEM_METADATA_COLUMN, literal(cassandraAspect.getSystemMetadata()))
            .value(CassandraAspect.METADATA_COLUMN, literal(cassandraAspect.getMetadata()))
            .value(CassandraAspect.CREATED_ON_COLUMN, literal(cassandraAspect.getCreatedOn().getTime()))
            .value(CassandraAspect.CREATED_FOR_COLUMN, literal(cassandraAspect.getCreatedFor()))
            .value(CassandraAspect.ENTITY_COLUMN, literal(entity))
            .value(CassandraAspect.CREATED_BY_COLUMN, literal(cassandraAspect.getCreatedBy()))
            .ifNotExists();
  }

  private Update createUpdateStatement(CassandraAspect newCassandraAspect, CassandraAspect oldCassandraAspect) {
    return update(CassandraAspect.TABLE_NAME)
            .setColumn(CassandraAspect.METADATA_COLUMN, literal(newCassandraAspect.getMetadata()))
            .setColumn(CassandraAspect.SYSTEM_METADATA_COLUMN, literal(newCassandraAspect.getSystemMetadata()))
            .setColumn(CassandraAspect.CREATED_ON_COLUMN, literal(newCassandraAspect.getCreatedOn().getTime()))
            .setColumn(CassandraAspect.CREATED_BY_COLUMN, literal(newCassandraAspect.getCreatedBy()))
            .setColumn(CassandraAspect.CREATED_FOR_COLUMN, literal(newCassandraAspect.getCreatedFor()))
            .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(newCassandraAspect.getUrn()))
            .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(newCassandraAspect.getAspect()))
            .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(newCassandraAspect.getVersion()));
  }

  public long batchSaveLatestAspect(
          @Nonnull final String urn,
          @Nonnull final String aspectName,
          @Nullable final String oldAspectMetadata,
          @Nullable final String oldActor,
          @Nullable final String oldImpersonator,
          @Nullable final Timestamp oldTime,
          @Nullable final String oldSystemMetadata,
          @Nonnull final String newAspectMetadata,
          @Nonnull final String newActor,
          @Nullable final String newImpersonator,
          @Nonnull final Timestamp newTime,
          @Nullable final String newSystemMetadata,
          final int nextVersion
  ) {
    BatchStatementBuilder batch = BatchStatement.builder(BatchType.LOGGED);
    CassandraAspect oldCassandraAspect = new CassandraAspect(
            urn, aspectName, nextVersion, oldAspectMetadata, oldSystemMetadata, oldTime, oldActor, oldImpersonator);

    if (oldAspectMetadata != null && oldTime != null) {
      // Save oldValue as nextVersion
      batch = batch.addStatement(createCondInsertStatement(oldCassandraAspect).build());
    }

    // Save newValue as the latest version (v0)
    CassandraAspect newCassandraAspect = new CassandraAspect(urn, aspectName, 0, newAspectMetadata, newSystemMetadata, newTime, newActor, newImpersonator);

    if (nextVersion == 0)  {
      batch = batch.addStatement(createCondInsertStatement(newCassandraAspect).build());
    } else {
      // We don't need to add a condition here as the conditional insert will fail if another thread has updated the
      // aspect in the meantime
      batch = batch.addStatement(createUpdateStatement(newCassandraAspect, oldCassandraAspect).build());
    }

    ResultSet rs = _cqlSession.execute(batch.build());
    if (!rs.wasApplied()) {
      throw new ConditionalWriteFailedException("Conditional entity ingestion failed");
    }

    // Apply retention policy
    applyRetention(urn, aspectName, getRetention(aspectName), nextVersion);

    return nextVersion;
  }

  private void applyRetention(
      @Nonnull final String urn,
      @Nonnull final String aspectName,
      @Nonnull final Retention retention,
      long largestVersion) {
    if (retention instanceof IndefiniteRetention) {
      return;
    }

    if (retention instanceof VersionBasedRetention) {
      applyVersionBasedRetention(urn, aspectName, (VersionBasedRetention) retention, largestVersion);
      return;
    }

    if (retention instanceof TimeBasedRetention) {
      applyTimeBasedRetention(urn, aspectName, (TimeBasedRetention) retention, _clock.millis());
    }
  }

  protected void applyVersionBasedRetention(
      @Nonnull final String urn,
      @Nonnull final String aspectName,
      @Nonnull final VersionBasedRetention retention,
      long largestVersion) {

    SimpleStatement ss = deleteFrom(CassandraAspect.TABLE_NAME)
      .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(urn))
      .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
      .whereColumn(CassandraAspect.VERSION_COLUMN).isNotEqualTo(literal(0))
      .whereColumn(CassandraAspect.VERSION_COLUMN).isLessThanOrEqualTo(literal(largestVersion - retention.getMaxVersionsToRetain() + 1))
      .build();

    _cqlSession.execute(ss);
  }

  protected void applyTimeBasedRetention(
      @Nonnull final String urn,
      @Nonnull final String aspectName,
      @Nonnull final TimeBasedRetention retention,
      long currentTime) {
    SimpleStatement ss = deleteFrom(CassandraAspect.TABLE_NAME)
      .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(urn))
      .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
      .whereColumn(CassandraAspect.CREATED_ON_COLUMN).isLessThanOrEqualTo(literal(new Timestamp(currentTime - retention.getMaxAgeToRetain())))
      .build();

    _cqlSession.execute(ss);
  }


  @Nonnull
  public Retention getRetention(@Nonnull final String aspectName) {
    return _aspectRetentionMap.getOrDefault(aspectName, INDEFINITE_RETENTION);
  }

  @Nonnull
  public <T> T runInConditionalWithRetry(@Nonnull final Supplier<T> block, final int maxConditionalRetry) {
    int retryCount = 0;
    Exception lastException;

    T result = null;
    do {
      try {
        result = block.get();
        lastException = null;
        break;
      } catch (ConditionalWriteFailedException exception) {
        lastException = exception;
      }
    } while (++retryCount <= maxConditionalRetry);

    if (lastException != null) {
      throw new RetryLimitReached("Failed to add after " + maxConditionalRetry + " retries", lastException);
    }

    return result;
  }

  public ResultSet updateSystemMetadata(@Nonnull CassandraAspect cassandraAspect) {

      Update u = update(CassandraAspect.TABLE_NAME)
              .setColumn(CassandraAspect.SYSTEM_METADATA_COLUMN, literal(cassandraAspect.getSystemMetadata()))
              .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(cassandraAspect.getUrn()))
              .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(cassandraAspect.getAspect()))
              .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(cassandraAspect.getVersion()));

      return _cqlSession.execute(u.build());
    }

  public ResultSet condUpsertAspect(CassandraAspect cassandraAspect, CassandraAspect oldCassandraAspect) {

    String entity;

    try {
      entity = (new Urn(cassandraAspect.getUrn())).getEntityType();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    if (oldCassandraAspect == null) {
      return _cqlSession.execute(createCondInsertStatement(cassandraAspect).build());
    } else {
      Update u = update(CassandraAspect.TABLE_NAME)
              .setColumn(CassandraAspect.METADATA_COLUMN, literal(cassandraAspect.getMetadata()))
              .setColumn(CassandraAspect.SYSTEM_METADATA_COLUMN, literal(cassandraAspect.getSystemMetadata()))
              .setColumn(CassandraAspect.CREATED_ON_COLUMN, literal(cassandraAspect.getCreatedOn() == null ? null : cassandraAspect.getCreatedOn().getTime()))
              .setColumn(CassandraAspect.CREATED_BY_COLUMN, literal(cassandraAspect.getCreatedBy()))
              .setColumn(CassandraAspect.ENTITY_COLUMN, literal(entity))
              .setColumn(CassandraAspect.CREATED_FOR_COLUMN, literal(cassandraAspect.getCreatedFor()))
              .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(cassandraAspect.getUrn()))
              .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(cassandraAspect.getAspect()))
              .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(cassandraAspect.getVersion()));

      return _cqlSession.execute(u.build());
    }
  }

  public void saveAspect(CassandraAspect cassandraAspect, final boolean insert) {

    String entity;

    try {
      entity = (new Urn(cassandraAspect.getUrn())).getEntityType();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    if (insert) {
      Insert ri = insertInto(CassandraAspect.TABLE_NAME)
        .value(CassandraAspect.URN_COLUMN, literal(cassandraAspect.getUrn()))
        .value(CassandraAspect.ASPECT_COLUMN, literal(cassandraAspect.getAspect()))
        .value(CassandraAspect.VERSION_COLUMN, literal(cassandraAspect.getVersion()))
        .value(CassandraAspect.SYSTEM_METADATA_COLUMN, literal(cassandraAspect.getSystemMetadata()))
        .value(CassandraAspect.METADATA_COLUMN, literal(cassandraAspect.getMetadata()))
        .value(CassandraAspect.CREATED_ON_COLUMN, literal(cassandraAspect.getCreatedOn().getTime()))
        .value(CassandraAspect.CREATED_FOR_COLUMN, literal(cassandraAspect.getCreatedFor()))
        .value(CassandraAspect.ENTITY_COLUMN, literal(entity))
        .value(CassandraAspect.CREATED_BY_COLUMN, literal(cassandraAspect.getCreatedBy()));
      _cqlSession.execute(ri.build());
    } else {

      UpdateWithAssignments uwa = update(CassandraAspect.TABLE_NAME)
        .setColumn(CassandraAspect.METADATA_COLUMN, literal(cassandraAspect.getMetadata()))
        .setColumn(CassandraAspect.SYSTEM_METADATA_COLUMN, literal(cassandraAspect.getSystemMetadata()))
        .setColumn(CassandraAspect.CREATED_ON_COLUMN, literal(cassandraAspect.getCreatedOn().getTime()))
        .setColumn(CassandraAspect.CREATED_BY_COLUMN, literal(cassandraAspect.getCreatedBy()))
        .setColumn(CassandraAspect.CREATED_FOR_COLUMN, literal(cassandraAspect.getCreatedFor()));

      Update u = uwa.whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(cassandraAspect.getUrn()))
        .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(cassandraAspect.getAspect()))
        .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(cassandraAspect.getVersion()));

      _cqlSession.execute(u.build());
    }
  }

  // TODO: can further improve by running the sub queries in parallel
  // TODO: look into supporting pagination
  @Nonnull
  public Map<CassandraAspect.PrimaryKey, CassandraAspect> batchGet(@Nonnull final Set<CassandraAspect.PrimaryKey> keys) {
    return keys.stream()
            .map(k -> getAspect(k))
            .filter(Objects::nonNull)
            .collect(Collectors.toMap(CassandraAspect::toPrimaryKey, record -> record));
  }

  public CassandraAspect getAspect(CassandraAspect.PrimaryKey pk) {
    return getAspect(pk.getUrn(), pk.getAspect(), pk.getVersion());
  }

  @Nonnull
  public ListResult<String> listLatestAspectMetadata(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final int start,
      final int pageSize) {
    return listAspectMetadata(entityName, aspectName, ASPECT_LATEST_VERSION, start, pageSize);
  }

  @Nonnull
  public ListResult<String> listAspectMetadata(
      @Nonnull final String entityName,
      @Nonnull final String aspectName,
      final long version,
      final int start,
      final int pageSize) {

    OffsetPager offsetPager = new OffsetPager(pageSize);

    SimpleStatement ss = selectFrom(CassandraAspect.TABLE_NAME)
      .all()
      .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
      .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(version))
      .whereColumn(CassandraAspect.ENTITY_COLUMN).isEqualTo(literal(entityName))
      .allowFiltering()
      .build();

    ResultSet rs = _cqlSession.execute(ss);

    int pageNumber = start / pageSize + 1;

    Page<Row> page = offsetPager.getPage(rs, pageNumber);

    final List<CassandraAspect> aspects = page
      .getElements()
      .stream().map(this::toCassandraAspect)
      .collect(Collectors.toList());

    // TODO: address performance issue for getting total count
    // https://www.datastax.com/blog/running-count-expensive-cassandra
    SimpleStatement ssCount = selectFrom(CassandraAspect.TABLE_NAME)
      .countAll()
      .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
      .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(version))
      .whereColumn(CassandraAspect.ENTITY_COLUMN).isEqualTo(literal(entityName))
      .allowFiltering()
      .build();

    long totalCount = _cqlSession.execute(ssCount).one().getLong(0);

    final List<String> aspectMetadatas = aspects
            .stream()
            .map(CassandraAspect::getMetadata)
            .collect(Collectors.toList());

    final ListResultMetadata listResultMetadata = toListResultMetadata(aspects
            .stream()
            .map(CassandraAspectDao::toExtraInfo)
            .collect(Collectors.toList()));

    return toListResult(aspectMetadatas, listResultMetadata, start, pageNumber, pageSize, totalCount);
  }

  private <T> ListResult<T> toListResult(
      @Nonnull final List<T> values,
      final ListResultMetadata listResultMetadata,
      @Nonnull final Integer start,
      @Nonnull final Integer pageNumber,
      @Nonnull final Integer pageSize,
      final long totalCount) {
    final int numPages = (int) (totalCount / pageSize + (totalCount % pageSize == 0 ? 0 : 1));
    final boolean hasNext = pageNumber < numPages;

    final int nextStart =
      (start != null && hasNext) ? (pageNumber * pageSize) : ListResult.INVALID_NEXT_START;

    return ListResult.<T>builder()
      .values(values)
      .metadata(listResultMetadata)
      .nextStart(nextStart)
      .hasNext(hasNext)
      .totalCount((int) totalCount)
      .totalPageCount(numPages)
      .pageSize(pageSize)
      .build();
  }

  @Nonnull
  private ListResultMetadata toListResultMetadata(@Nonnull final List<ExtraInfo> extraInfos) {
    final ListResultMetadata listResultMetadata = new ListResultMetadata();
    listResultMetadata.setExtraInfos(new ExtraInfoArray(extraInfos));
    return listResultMetadata;
  }

  @Nonnull
  private static ExtraInfo toExtraInfo(@Nonnull final CassandraAspect aspect) {
    final ExtraInfo extraInfo = new ExtraInfo();
    extraInfo.setVersion(aspect.getVersion());
    extraInfo.setAudit(toAuditStamp(aspect));
    try {
      extraInfo.setUrn(Urn.createFromString(aspect.getUrn()));
    } catch (URISyntaxException e) {
      throw new ModelConversionException(e.getMessage());
    }

    return extraInfo;
  }

  @Nonnull
  private static AuditStamp toAuditStamp(@Nonnull final CassandraAspect aspect) {
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(aspect.getCreatedOn().getTime());

    try {
      auditStamp.setActor(new Urn(aspect.getCreatedBy()));
      if (aspect.getCreatedFor() != null) {
        auditStamp.setImpersonator(new Urn(aspect.getCreatedFor()));
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return auditStamp;
  }

  public boolean deleteAspect(@Nonnull final CassandraAspect aspect) {
      SimpleStatement ss = deleteFrom(CassandraAspect.TABLE_NAME)
      .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(aspect.getUrn()))
      .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspect.getAspect()))
      .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(aspect.getVersion()))
        .build();
    ResultSet rs = _cqlSession.execute(ss);

    return rs.getExecutionInfo().getErrors().size() == 0;
  }

  public int deleteUrn(@Nonnull final String urn) {
      SimpleStatement ss = deleteFrom(CassandraAspect.TABLE_NAME)
      .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(urn))
        .build();
    ResultSet rs = _cqlSession.execute(ss);
    // TODO: look into how to get around this for counts in Cassandra
    // https://stackoverflow.com/questions/28611459/how-to-know-affected-rows-in-cassandracql
    return rs.getExecutionInfo().getErrors().size() == 0 ? -1 : 0;
  }

  @Nullable
  public Optional<CassandraAspect> getEarliestAspect(@Nonnull final String urn) {
    SimpleStatement ss = selectFrom(CassandraAspect.TABLE_NAME)
      .all()
      .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(urn))
      .build();

    ResultSet rs = _cqlSession.execute(ss);
    List<CassandraAspect> das = rs.all().stream().map(this::toCassandraAspect).collect(Collectors.toList());

    if (das.size() == 0) {
      return Optional.empty();
    }

    return das.stream().reduce((d1, d2) -> d1.getCreatedOn().before(d2.getCreatedOn()) ? d1 : d2);
  }

  public List<CassandraAspect> getAllAspects(String urn, String aspectName) {
    SimpleStatement ss = selectFrom(CassandraAspect.TABLE_NAME)
            .all()
            .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(urn))
            .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
            .build();

    ResultSet rs = _cqlSession.execute(ss);

    return rs.all().stream().map(this::toCassandraAspect).collect(Collectors.toList());
  }

  public CassandraAspect getAspect(String urn, String aspectName, long version) {
    SimpleStatement ss = selectFrom(CassandraAspect.TABLE_NAME)
      .all()
      .whereColumn(CassandraAspect.URN_COLUMN).isEqualTo(literal(urn))
      .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
      .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(version))
      .limit(1)
      .build();


    ResultSet rs = _cqlSession.execute(ss);
    Row r = rs.one();

    if (r == null) {
      return null;
    }

    return toCassandraAspect(r);
  }

  @Nonnull
  public ListResult<String> listUrns(
          @Nonnull final String aspectName,
          final int start,
          final int pageSize) {

    OffsetPager offsetPager = new OffsetPager(pageSize);

    SimpleStatement ss = selectFrom(CassandraAspect.TABLE_NAME)
            .all()
            .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
            .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(ASPECT_LATEST_VERSION))
            .allowFiltering()
            .build();

    ResultSet rs = _cqlSession.execute(ss);

    int pageNumber = start / pageSize + 1;

    Page<Row> page = offsetPager.getPage(rs, pageNumber);

    final List<String> urns = page
            .getElements()
            .stream().map(r -> toCassandraAspect(r).getUrn())
            .collect(Collectors.toList());

    // TODO: address performance issue for getting total count
    // https://www.datastax.com/blog/running-count-expensive-cassandra
    SimpleStatement ssCount = selectFrom(CassandraAspect.TABLE_NAME)
            .countAll()
            .whereColumn(CassandraAspect.ASPECT_COLUMN).isEqualTo(literal(aspectName))
            .whereColumn(CassandraAspect.VERSION_COLUMN).isEqualTo(literal(ASPECT_LATEST_VERSION))
            .allowFiltering()
            .build();

    long totalCount = _cqlSession.execute(ssCount).one().getLong(0);

    return toListResult(urns, null, start, pageNumber, pageSize, totalCount);
  }

  private CassandraAspect toCassandraAspect(Row r) {
    return new CassandraAspect(
            r.getString(CassandraAspect.URN_COLUMN),
            r.getString(CassandraAspect.ASPECT_COLUMN),
            r.getLong(CassandraAspect.VERSION_COLUMN),
            r.getString(CassandraAspect.METADATA_COLUMN),
            r.getString(CassandraAspect.SYSTEM_METADATA_COLUMN),
            r.getInstant(CassandraAspect.CREATED_ON_COLUMN) == null ? null : Timestamp.from(r.getInstant(CassandraAspect.CREATED_ON_COLUMN)),
            r.getString(CassandraAspect.CREATED_BY_COLUMN),
            r.getString(CassandraAspect.CREATED_FOR_COLUMN));
  }
}