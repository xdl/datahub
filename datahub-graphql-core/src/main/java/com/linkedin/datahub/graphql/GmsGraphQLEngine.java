package com.linkedin.datahub.graphql;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.analytics.resolver.AnalyticsChartTypeResolver;
import com.linkedin.datahub.graphql.analytics.resolver.GetChartsResolver;
import com.linkedin.datahub.graphql.analytics.resolver.GetHighlightsResolver;
import com.linkedin.datahub.graphql.analytics.resolver.IsAnalyticsEnabledResolver;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.generated.Aspect;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.ChartInfo;
import com.linkedin.datahub.graphql.generated.DashboardInfo;
import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.DataJobInputOutput;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityRelationship;
import com.linkedin.datahub.graphql.generated.EntityRelationshipLegacy;
import com.linkedin.datahub.graphql.generated.ForeignKeyConstraint;
import com.linkedin.datahub.graphql.generated.MLModelProperties;
import com.linkedin.datahub.graphql.generated.RelatedDataset;
import com.linkedin.datahub.graphql.generated.SearchResult;
import com.linkedin.datahub.graphql.generated.InstitutionalMemoryMetadata;
import com.linkedin.datahub.graphql.generated.UsageQueryResult;
import com.linkedin.datahub.graphql.generated.UserUsageCounts;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.CorpUserInfo;
import com.linkedin.datahub.graphql.generated.CorpGroupInfo;
import com.linkedin.datahub.graphql.generated.Owner;
import com.linkedin.datahub.graphql.generated.MLModel;
import com.linkedin.datahub.graphql.generated.MLModelGroup;
import com.linkedin.datahub.graphql.generated.MLFeatureTable;
import com.linkedin.datahub.graphql.generated.MLFeatureTableProperties;
import com.linkedin.datahub.graphql.generated.MLFeature;
import com.linkedin.datahub.graphql.generated.MLFeatureProperties;
import com.linkedin.datahub.graphql.generated.MLPrimaryKey;
import com.linkedin.datahub.graphql.generated.MLPrimaryKeyProperties;
import com.linkedin.datahub.graphql.resolvers.MeResolver;
import com.linkedin.datahub.graphql.resolvers.load.AspectResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityTypeBatchResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityTypeResolver;
import com.linkedin.datahub.graphql.resolvers.load.LoadableTypeBatchResolver;
import com.linkedin.datahub.graphql.resolvers.load.EntityRelationshipsResultResolver;
import com.linkedin.datahub.graphql.resolvers.load.TimeSeriesAspectResolver;
import com.linkedin.datahub.graphql.resolvers.load.UsageTypeResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.AddLinkResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.AddOwnerResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.AddTagResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.AddTermResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.MutableTypeResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.RemoveLinkResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.RemoveOwnerResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.RemoveTagResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.RemoveTermResolver;
import com.linkedin.datahub.graphql.resolvers.mutate.UpdateFieldDescriptionResolver;
import com.linkedin.datahub.graphql.resolvers.policy.DeletePolicyResolver;
import com.linkedin.datahub.graphql.resolvers.policy.ListPoliciesResolver;
import com.linkedin.datahub.graphql.resolvers.config.AppConfigResolver;
import com.linkedin.datahub.graphql.resolvers.policy.UpsertPolicyResolver;
import com.linkedin.datahub.graphql.resolvers.type.AspectInterfaceTypeResolver;
import com.linkedin.datahub.graphql.resolvers.type.HyperParameterValueTypeResolver;
import com.linkedin.datahub.graphql.resolvers.type.ResultsTypeResolver;
import com.linkedin.datahub.graphql.resolvers.type.TimeSeriesAspectInterfaceTypeResolver;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.EntityType;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.aspect.AspectType;
import com.linkedin.datahub.graphql.types.chart.ChartType;
import com.linkedin.datahub.graphql.types.corpuser.CorpUserType;
import com.linkedin.datahub.graphql.types.corpgroup.CorpGroupType;
import com.linkedin.datahub.graphql.types.dashboard.DashboardType;
import com.linkedin.datahub.graphql.types.dataplatform.DataPlatformType;
import com.linkedin.datahub.graphql.types.dataset.DatasetType;
import com.linkedin.datahub.graphql.resolvers.AuthenticatedResolver;
import com.linkedin.datahub.graphql.resolvers.load.LoadableTypeResolver;
import com.linkedin.datahub.graphql.resolvers.load.OwnerTypeResolver;
import com.linkedin.datahub.graphql.resolvers.browse.BrowsePathsResolver;
import com.linkedin.datahub.graphql.resolvers.browse.BrowseResolver;
import com.linkedin.datahub.graphql.resolvers.search.AutoCompleteResolver;
import com.linkedin.datahub.graphql.resolvers.search.AutoCompleteForMultipleResolver;
import com.linkedin.datahub.graphql.resolvers.search.SearchResolver;
import com.linkedin.datahub.graphql.resolvers.type.EntityInterfaceTypeResolver;
import com.linkedin.datahub.graphql.resolvers.type.PlatformSchemaUnionTypeResolver;
import com.linkedin.datahub.graphql.types.dataset.mappers.DatasetProfileMapper;
import com.linkedin.datahub.graphql.types.lineage.DownstreamLineageType;
import com.linkedin.datahub.graphql.types.lineage.UpstreamLineageType;
import com.linkedin.datahub.graphql.types.mlmodel.MLFeatureTableType;
import com.linkedin.datahub.graphql.types.mlmodel.MLFeatureType;
import com.linkedin.datahub.graphql.types.mlmodel.MLPrimaryKeyType;
import com.linkedin.datahub.graphql.types.tag.TagType;
import com.linkedin.datahub.graphql.types.mlmodel.MLModelType;
import com.linkedin.datahub.graphql.types.mlmodel.MLModelGroupType;
import com.linkedin.datahub.graphql.types.dataflow.DataFlowType;
import com.linkedin.datahub.graphql.types.datajob.DataJobType;
import com.linkedin.datahub.graphql.types.lineage.DataFlowDataJobsRelationshipsType;
import com.linkedin.datahub.graphql.types.glossary.GlossaryTermType;

import com.linkedin.datahub.graphql.types.usage.UsageType;
import com.linkedin.metadata.entity.EntityService;
import graphql.execution.DataFetcherResult;
import graphql.schema.idl.RuntimeWiring;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.commons.io.IOUtils;
import org.dataloader.BatchLoaderContextProvider;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderOptions;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.datahub.graphql.Constants.*;
import static graphql.Scalars.GraphQLLong;

/**
 * A {@link GraphQLEngine} configured to provide access to the entities and aspects on the the GMS graph.
 *
 * TODO: Accept GMS clients as constructor arguments.
 */
public class GmsGraphQLEngine {

    private static final Logger _logger = LoggerFactory.getLogger(GmsGraphQLEngine.class.getName());

    private final AnalyticsService analyticsService;
    private final EntityService entityService;

    private final DatasetType datasetType;
    private final CorpUserType corpUserType;
    private final CorpGroupType corpGroupType;
    private final ChartType chartType;
    private final DashboardType dashboardType;
    private final DataPlatformType dataPlatformType;
    private final DownstreamLineageType downstreamLineageType;
    private final UpstreamLineageType upstreamLineageType;
    private final TagType tagType;
    private final MLModelType mlModelType;
    private final MLModelGroupType mlModelGroupType;
    private final MLFeatureType mlFeatureType;
    private final MLFeatureTableType mlFeatureTableType;
    private final MLPrimaryKeyType mlPrimaryKeyType;
    private final DataFlowType dataFlowType;
    private final DataJobType dataJobType;
    private final DataFlowDataJobsRelationshipsType dataFlowDataJobsRelationshipType;
    private final GlossaryTermType glossaryTermType;
    private final AspectType aspectType;
    private final UsageType usageType;

    /**
     * Configures the graph objects that can be fetched primary key.
     */
    public final List<EntityType<?>> entityTypes;

    /**
     * Configures the graph objects that cannot be fetched by primary key
     */
    public final List<LoadableType<?>> relationshipTypes;

    /**
     * Configures all graph objects
     */
    public final List<LoadableType<?>> loadableTypes;

    /**
     * Configures the graph objects for owner
     */
    public final List<LoadableType<?>> ownerTypes;

    /**
     * Configures the graph objects that can be searched.
     */
    public final List<SearchableEntityType<?>> searchableTypes;

    /**
     * Configures the graph objects that can be browsed.
     */
    public final List<BrowsableEntityType<?>> browsableTypes;

    public GmsGraphQLEngine() {
        this(null, null);
    }

    public GmsGraphQLEngine(final AnalyticsService analyticsService, final EntityService entityService) {
        this.analyticsService = analyticsService;
        this.entityService = entityService;

        this.datasetType = new DatasetType(GmsClientFactory.getEntitiesClient());
        this.corpUserType = new CorpUserType(GmsClientFactory.getEntitiesClient());
        this.corpGroupType = new CorpGroupType(GmsClientFactory.getEntitiesClient());
        this.chartType = new ChartType(GmsClientFactory.getEntitiesClient());
        this.dashboardType = new DashboardType(GmsClientFactory.getEntitiesClient());
        this.dataPlatformType = new DataPlatformType(GmsClientFactory.getEntitiesClient());
        this.downstreamLineageType = new DownstreamLineageType(
            GmsClientFactory.getLineagesClient()
        );
        this.upstreamLineageType = new UpstreamLineageType(
            GmsClientFactory.getLineagesClient()
        );
        this.tagType = new TagType(GmsClientFactory.getEntitiesClient());
        this.mlModelType = new MLModelType(GmsClientFactory.getEntitiesClient());
        this.mlModelGroupType = new MLModelGroupType(GmsClientFactory.getEntitiesClient());
        this.mlFeatureType = new MLFeatureType(GmsClientFactory.getEntitiesClient());
        this.mlFeatureTableType = new MLFeatureTableType(GmsClientFactory.getEntitiesClient());
        this.mlPrimaryKeyType = new MLPrimaryKeyType(GmsClientFactory.getEntitiesClient());
        this.dataFlowType = new DataFlowType(GmsClientFactory.getEntitiesClient());
        this.dataJobType = new DataJobType(GmsClientFactory.getEntitiesClient());
        this.dataFlowDataJobsRelationshipType = new DataFlowDataJobsRelationshipsType(
            GmsClientFactory.getRelationshipsClient()
        );
        this.glossaryTermType = new GlossaryTermType(GmsClientFactory.getEntitiesClient());
        this.aspectType = new AspectType(GmsClientFactory.getAspectsClient());
        this.usageType = new UsageType(GmsClientFactory.getUsageClient());

        // Init Lists
        this.entityTypes = ImmutableList.of(datasetType, corpUserType, corpGroupType,
            dataPlatformType, chartType, dashboardType, tagType, mlModelType, mlModelGroupType, mlFeatureType,
            mlFeatureTableType, mlPrimaryKeyType, dataFlowType, dataJobType, glossaryTermType
        );
        this.relationshipTypes = ImmutableList.of(downstreamLineageType, upstreamLineageType,
            dataFlowDataJobsRelationshipType
        );
        this.loadableTypes = Stream.concat(entityTypes.stream(), relationshipTypes.stream()).collect(Collectors.toList());
        this.ownerTypes = ImmutableList.of(corpUserType, corpGroupType);
        this.searchableTypes = loadableTypes.stream()
            .filter(type -> (type instanceof SearchableEntityType<?>))
            .map(type -> (SearchableEntityType<?>) type)
            .collect(Collectors.toList());
        this.browsableTypes = loadableTypes.stream()
            .filter(type -> (type instanceof BrowsableEntityType<?>))
            .map(type -> (BrowsableEntityType<?>) type)
            .collect(Collectors.toList());
    }

    public static String entitySchema() {
        String defaultSchemaString;
        try {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(GMS_SCHEMA_FILE);
            defaultSchemaString = IOUtils.toString(is, StandardCharsets.UTF_8);
            is.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find GraphQL Schema with name " + GMS_SCHEMA_FILE, e);
        }
        return defaultSchemaString;
    }

    public static String searchSchema() {
        String defaultSchemaString;
        try {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(SEARCH_SCHEMA_FILE);
            defaultSchemaString = IOUtils.toString(is, StandardCharsets.UTF_8);
            is.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find GraphQL Schema with name " + SEARCH_SCHEMA_FILE, e);
        }
        return defaultSchemaString;
    }

    public static String appSchema() {
        String defaultSchemaString;
        try {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(APP_SCHEMA_FILE);
            defaultSchemaString = IOUtils.toString(is, StandardCharsets.UTF_8);
            is.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find GraphQL Schema with name " + APP_SCHEMA_FILE, e);
        }
        return defaultSchemaString;
    }

    public static String analyticsSchema() {
        String analyticsSchemaString;
        try {
            InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(ANALYTICS_SCHEMA_FILE);
            analyticsSchemaString = IOUtils.toString(is, StandardCharsets.UTF_8);
            is.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to find GraphQL Schema with name " + ANALYTICS_SCHEMA_FILE, e);
        }
        return analyticsSchemaString;
    }

    /**
     * Returns a {@link Supplier} responsible for creating a new {@link DataLoader} from
     * a {@link LoadableType}.
     */
    public Map<String, Function<QueryContext, DataLoader<?, ?>>> loaderSuppliers(final List<LoadableType<?>> loadableTypes) {
        return loadableTypes
            .stream()
            .collect(Collectors.toMap(
                    LoadableType::name,
                    (graphType) -> (context) -> createDataLoader(graphType, context)
            ));
    }

    public void configureRuntimeWiring(final RuntimeWiring.Builder builder) {
        configureQueryResolvers(builder);
        configureMutationResolvers(builder);
        configureGenericEntityResolvers(builder);
        configureDatasetResolvers(builder);
        configureCorpUserResolvers(builder);
        configureCorpGroupResolvers(builder);
        configureDashboardResolvers(builder);
        configureChartResolvers(builder);
        configureTypeResolvers(builder);
        configureTypeExtensions(builder);
        configureTagAssociationResolver(builder);
        configureDataJobResolvers(builder);
        configureDataFlowResolvers(builder);
        configureMLFeatureTableResolvers(builder);
        configureGlossaryRelationshipResolvers(builder);
        configureAnalyticsResolvers(builder);
    }

    public GraphQLEngine.Builder builder() {
        return GraphQLEngine.builder()
            .addSchema(entitySchema())
            .addSchema(searchSchema())
            .addSchema(appSchema())
            .addSchema(analyticsSchema())
            .addDataLoaders(loaderSuppliers(loadableTypes))
            .addDataLoader("Aspect", (context) -> createAspectLoader(context))
            .addDataLoader("UsageQueryResult", (context) -> createUsageLoader(context))
            .configureRuntimeWiring(this::configureRuntimeWiring);
    }

    private void configureAnalyticsResolvers(final RuntimeWiring.Builder builder) {
        final boolean isAnalyticsEnabled = analyticsService != null;
        builder.type("Query", typeWiring -> typeWiring.dataFetcher("isAnalyticsEnabled", new IsAnalyticsEnabledResolver(isAnalyticsEnabled)))
            .type("AnalyticsChart", typeWiring -> typeWiring.typeResolver(new AnalyticsChartTypeResolver()));
        if (isAnalyticsEnabled) {
            builder.type("Query",
                typeWiring -> typeWiring.dataFetcher("getAnalyticsCharts", new GetChartsResolver(analyticsService))
                    .dataFetcher("getHighlights", new GetHighlightsResolver(analyticsService)));
        }
    }

    private void configureQueryResolvers(final RuntimeWiring.Builder builder) {
        builder.type("Query", typeWiring -> typeWiring
            .dataFetcher("appConfig",
                new AppConfigResolver(analyticsService != null))
            .dataFetcher("me", new AuthenticatedResolver<>(
                    new MeResolver(GmsClientFactory.getEntitiesClient())))
            .dataFetcher("search", new AuthenticatedResolver<>(
                    new SearchResolver(searchableTypes)))
            .dataFetcher("autoComplete", new AuthenticatedResolver<>(
                    new AutoCompleteResolver(searchableTypes)))
            .dataFetcher("autoCompleteForMultiple", new AuthenticatedResolver<>(
                    new AutoCompleteForMultipleResolver(searchableTypes)))
            .dataFetcher("browse", new AuthenticatedResolver<>(
                    new BrowseResolver(browsableTypes)))
            .dataFetcher("browsePaths", new AuthenticatedResolver<>(
                    new BrowsePathsResolver(browsableTypes)))
            .dataFetcher("dataset", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(datasetType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("corpUser", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(corpUserType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("corpGroup", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(corpGroupType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("dashboard", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(dashboardType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("chart", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(chartType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("tag", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(tagType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("dataFlow", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(dataFlowType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("dataJob", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(dataJobType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("glossaryTerm", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(glossaryTermType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("mlFeatureTable", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(mlFeatureTableType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("mlFeature", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(mlFeatureType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("mlPrimaryKey", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(mlPrimaryKeyType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("mlModel", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(mlModelType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("mlModelGroup", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(mlModelGroupType,
                            (env) -> env.getArgument(URN_FIELD_NAME))))
            .dataFetcher("listPolicies",
                new ListPoliciesResolver(GmsClientFactory.getEntitiesClient()))
        );
    }

    private void configureMutationResolvers(final RuntimeWiring.Builder builder) {
        builder.type("Mutation", typeWiring -> typeWiring
            .dataFetcher("updateDataset", new AuthenticatedResolver<>(new MutableTypeResolver<>(datasetType)))
            .dataFetcher("updateTag", new AuthenticatedResolver<>(new MutableTypeResolver<>(tagType)))
            .dataFetcher("updateChart", new AuthenticatedResolver<>(new MutableTypeResolver<>(chartType)))
            .dataFetcher("updateDashboard", new AuthenticatedResolver<>(new MutableTypeResolver<>(dashboardType)))
            .dataFetcher("updateDataJob", new AuthenticatedResolver<>(new MutableTypeResolver<>(dataJobType)))
            .dataFetcher("updateDataFlow", new AuthenticatedResolver<>(new MutableTypeResolver<>(dataFlowType)))
            .dataFetcher("addTag", new AuthenticatedResolver<>(new AddTagResolver(entityService)))
            .dataFetcher("removeTag", new AuthenticatedResolver<>(new RemoveTagResolver(entityService)))
            .dataFetcher("addTerm", new AuthenticatedResolver<>(new AddTermResolver(entityService)))
            .dataFetcher("removeTerm", new AuthenticatedResolver<>(new RemoveTermResolver(entityService)))
            .dataFetcher("createPolicy", new UpsertPolicyResolver(GmsClientFactory.getAspectsClient()))
            .dataFetcher("updatePolicy", new UpsertPolicyResolver(GmsClientFactory.getAspectsClient()))
            .dataFetcher("deletePolicy", new DeletePolicyResolver(GmsClientFactory.getEntitiesClient()))
            .dataFetcher("updateDescription", new AuthenticatedResolver<>(new UpdateFieldDescriptionResolver(entityService)))
            .dataFetcher("addOwner", new AddOwnerResolver(entityService))
            .dataFetcher("removeOwner", new RemoveOwnerResolver(entityService))
            .dataFetcher("addLink", new AddLinkResolver(entityService))
            .dataFetcher("removeLink", new RemoveLinkResolver(entityService))
        );
    }

    private void configureGenericEntityResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("SearchResult", typeWiring -> typeWiring
                .dataFetcher("entity", new AuthenticatedResolver<>(
                    new EntityTypeResolver(
                        entityTypes.stream().collect(Collectors.toList()),
                        (env) -> ((SearchResult) env.getSource()).getEntity()))
                )
            )
            .type("BrowseResults", typeWiring -> typeWiring
                .dataFetcher("entities", new AuthenticatedResolver<>(
                    new EntityTypeBatchResolver(
                        entityTypes.stream().collect(Collectors.toList()),
                        (env) -> ((BrowseResults) env.getSource()).getEntities()))
                )
            )
            .type("EntityRelationshipLegacy", typeWiring -> typeWiring
                .dataFetcher("entity", new AuthenticatedResolver<>(
                    new EntityTypeResolver(
                        new ArrayList<>(entityTypes),
                        (env) -> ((EntityRelationshipLegacy) env.getSource()).getEntity()))
                )
            )
            .type("EntityRelationship", typeWiring -> typeWiring
                .dataFetcher("entity", new AuthenticatedResolver<>(
                    new EntityTypeResolver(
                        new ArrayList<>(entityTypes),
                        (env) -> ((EntityRelationship) env.getSource()).getEntity()))
                )
            );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.Dataset} type.
     */
    private void configureDatasetResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("Dataset", typeWiring -> typeWiring
                .dataFetcher("platform", new AuthenticatedResolver<>(
                        new LoadableTypeResolver<>(dataPlatformType,
                                (env) -> ((Dataset) env.getSource()).getPlatform().getUrn()))
                )
                .dataFetcher("downstreamLineage", new AuthenticatedResolver<>(
                        new LoadableTypeResolver<>(downstreamLineageType,
                                (env) -> ((Entity) env.getSource()).getUrn()))
                )
                .dataFetcher("upstreamLineage", new AuthenticatedResolver<>(
                        new LoadableTypeResolver<>(upstreamLineageType,
                                (env) -> ((Entity) env.getSource()).getUrn()))
                )
                .dataFetcher("datasetProfiles", new AuthenticatedResolver<>(
                    new TimeSeriesAspectResolver(
                        GmsClientFactory.getAspectsClient(),
                        "dataset",
                        "datasetProfile",
                        DatasetProfileMapper::map
                    )
                ))
                .dataFetcher("usageStats", new AuthenticatedResolver<>(new UsageTypeResolver()))
                .dataFetcher("schemaMetadata", new AuthenticatedResolver<>(
                    new AspectResolver())
                )
            )
            .type("Owner", typeWiring -> typeWiring
                    .dataFetcher("owner", new AuthenticatedResolver<>(
                            new OwnerTypeResolver<>(ownerTypes,
                                    (env) -> ((Owner) env.getSource()).getOwner()))
                    )
            )
            .type("UserUsageCounts", typeWiring -> typeWiring
                .dataFetcher("user", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(corpUserType,
                        (env) -> ((UserUsageCounts) env.getSource()).getUser().getUrn()))
                )
            )
            .type("RelatedDataset", typeWiring -> typeWiring
                    .dataFetcher("dataset", new AuthenticatedResolver<>(
                            new LoadableTypeResolver<>(datasetType,
                                    (env) -> ((RelatedDataset) env.getSource()).getDataset().getUrn()))
                    )
            )
            .type("ForeignKeyConstraint", typeWiring -> typeWiring
                .dataFetcher("foreignDataset", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(datasetType,
                        (env) -> ((ForeignKeyConstraint) env.getSource()).getForeignDataset().getUrn()))
                )
            )
            .type("InstitutionalMemoryMetadata", typeWiring -> typeWiring
                .dataFetcher("author", new AuthenticatedResolver<>(
                        new LoadableTypeResolver<>(corpUserType,
                                (env) -> ((InstitutionalMemoryMetadata) env.getSource()).getAuthor().getUrn()))
                )
            );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.CorpUser} type.
     */
    private void configureCorpUserResolvers(final RuntimeWiring.Builder builder) {
        builder.type("CorpUser", typeWiring -> typeWiring
            .dataFetcher("relationships", new AuthenticatedResolver<>(
                new EntityRelationshipsResultResolver(GmsClientFactory.getRelationshipsClient())
            ))
        );
        builder.type("CorpUserInfo", typeWiring -> typeWiring
            .dataFetcher("manager", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(corpUserType,
                            (env) -> ((CorpUserInfo) env.getSource()).getManager().getUrn()))
            )
        );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.CorpGroup} type.
     */
    private void configureCorpGroupResolvers(final RuntimeWiring.Builder builder) {
        builder.type("CorpGroup", typeWiring -> typeWiring
            .dataFetcher("relationships", new AuthenticatedResolver<>(
                new EntityRelationshipsResultResolver(GmsClientFactory.getRelationshipsClient())
            ))
        );
        builder.type("CorpGroupInfo", typeWiring -> typeWiring
            .dataFetcher("admins", new AuthenticatedResolver<>(
                    new LoadableTypeBatchResolver<>(corpUserType,
                            (env) -> ((CorpGroupInfo) env.getSource()).getAdmins().stream()
                                    .map(CorpUser::getUrn)
                                    .collect(Collectors.toList())))
            )
            .dataFetcher("members", new AuthenticatedResolver<>(
                    new LoadableTypeBatchResolver<>(corpUserType,
                            (env) -> ((CorpGroupInfo) env.getSource()).getMembers().stream()
                                    .map(CorpUser::getUrn)
                                    .collect(Collectors.toList())))
            )
        );
    }

    private void configureTagAssociationResolver(final RuntimeWiring.Builder builder) {
        builder.type("Tag", typeWiring -> typeWiring
            .dataFetcher("relationships", new AuthenticatedResolver<>(
                new EntityRelationshipsResultResolver(GmsClientFactory.getRelationshipsClient())
            ))
        );
        builder.type("TagAssociation", typeWiring -> typeWiring
            .dataFetcher("tag", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(tagType,
                            (env) -> ((com.linkedin.datahub.graphql.generated.TagAssociation) env.getSource()).getTag().getUrn()))
            )
        );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.Dashboard} type.
     */
    private void configureDashboardResolvers(final RuntimeWiring.Builder builder) {
        builder.type("Dashboard", typeWiring -> typeWiring
            .dataFetcher("relationships", new AuthenticatedResolver<>(
                new EntityRelationshipsResultResolver(GmsClientFactory.getRelationshipsClient())
            ))
            .dataFetcher("downstreamLineage", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(downstreamLineageType,
                            (env) -> ((Entity) env.getSource()).getUrn()))
            )
            .dataFetcher("upstreamLineage", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(upstreamLineageType,
                            (env) -> ((Entity) env.getSource()).getUrn()))
            )
        );
        builder.type("DashboardInfo", typeWiring -> typeWiring
            .dataFetcher("charts", new AuthenticatedResolver<>(
                new LoadableTypeBatchResolver<>(chartType,
                    (env) -> ((DashboardInfo) env.getSource()).getCharts().stream()
                        .map(Chart::getUrn)
                        .collect(Collectors.toList())))
            )
        );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.Chart} type.
     */
    private void configureChartResolvers(final RuntimeWiring.Builder builder) {
        builder.type("Chart", typeWiring -> typeWiring
            .dataFetcher("relationships", new AuthenticatedResolver<>(
                new EntityRelationshipsResultResolver(GmsClientFactory.getRelationshipsClient())
            ))
            .dataFetcher("downstreamLineage", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(downstreamLineageType,
                            (env) -> ((Entity) env.getSource()).getUrn()))
            )
            .dataFetcher("upstreamLineage", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(upstreamLineageType,
                            (env) -> ((Entity) env.getSource()).getUrn()))
            )
        );
        builder.type("ChartInfo", typeWiring -> typeWiring
            .dataFetcher("inputs", new AuthenticatedResolver<>(
                    new LoadableTypeBatchResolver<>(datasetType,
                            (env) -> ((ChartInfo) env.getSource()).getInputs().stream()
                                    .map(Dataset::getUrn)
                                    .collect(Collectors.toList())))
            )
        );
    }

    /**
     * Configures {@link graphql.schema.TypeResolver}s for any GQL 'union' or 'interface' types.
     */
    private void configureTypeResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("Entity", typeWiring -> typeWiring
                .typeResolver(new EntityInterfaceTypeResolver(loadableTypes.stream()
                        .filter(graphType -> graphType instanceof EntityType)
                        .map(graphType -> (EntityType<?>) graphType)
                        .collect(Collectors.toList())
                )))
            .type("EntityWithRelationships", typeWiring -> typeWiring
                .typeResolver(new EntityInterfaceTypeResolver(loadableTypes.stream()
                        .filter(graphType -> graphType instanceof EntityType)
                        .map(graphType -> (EntityType<?>) graphType)
                        .collect(Collectors.toList())
                )))
            .type("OwnerType", typeWiring -> typeWiring
                .typeResolver(new EntityInterfaceTypeResolver(ownerTypes.stream()
                    .filter(graphType -> graphType instanceof EntityType)
                    .map(graphType -> (EntityType<?>) graphType)
                    .collect(Collectors.toList())
                )))
            .type("PlatformSchema", typeWiring -> typeWiring
                    .typeResolver(new PlatformSchemaUnionTypeResolver())
            )
            .type("HyperParameterValueType", typeWiring -> typeWiring
                    .typeResolver(new HyperParameterValueTypeResolver())
            )
            .type("Aspect", typeWiring -> typeWiring.typeResolver(new AspectInterfaceTypeResolver()))
            .type("TimeSeriesAspect", typeWiring -> typeWiring.typeResolver(new TimeSeriesAspectInterfaceTypeResolver()))
            .type("ResultsType", typeWiring -> typeWiring
                    .typeResolver(new ResultsTypeResolver()));
    }

    /**
     * Configures custom type extensions leveraged within our GraphQL schema.
     */
    private void configureTypeExtensions(final RuntimeWiring.Builder builder) {
        builder.scalar(GraphQLLong);
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.DataJob} type.
     */
    private void configureDataJobResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("DataJob", typeWiring -> typeWiring
                .dataFetcher("relationships", new AuthenticatedResolver<>(
                    new EntityRelationshipsResultResolver(GmsClientFactory.getRelationshipsClient())
                ))
                .dataFetcher("dataFlow", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(dataFlowType,
                        (env) -> ((DataJob) env.getSource()).getDataFlow().getUrn()))
                )
                .dataFetcher("downstreamLineage", new AuthenticatedResolver<>(
                        new LoadableTypeResolver<>(downstreamLineageType,
                                (env) -> ((Entity) env.getSource()).getUrn()))
                )
                .dataFetcher("upstreamLineage", new AuthenticatedResolver<>(
                        new LoadableTypeResolver<>(upstreamLineageType,
                                (env) -> ((Entity) env.getSource()).getUrn()))
                )
            )
            .type("DataJobInputOutput", typeWiring -> typeWiring
                .dataFetcher("inputDatasets", new AuthenticatedResolver<>(
                    new LoadableTypeBatchResolver<>(datasetType,
                        (env) -> ((DataJobInputOutput) env.getSource()).getInputDatasets().stream()
                                .map(Dataset::getUrn)
                                .collect(Collectors.toList())))
                )
                .dataFetcher("outputDatasets", new AuthenticatedResolver<>(
                    new LoadableTypeBatchResolver<>(datasetType,
                        (env) -> ((DataJobInputOutput) env.getSource()).getOutputDatasets().stream()
                                .map(Dataset::getUrn)
                                .collect(Collectors.toList())))
                )
                .dataFetcher("inputDatajobs", new AuthenticatedResolver<>(
                    new LoadableTypeBatchResolver<>(dataJobType,
                        (env) -> ((DataJobInputOutput) env.getSource()).getInputDatajobs().stream()
                            .map(DataJob::getUrn)
                            .collect(Collectors.toList())))
                )
            );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.DataFlow} type.
     */
    private void configureDataFlowResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("DataFlow", typeWiring -> typeWiring
                .dataFetcher("dataJobs", new AuthenticatedResolver<>(
                        new LoadableTypeResolver<>(dataFlowDataJobsRelationshipType,
                            (env) -> ((Entity) env.getSource()).getUrn())
                    )
                )
                .dataFetcher("relationships", new AuthenticatedResolver<>(
                    new EntityRelationshipsResultResolver(GmsClientFactory.getRelationshipsClient())
                ))
            );
    }

    /**
     * Configures resolvers responsible for resolving the {@link com.linkedin.datahub.graphql.generated.MLFeatureTable} type.
     */
    private void configureMLFeatureTableResolvers(final RuntimeWiring.Builder builder) {
        builder
            .type("MLFeatureTable", typeWiring -> typeWiring
                .dataFetcher("relationships", new AuthenticatedResolver<>(
                    new EntityRelationshipsResultResolver(GmsClientFactory.getRelationshipsClient())
                ))
                .dataFetcher("platform", new AuthenticatedResolver<>(
                        new LoadableTypeResolver<>(dataPlatformType,
                                (env) -> ((MLFeatureTable) env.getSource()).getPlatform().getUrn()))
                )
            )
            .type("MLFeatureTableProperties", typeWiring -> typeWiring
                .dataFetcher("mlFeatures", new AuthenticatedResolver<>(
                                new LoadableTypeBatchResolver<>(mlFeatureType,
                                        (env) -> ((MLFeatureTableProperties) env.getSource()).getMlFeatures().stream()
                                        .map(MLFeature::getUrn)
                                        .collect(Collectors.toList())))
                )
                .dataFetcher("mlPrimaryKeys", new AuthenticatedResolver<>(
                                new LoadableTypeBatchResolver<>(mlPrimaryKeyType,
                                        (env) -> ((MLFeatureTableProperties) env.getSource()).getMlPrimaryKeys().stream()
                                        .map(MLPrimaryKey::getUrn)
                                        .collect(Collectors.toList())))
                )
            )
            .type("MLFeatureProperties", typeWiring -> typeWiring
                .dataFetcher("sources", new AuthenticatedResolver<>(
                        new LoadableTypeBatchResolver<>(datasetType,
                                (env) -> ((MLFeatureProperties) env.getSource()).getSources().stream()
                                        .map(Dataset::getUrn)
                                        .collect(Collectors.toList())))
                )
            )
            .type("MLPrimaryKeyProperties", typeWiring -> typeWiring
                .dataFetcher("sources", new AuthenticatedResolver<>(
                        new LoadableTypeBatchResolver<>(datasetType,
                                (env) -> ((MLPrimaryKeyProperties) env.getSource()).getSources().stream()
                                        .map(Dataset::getUrn)
                                        .collect(Collectors.toList())))
                )
            )
            .type("MLModel", typeWiring -> typeWiring
                .dataFetcher("relationships", new AuthenticatedResolver<>(
                    new EntityRelationshipsResultResolver(GmsClientFactory.getRelationshipsClient())
                ))
                .dataFetcher("platform", new AuthenticatedResolver<>(
                        new LoadableTypeResolver<>(dataPlatformType,
                                (env) -> ((MLModel) env.getSource()).getPlatform().getUrn()))
                )
                .dataFetcher("downstreamLineage", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(downstreamLineageType,
                        (env) -> ((Entity) env.getSource()).getUrn()))
                )
                .dataFetcher("upstreamLineage", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(upstreamLineageType,
                        (env) -> ((Entity) env.getSource()).getUrn()))
                )
            )
            .type("MLModelProperties", typeWiring -> typeWiring
                .dataFetcher("groups", new AuthenticatedResolver<>(
                    new LoadableTypeBatchResolver<>(mlModelGroupType,
                        (env) -> {
                            MLModelProperties properties = env.getSource();
                            if (properties.getGroups() != null) {
                                return properties.getGroups().stream()
                                    .map(MLModelGroup::getUrn)
                                    .collect(Collectors.toList());
                            }
                            return Collections.emptyList();
                        }))
                )
            )
            .type("MLModelGroup", typeWiring -> typeWiring
                .dataFetcher("relationships", new AuthenticatedResolver<>(
                    new EntityRelationshipsResultResolver(GmsClientFactory.getRelationshipsClient())
                ))
                .dataFetcher("platform", new AuthenticatedResolver<>(
                        new LoadableTypeResolver<>(dataPlatformType,
                                (env) -> ((MLModelGroup) env.getSource()).getPlatform().getUrn()))
                )
                .dataFetcher("downstreamLineage", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(downstreamLineageType,
                        (env) -> ((Entity) env.getSource()).getUrn()))
                )
                .dataFetcher("upstreamLineage", new AuthenticatedResolver<>(
                    new LoadableTypeResolver<>(upstreamLineageType,
                        (env) -> ((Entity) env.getSource()).getUrn()))
                )
            );
    }

    private static void configureGlossaryRelationshipResolvers(final RuntimeWiring.Builder builder) {
        builder.type("GlossaryTerm", typeWiring -> typeWiring
            .dataFetcher("relationships", new AuthenticatedResolver<>(
                new EntityRelationshipsResultResolver(GmsClientFactory.getRelationshipsClient())
            ))
        );
    }


    private <T> DataLoader<String, DataFetcherResult<T>> createDataLoader(final LoadableType<T> graphType, final QueryContext queryContext) {
        BatchLoaderContextProvider contextProvider = () -> queryContext;
        DataLoaderOptions loaderOptions = DataLoaderOptions.newOptions().setBatchLoaderContextProvider(contextProvider);
        return DataLoader.newDataLoader((keys, context) -> CompletableFuture.supplyAsync(() -> {
            try {
                _logger.debug(String.format("Batch loading entities of type: %s, keys: %s", graphType.name(), keys));
                return graphType.batchLoad(keys, context.getContext());
            } catch (Exception e) {
                _logger.error(String.format("Failed to load Entities of type: %s, keys: %s", graphType.name(), keys) + " " + e.getMessage());
                throw new RuntimeException(String.format("Failed to retrieve entities of type %s", graphType.name()), e);
            }
        }), loaderOptions);
    }

    private DataLoader<VersionedAspectKey, DataFetcherResult<Aspect>> createAspectLoader(final QueryContext queryContext) {
        BatchLoaderContextProvider contextProvider = () -> queryContext;
        DataLoaderOptions loaderOptions = DataLoaderOptions.newOptions().setBatchLoaderContextProvider(contextProvider);
        return DataLoader.newDataLoader((keys, context) -> CompletableFuture.supplyAsync(() -> {
            try {
                _logger.debug(String.format("Batch loading aspects with keys: %s", keys));
                return aspectType.batchLoad(keys, context.getContext());
            } catch (Exception e) {
                _logger.error(String.format("Failed to load Aspect for entity. keys: %s", keys) + " " + e.getMessage());
                throw new RuntimeException(String.format("Failed to retrieve entities of type Aspect", e));
            }
        }), loaderOptions);
    }

    private DataLoader<UsageStatsKey, DataFetcherResult<UsageQueryResult>> createUsageLoader(final QueryContext queryContext) {
        BatchLoaderContextProvider contextProvider = () -> queryContext;
        DataLoaderOptions loaderOptions = DataLoaderOptions.newOptions().setBatchLoaderContextProvider(contextProvider);
        return DataLoader.newDataLoader((keys, context) -> CompletableFuture.supplyAsync(() -> {
            try {
                return usageType.batchLoad(keys, context.getContext());
            } catch (Exception e) {
                throw new RuntimeException(String.format("Failed to retrieve usage stats", e));
            }
        }), loaderOptions);
    }
}
