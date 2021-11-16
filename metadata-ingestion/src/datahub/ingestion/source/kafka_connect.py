import logging
import re
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

import jpype
import jpype.imports
import requests
from sqlalchemy.engine.url import make_url

import datahub.emitter.mce_builder as builder
import datahub.metadata.schema_classes as models
from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit

logger = logging.getLogger(__name__)


class KafkaConnectSourceConfig(ConfigModel):
    # See the Connect REST Interface for details
    # https://docs.confluent.io/platform/current/connect/references/restapi.html#
    connect_uri: str = "http://localhost:8083/"
    username: Optional[str] = None
    password: Optional[str] = None
    cluster_name: Optional[str] = "connect-cluster"
    env: str = builder.DEFAULT_ENV
    construct_lineage_workunits: bool = True
    connector_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()


@dataclass
class KafkaConnectSourceReport(SourceReport):
    connectors_scanned: int = 0
    filtered: List[str] = field(default_factory=list)

    def report_connector_scanned(self, connector: str) -> None:
        self.connectors_scanned += 1

    def report_dropped(self, connector: str) -> None:
        self.filtered.append(connector)


@dataclass
class KafkaConnectLineage:
    """Class to store Kafka Connect lineage mapping, Each instance is potential DataJob"""

    source_platform: str
    target_dataset: str
    target_platform: str
    job_property_bag: Optional[Dict[str, str]] = None
    source_dataset: Optional[str] = None


@dataclass
class ConnectorManifest:
    """Each instance is potential DataFlow"""

    name: str
    type: str
    config: Dict
    tasks: Dict
    url: Optional[str] = None
    flow_property_bag: Optional[Dict[str, str]] = None
    lineages: List[KafkaConnectLineage] = field(default_factory=list)
    topic_names: Iterable[str] = field(default_factory=list)


def remove_prefix(text: str, prefix: str) -> str:
    if text.startswith(prefix):
        index = len(prefix)
        return text[index:]
    return text


def unquote(string: str, leading_quote: str = '"', trailing_quote: str = None) -> str:
    """
    If string starts and ends with a quote, unquote it
    """
    trailing_quote = trailing_quote if trailing_quote else leading_quote
    if string.startswith(leading_quote) and string.endswith(trailing_quote):
        string = string[1:-1]
    return string


@dataclass
class ConfluentJDBCSourceConnector:
    connector_manifest: ConnectorManifest
    report: KafkaConnectSourceReport

    def __init__(
        self, connector_manifest: ConnectorManifest, report: KafkaConnectSourceReport
    ) -> None:
        self.connector_manifest = connector_manifest
        self.report = report
        self._extract_lineages()

    REGEXROUTER = "org.apache.kafka.connect.transforms.RegexRouter"
    KNOWN_TOPICROUTING_TRANSFORMS = [REGEXROUTER]
    # https://kafka.apache.org/documentation/#connect_included_transformation
    KAFKA_NONTOPICROUTING_TRANSFORMS = [
        "InsertField",
        "InsertField$Key",
        "InsertField$Value",
        "ReplaceField",
        "ReplaceField$Key",
        "ReplaceField$Value",
        "MaskField",
        "MaskField$Key",
        "MaskField$Value",
        "ValueToKey",
        "ValueToKey$Key",
        "ValueToKey$Value",
        "HoistField",
        "HoistField$Key",
        "HoistField$Value",
        "ExtractField",
        "ExtractField$Key",
        "ExtractField$Value",
        "SetSchemaMetadata",
        "SetSchemaMetadata$Key",
        "SetSchemaMetadata$Value",
        "Flatten",
        "Flatten$Key",
        "Flatten$Value",
        "Cast",
        "Cast$Key",
        "Cast$Value",
        "HeadersFrom",
        "HeadersFrom$Key",
        "HeadersFrom$Value",
        "TimestampConverter",
        "Filter",
        "InsertHeader",
        "DropHeaders",
    ]
    # https://docs.confluent.io/platform/current/connect/transforms/overview.html
    CONFLUENT_NONTOPICROUTING_TRANSFORMS = [
        "Drop",
        "Drop$Key",
        "Drop$Value",
        "Filter",
        "Filter$Key",
        "Filter$Value",
        "TombstoneHandler",
    ]
    KNOWN_NONTOPICROUTING_TRANSFORMS = (
        KAFKA_NONTOPICROUTING_TRANSFORMS
        + [
            "org.apache.kafka.connect.transforms.{}".format(t)
            for t in KAFKA_NONTOPICROUTING_TRANSFORMS
        ]
        + CONFLUENT_NONTOPICROUTING_TRANSFORMS
        + [
            "io.confluent.connect.transforms.{}".format(t)
            for t in CONFLUENT_NONTOPICROUTING_TRANSFORMS
        ]
    )

    @dataclass
    class JdbcParser:
        db_connection_url: str
        source_platform: str
        database_name: str
        topic_prefix: str
        query: str
        transforms: list

    def report_warning(self, key: str, reason: str) -> None:
        logger.warning(f"{key}: {reason}")
        self.report.report_warning(key, reason)

    def get_parser(
        self,
        connector_manifest: ConnectorManifest,
    ) -> JdbcParser:

        url = remove_prefix(
            str(connector_manifest.config.get("connection.url")), "jdbc:"
        )
        url_instance = make_url(url)
        source_platform = url_instance.drivername
        database_name = url_instance.database
        db_connection_url = f"{url_instance.drivername}://{url_instance.host}:{url_instance.port}/{url_instance.database}"

        topic_prefix = self.connector_manifest.config.get("topic.prefix", None)

        query = self.connector_manifest.config.get("query", None)

        transform_names = (
            self.connector_manifest.config.get("transforms", "").split(",")
            if self.connector_manifest.config.get("transforms")
            else []
        )

        transforms = []
        for name in transform_names:
            transform = {"name": name}
            transforms.append(transform)
            for key in self.connector_manifest.config.keys():
                if key.startswith("transforms.{}.".format(name)):
                    transform[
                        key.replace("transforms.{}.".format(name), "")
                    ] = self.connector_manifest.config[key]

        return self.JdbcParser(
            db_connection_url,
            source_platform,
            database_name,
            topic_prefix,
            query,
            transforms,
        )

    def default_get_lineages(
        self,
        topic_prefix,
        database_name,
        source_platform,
        topic_names=None,
        include_source_dataset=True,
    ):
        lineages: List[KafkaConnectLineage] = list()
        if not topic_names:
            topic_names = self.connector_manifest.topic_names
        for topic in topic_names:
            # All good for NO_TRANSFORM or (SINGLE_TRANSFORM and KNOWN_NONTOPICROUTING_TRANSFORM) or (not SINGLE_TRANSFORM and all(KNOWN_NONTOPICROUTING_TRANSFORM))
            # default method - as per earlier implementation
            if topic_prefix:
                source_table = remove_prefix(topic, topic_prefix)
            else:
                source_table = topic
            dataset_name = (
                database_name + "." + source_table if database_name else source_table
            )
            lineage = KafkaConnectLineage(
                source_dataset=dataset_name if include_source_dataset else None,
                source_platform=source_platform,
                target_dataset=topic,
                target_platform="kafka",
            )
            lineages.append(lineage)
        return lineages

    def get_table_names(self):
        if self.connector_manifest.config.get("table.whitelist"):
            return self.connector_manifest.config.get("table.whitelist").split(",")  # type: ignore

        if self.connector_manifest.tasks:
            sep = "."
            leading_quote_char = trailing_quote_char = '"'
            quote_method = self.connector_manifest.config.get(
                "quote.sql.identifiers", "always"
            )

            tableIds = ",".join(
                [task["config"].get("tables") for task in self.connector_manifest.tasks]
            )
            if quote_method == "always":
                leading_quote_char = tableIds[0]
                trailing_quote_char = tableIds[-1]
                # This will only work for single character quotes

            tables = [
                unquote(tableId.split(sep)[-1], leading_quote_char, trailing_quote_char)
                for tableId in tableIds.split(",")
            ]
            return tables

        return []

    def _extract_lineages(self):
        lineages: List[KafkaConnectLineage] = list()
        parser = self.get_parser(self.connector_manifest)
        source_platform = parser.source_platform
        database_name = parser.database_name
        query = parser.query
        topic_prefix = parser.topic_prefix
        transforms = parser.transforms
        self.connector_manifest.flow_property_bag = self.connector_manifest.config

        # Mask/Remove properties that may reveal credentials
        self.connector_manifest.flow_property_bag[
            "connection.url"
        ] = parser.db_connection_url
        if "connection.password" in self.connector_manifest.flow_property_bag:
            del self.connector_manifest.flow_property_bag["connection.password"]
        if "connection.user" in self.connector_manifest.flow_property_bag:
            del self.connector_manifest.flow_property_bag["connection.user"]

        logging.debug(
            f"Extracting source platform: {source_platform} and database name: {database_name} from connection url "
        )

        if not self.connector_manifest.topic_names:
            self.connector_manifest.lineages = lineages
            return

        if query:
            # Lineage source_table can be extracted by parsing query
            # For now, we use source table as topic (expected to be same as topic prefix)
            for topic in self.connector_manifest.topic_names:
                # default method - as per earlier implementation
                source_table = topic
                dataset_name = (
                    database_name + "." + source_table
                    if database_name
                    else source_table
                )
                lineage = KafkaConnectLineage(
                    source_platform=source_platform,
                    target_dataset=topic,
                    target_platform="kafka",
                )
                lineages.append(lineage)
                self.report_warning(
                    self.connector_manifest.name,
                    "could not find input dataset, the connector has query configuration set",
                )
                self.connector_manifest.lineages = lineages
                return

        SINGLE_TRANSFORM = len(transforms) == 1
        NO_TRANSFORM = len(transforms) == 0
        UNKNOWN_TRANSFORM = any(
            [
                transform["type"]
                not in self.KNOWN_TOPICROUTING_TRANSFORMS
                + self.KNOWN_NONTOPICROUTING_TRANSFORMS
                for transform in transforms
            ]
        )
        ALL_TRANSFORMS_NON_TOPICROUTING = all(
            [
                transform["type"] in self.KNOWN_NONTOPICROUTING_TRANSFORMS
                for transform in transforms
            ]
        )

        if NO_TRANSFORM or ALL_TRANSFORMS_NON_TOPICROUTING:
            self.connector_manifest.lineages = self.default_get_lineages(
                database_name=database_name,
                source_platform=source_platform,
                topic_prefix=topic_prefix,
            )
            return

        if SINGLE_TRANSFORM and transforms[0]["type"] == self.REGEXROUTER:

            tables = self.get_table_names()
            topic_names = list(self.connector_manifest.topic_names)

            from java.util.regex import Pattern

            for source_table in tables:
                topic = topic_prefix + source_table if topic_prefix else source_table

                transform_regex = Pattern.compile(transforms[0]["regex"])
                transform_replacement = transforms[0]["replacement"]

                matcher = transform_regex.matcher(topic)
                if matcher.matches():
                    topic = matcher.replaceFirst(transform_replacement)

                # Additional check to confirm that the topic present
                # in connector topics

                if topic in self.connector_manifest.topic_names:
                    dataset_name = (
                        database_name + "." + source_table
                        if database_name
                        else source_table
                    )

                    lineage = KafkaConnectLineage(
                        source_dataset=dataset_name,
                        source_platform=source_platform,
                        target_dataset=topic,
                        target_platform="kafka",
                    )
                    topic_names.remove(topic)
                    lineages.append(lineage)

            if topic_names:
                lineages.extend(
                    self.default_get_lineages(
                        database_name=database_name,
                        source_platform=source_platform,
                        topic_prefix=topic_prefix,
                        topic_names=topic_names,
                        include_source_dataset=False,
                    )
                )
                self.report_warning(
                    self.connector_manifest.name,
                    f"could not find input dataset, for connector topics {topic_names}",
                )
            self.connector_manifest.lineages = lineages
            return
        else:
            include_source_dataset = True
            if SINGLE_TRANSFORM and UNKNOWN_TRANSFORM:
                self.report_warning(
                    self.connector_manifest.name,
                    f"could not find input dataset, connector has unknown transform - {transforms[0]['type']}",
                )
                include_source_dataset = False
            if not SINGLE_TRANSFORM and UNKNOWN_TRANSFORM:
                self.report_warning(
                    self.connector_manifest.name,
                    "could not find input dataset, connector has one or more unknown transforms",
                )
                include_source_dataset = False
            lineages = self.default_get_lineages(
                database_name=database_name,
                source_platform=source_platform,
                topic_prefix=topic_prefix,
                include_source_dataset=include_source_dataset,
            )
            self.connector_manifest.lineages = lineages
            return


@dataclass
class DebeziumSourceConnector:
    connector_manifest: ConnectorManifest

    def __init__(self, connector_manifest: ConnectorManifest) -> None:
        self.connector_manifest = connector_manifest
        self._extract_lineages()

    @dataclass
    class DebeziumParser:
        source_platform: str
        server_name: Optional[str]
        database_name: Optional[str]

    def get_parser(
        self,
        connector_manifest: ConnectorManifest,
    ) -> DebeziumParser:
        connector_class = connector_manifest.config.get("connector.class", "")
        if connector_class == "io.debezium.connector.mysql.MySqlConnector":
            # https://debezium.io/documentation/reference/connectors/mysql.html#mysql-topic-names
            parser = self.DebeziumParser(
                source_platform="mysql",
                server_name=connector_manifest.config.get("database.server.name"),
                database_name=None,
            )
        elif connector_class == "MySqlConnector":
            parser = self.DebeziumParser(
                source_platform="mysql",
                server_name=connector_manifest.config.get("database.server.name"),
                database_name=None,
            )
        elif connector_class == "io.debezium.connector.mongodb.MongoDbConnector":
            # https://debezium.io/documentation/reference/connectors/mongodb.html#mongodb-topic-names
            parser = self.DebeziumParser(
                source_platform="mongodb",
                server_name=connector_manifest.config.get("database.server.name"),
                database_name=None,
            )
        elif connector_class == "io.debezium.connector.postgresql.PostgresConnector":
            # https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-topic-names
            parser = self.DebeziumParser(
                source_platform="postgres",
                server_name=connector_manifest.config.get("database.server.name"),
                database_name=connector_manifest.config.get("database.dbname"),
            )
        elif connector_class == "io.debezium.connector.oracle.OracleConnector":
            # https://debezium.io/documentation/reference/connectors/oracle.html#oracle-topic-names
            parser = self.DebeziumParser(
                source_platform="oracle",
                server_name=connector_manifest.config.get("database.server.name"),
                database_name=connector_manifest.config.get("database.dbname"),
            )
        elif connector_class == "io.debezium.connector.sqlserver.SqlServerConnector":
            # https://debezium.io/documentation/reference/connectors/sqlserver.html#sqlserver-topic-names
            parser = self.DebeziumParser(
                source_platform="mssql",
                server_name=connector_manifest.config.get("database.server.name"),
                database_name=connector_manifest.config.get("database.dbname"),
            )
        elif connector_class == "io.debezium.connector.db2.Db2Connector":
            # https://debezium.io/documentation/reference/connectors/db2.html#db2-topic-names
            parser = self.DebeziumParser(
                source_platform="db2",
                server_name=connector_manifest.config.get("database.server.name"),
                database_name=connector_manifest.config.get("database.dbname"),
            )
        elif connector_class == "io.debezium.connector.vitess.VitessConnector":
            # https://debezium.io/documentation/reference/connectors/vitess.html#vitess-topic-names
            parser = self.DebeziumParser(
                source_platform="vitess",
                server_name=connector_manifest.config.get("database.server.name"),
                database_name=connector_manifest.config.get("vitess.keyspace"),
            )
        else:
            raise ValueError(f"Connector class '{connector_class}' is unknown.")

        return parser

    def _extract_lineages(self):
        lineages: List[KafkaConnectLineage] = list()
        parser = self.get_parser(self.connector_manifest)
        source_platform = parser.source_platform
        server_name = parser.server_name
        database_name = parser.database_name
        topic_naming_pattern = r"({0})\.(\w+\.\w+)".format(server_name)

        if not self.connector_manifest.topic_names:
            return lineages

        for topic in self.connector_manifest.topic_names:
            found = re.search(re.compile(topic_naming_pattern), topic)

            if found:
                table_name = (
                    database_name + "." + found.group(2)
                    if database_name
                    else found.group(2)
                )

                lineage = KafkaConnectLineage(
                    source_dataset=table_name,
                    source_platform=source_platform,
                    target_dataset=topic,
                    target_platform="kafka",
                )
                lineages.append(lineage)
        self.connector_manifest.lineages = lineages


class KafkaConnectSource(Source):
    """The class for Kafka Connect source.

    Attributes:
        config (KafkaConnectSourceConfig): Kafka Connect cluster REST API configurations.
        report (KafkaConnectSourceReport): Kafka Connect source ingestion report.

    """

    config: KafkaConnectSourceConfig
    report: KafkaConnectSourceReport

    def __init__(self, config: KafkaConnectSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = KafkaConnectSourceReport()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

        # Test the connection
        test_response = self.session.get(f"{self.config.connect_uri}")
        test_response.raise_for_status()
        logger.info(f"Connection to {self.config.connect_uri} is ok")

        if not jpype.isJVMStarted():
            jpype.startJVM()

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = KafkaConnectSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_connectors_manifest(self) -> List[ConnectorManifest]:
        """Get Kafka Connect connectors manifest using REST API.

        Enrich with lineages metadata.
        """
        connectors_manifest = list()

        connector_response = self.session.get(
            f"{self.config.connect_uri}/connectors",
        )

        payload = connector_response.json()

        for c in payload:
            connector_url = f"{self.config.connect_uri}/connectors/{c}"
            connector_response = self.session.get(connector_url)

            manifest = connector_response.json()
            connector_manifest = ConnectorManifest(**manifest)
            # Initialize connector lineages
            connector_manifest.lineages = list()
            connector_manifest.url = connector_url

            # Populate Source Connector metadata
            if connector_manifest.type == "source":
                topics = self.session.get(
                    f"{self.config.connect_uri}/connectors/{c}/topics",
                ).json()

                connector_manifest.topic_names = topics[c]["topics"]

                tasks = self.session.get(
                    f"{self.config.connect_uri}/connectors/{c}/tasks",
                ).json()

                connector_manifest.tasks = tasks

                # JDBC source connector lineages
                if connector_manifest.config.get("connector.class").__eq__(
                    "io.confluent.connect.jdbc.JdbcSourceConnector"
                ):
                    connector_manifest = ConfluentJDBCSourceConnector(
                        connector_manifest=connector_manifest, report=self.report
                    ).connector_manifest
                else:
                    # Debezium Source Connector lineages
                    try:
                        connector_manifest = DebeziumSourceConnector(
                            connector_manifest=connector_manifest
                        ).connector_manifest

                    except ValueError as err:
                        logger.warning(
                            f"Skipping connector {connector_manifest.name} due to error: {err}"
                        )
                        self.report.report_failure(connector_manifest.name, str(err))
                        continue

            if connector_manifest.type == "sink":
                # TODO: Sink Connector not yet implemented
                self.report.report_dropped(connector_manifest.name)
                logger.warning(
                    f"Skipping connector {connector_manifest.name}. Lineage for Sink Connector not yet implemented"
                )
                pass

            connectors_manifest.append(connector_manifest)

        return connectors_manifest

    def construct_flow_workunit(
        self, connector: ConnectorManifest
    ) -> Iterable[MetadataWorkUnit]:
        connector_name = connector.name
        connector_type = connector.type
        connector_class = connector.config.get("connector.class")
        flow_property_bag = connector.flow_property_bag
        # connector_url = connector.url  # NOTE: this will expose connector credential when used
        flow_urn = builder.make_data_flow_urn(
            "kafka-connect", connector_name, self.config.env
        )

        mcp = MetadataChangeProposalWrapper(
            entityType="dataFlow",
            entityUrn=flow_urn,
            changeType=models.ChangeTypeClass.UPSERT,
            aspectName="dataFlowInfo",
            aspect=models.DataFlowInfoClass(
                name=connector_name,
                description=f"{connector_type.capitalize()} connector using `{connector_class}` plugin.",
                customProperties=flow_property_bag,
                # externalUrl=connector_url, # NOTE: this will expose connector credential when used
            ),
        )

        for proposal in [mcp]:
            wu = MetadataWorkUnit(
                id=f"kafka-connect.{connector_name}.{proposal.aspectName}", mcp=proposal
            )
            self.report.report_workunit(wu)
            yield wu

    def construct_job_workunits(
        self, connector: ConnectorManifest
    ) -> Iterable[MetadataWorkUnit]:

        connector_name = connector.name
        flow_urn = builder.make_data_flow_urn(
            "kafka-connect", connector_name, self.config.env
        )

        lineages = connector.lineages
        if lineages:
            for lineage in lineages:
                source_dataset = lineage.source_dataset
                source_platform = lineage.source_platform
                target_dataset = lineage.target_dataset
                target_platform = lineage.target_platform
                # job_property_bag = lineage.job_property_bag

                job_id = (
                    source_dataset
                    if source_dataset
                    else f"unknown_source.{target_dataset}"
                )
                job_urn = builder.make_data_job_urn_with_flow(flow_urn, job_id)

                inlets = (
                    [builder.make_dataset_urn(source_platform, source_dataset)]
                    if source_dataset
                    else []
                )
                outlets = [builder.make_dataset_urn(target_platform, target_dataset)]

                mcp = MetadataChangeProposalWrapper(
                    entityType="dataJob",
                    entityUrn=job_urn,
                    changeType=models.ChangeTypeClass.UPSERT,
                    aspectName="dataJobInfo",
                    aspect=models.DataJobInfoClass(
                        name=f"{connector_name}:{job_id}",
                        type="COMMAND",
                        description=None,
                        # customProperties=job_property_bag
                        # externalUrl=job_url,
                    ),
                )

                wu = MetadataWorkUnit(
                    id=f"kafka-connect.{connector_name}.{job_id}.{mcp.aspectName}",
                    mcp=mcp,
                )
                self.report.report_workunit(wu)
                yield wu

                mcp = MetadataChangeProposalWrapper(
                    entityType="dataJob",
                    entityUrn=job_urn,
                    changeType=models.ChangeTypeClass.UPSERT,
                    aspectName="dataJobInputOutput",
                    aspect=models.DataJobInputOutputClass(
                        inputDatasets=inlets,
                        outputDatasets=outlets,
                    ),
                )

                wu = MetadataWorkUnit(
                    id=f"kafka-connect.{connector_name}.{job_id}.{mcp.aspectName}",
                    mcp=mcp,
                )
                self.report.report_workunit(wu)
                yield wu

    def construct_lineage_workunits(
        self, connector: ConnectorManifest
    ) -> Iterable[MetadataWorkUnit]:

        lineages = connector.lineages
        if lineages:
            for lineage in lineages:
                source_dataset = lineage.source_dataset
                source_platform = lineage.source_platform
                target_dataset = lineage.target_dataset
                target_platform = lineage.target_platform

                mcp = MetadataChangeProposalWrapper(
                    entityType="dataset",
                    entityUrn=builder.make_dataset_urn(
                        target_platform, target_dataset, self.config.env
                    ),
                    changeType=models.ChangeTypeClass.UPSERT,
                    aspectName="dataPlatformInstance",
                    aspect=models.DataPlatformInstanceClass(platform=target_platform),
                )

                wu = MetadataWorkUnit(id=target_dataset, mcp=mcp)
                self.report.report_workunit(wu)
                yield wu
                if source_dataset:
                    mcp = MetadataChangeProposalWrapper(
                        entityType="dataset",
                        entityUrn=builder.make_dataset_urn(
                            source_platform, source_dataset, self.config.env
                        ),
                        changeType=models.ChangeTypeClass.UPSERT,
                        aspectName="dataPlatformInstance",
                        aspect=models.DataPlatformInstanceClass(
                            platform=source_platform
                        ),
                    )

                    wu = MetadataWorkUnit(id=source_dataset, mcp=mcp)
                    self.report.report_workunit(wu)
                    yield wu

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        connectors_manifest = self.get_connectors_manifest()
        for connector in connectors_manifest:
            name = connector.name
            if self.config.connector_patterns.allowed(name):
                yield from self.construct_flow_workunit(connector)
                yield from self.construct_job_workunits(connector)
                if self.config.construct_lineage_workunits:
                    yield from self.construct_lineage_workunits(connector)

                self.report.report_connector_scanned(name)

            else:
                self.report.report_dropped(name)

    def get_report(self) -> KafkaConnectSourceReport:
        return self.report
