import json
import logging
import os.path
import sys
import typing
import urllib.parse
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

import click
import requests
import yaml
from pydantic import BaseModel, ValidationError
from requests.models import Response
from requests.sessions import Session

log = logging.getLogger(__name__)

DEFAULT_GMS_HOST = "http://localhost:8080"
CONDENSED_DATAHUB_CONFIG_PATH = "~/.datahubenv"
DATAHUB_CONFIG_PATH = os.path.expanduser(CONDENSED_DATAHUB_CONFIG_PATH)

ENV_SKIP_CONFIG = "DATAHUB_SKIP_CONFIG"
ENV_METADATA_HOST = "DATAHUB_GMS_HOST"
ENV_METADATA_TOKEN = "DATAHUB_GMS_TOKEN"


class GmsConfig(BaseModel):
    server: str
    token: Optional[str]


class DatahubConfig(BaseModel):
    gms: GmsConfig


def write_datahub_config(host: str, token: Optional[str]) -> None:
    config = {
        "gms": {
            "server": host,
            "token": token,
        }
    }
    with open(DATAHUB_CONFIG_PATH, "w+") as outfile:
        yaml.dump(config, outfile, default_flow_style=False)
    return None


def should_skip_config() -> bool:
    return os.getenv(ENV_SKIP_CONFIG, False) == "True"


def ensure_datahub_config() -> None:
    if not os.path.isfile(DATAHUB_CONFIG_PATH):
        click.secho(
            f"No {CONDENSED_DATAHUB_CONFIG_PATH} file found, generating one for you...",
            bold=True,
        )
        write_datahub_config(DEFAULT_GMS_HOST, None)


def get_details_from_config():
    with open(DATAHUB_CONFIG_PATH, "r") as stream:
        try:
            config_json = yaml.safe_load(stream)
            try:
                datahub_config = DatahubConfig(**config_json)
            except ValidationError as e:
                click.echo(
                    f"Received error, please check your {CONDENSED_DATAHUB_CONFIG_PATH}"
                )
                click.echo(e, err=True)
                sys.exit(1)

            gms_config = datahub_config.gms

            gms_host = gms_config.server
            gms_token = gms_config.token
            return gms_host, gms_token
        except yaml.YAMLError as exc:
            click.secho(f"{DATAHUB_CONFIG_PATH} malformatted, error: {exc}", bold=True)
    return None, None


def get_details_from_env() -> Tuple[Optional[str], Optional[str]]:
    return os.environ.get(ENV_METADATA_HOST), os.environ.get(ENV_METADATA_TOKEN)


def first_non_null(ls: List[Optional[str]]) -> Optional[str]:
    return next((el for el in ls if el is not None and el.strip() != ""), None)


def get_session_and_host():
    session = requests.Session()

    gms_host_env, gms_token_env = get_details_from_env()
    if should_skip_config():
        gms_host = gms_host_env
        gms_token = gms_token_env
    else:
        ensure_datahub_config()
        gms_host_conf, gms_token_conf = get_details_from_config()
        gms_host = first_non_null([gms_host_env, gms_host_conf])
        gms_token = first_non_null([gms_token_env, gms_token_conf])

    if gms_host is None or gms_host.strip() == "":
        log.error(
            f"GMS Host is not set. Use datahub init command or set {ENV_METADATA_HOST} env var"
        )
        return None, None

    session.headers.update(
        {
            "X-RestLi-Protocol-Version": "2.0.0",
            "Content-Type": "application/json",
        }
    )
    if isinstance(gms_token, str) and len(gms_token) > 0:
        session.headers.update(
            {"Authorization": f"Bearer {gms_token.format(**os.environ)}"}
        )

    return session, gms_host


def test_connection():
    (session, host) = get_session_and_host()
    url = host + "/config"
    response = session.get(url)
    response.raise_for_status()


def parse_run_restli_response(response):
    response_json = response.json()

    if not isinstance(response_json, dict):
        click.echo(f"Received error, please check your {CONDENSED_DATAHUB_CONFIG_PATH}")
        click.echo()
        click.echo(response_json)
        exit()

    summary = response_json.get("value")
    if not isinstance(summary, dict):
        click.echo(f"Received error, please check your {CONDENSED_DATAHUB_CONFIG_PATH}")
        click.echo()
        click.echo(response_json)
        exit()

    return summary


def post_rollback_endpoint(
    payload_obj: dict,
    path: str,
) -> typing.Tuple[typing.List[typing.List[str]], int, int]:
    session, gms_host = get_session_and_host()
    url = gms_host + path

    payload = json.dumps(payload_obj)

    response = session.post(url, payload)

    summary = parse_run_restli_response(response)
    rows = summary.get("aspectRowSummaries")
    entities_affected = summary.get("entitiesAffected")
    aspects_affected = summary.get("aspectsAffected")

    if len(rows) == 0:
        click.echo("No entities touched by this run. Double check your run id?")

    structured_rows = [
        [
            row.get("urn"),
            row.get("aspectName"),
            datetime.utcfromtimestamp(row.get("timestamp") / 1000).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
        ]
        for row in rows
    ]

    return structured_rows, entities_affected, aspects_affected


def post_delete_endpoint(
    payload_obj: dict,
    path: str,
    cached_session_host: Optional[Tuple[Session, str]] = None,
) -> typing.Tuple[str, int]:
    if not cached_session_host:
        session, gms_host = get_session_and_host()
    else:
        session, gms_host = cached_session_host
    url = gms_host + path

    return post_delete_endpoint_with_session_and_url(session, url, payload_obj)


def post_delete_endpoint_with_session_and_url(
    session: Session,
    url: str,
    payload_obj: dict,
) -> typing.Tuple[str, int]:
    payload = json.dumps(payload_obj)

    response = session.post(url, payload)

    summary = parse_run_restli_response(response)
    urn = summary.get("urn")
    rows_affected = summary.get("rows")

    return urn, rows_affected


def get_urns_by_filter(
    platform: Optional[str],
    env: Optional[str],
    entity_type: str = "dataset",
    search_query: str = "*",
) -> Iterable[str]:
    session, gms_host = get_session_and_host()
    endpoint: str = "/entities?action=search"
    url = gms_host + endpoint
    filter_criteria = []
    if env:
        filter_criteria.append({"field": "origin", "value": env, "condition": "EQUAL"})

    if (
        platform is not None
        and entity_type == "dataset"
        or entity_type == "dataflow"
        or entity_type == "datajob"
    ):
        filter_criteria.append(
            {
                "field": "platform",
                "value": f"urn:li:dataPlatform:{platform}",
                "condition": "EQUAL",
            }
        )
    if platform is not None and (
        entity_type.lower() == "chart" or entity_type.lower() == "dashboard"
    ):
        filter_criteria.append(
            {
                "field": "tool",
                "value": platform,
                "condition": "EQUAL",
            }
        )
    if platform is not None and (
        entity_type.lower() == "dataflow" or entity_type.lower() == "dashboard"
    ):
        filter_criteria.append(
            {
                "field": "tool",
                "value": platform,
                "condition": "EQUAL",
            }
        )

    search_body = {
        "input": search_query,
        "entity": entity_type,
        "start": 0,
        "count": 10000,
        "filter": {"or": [{"and": filter_criteria}]},
    }
    payload = json.dumps(search_body)
    log.debug(payload)
    response: Response = session.post(url, payload)
    if response.status_code == 200:
        assert response._content
        results = json.loads(response._content)
        num_entities = results["value"]["numEntities"]
        entities_yielded: int = 0
        for x in results["value"]["entities"]:
            entities_yielded += 1
            log.debug(f"yielding {x['entity']}")
            yield x["entity"]
        assert (
            entities_yielded == num_entities
        ), "Did not delete all entities, try running this command again!"
    else:
        log.error(f"Failed to execute search query with {str(response.content)}")
        response.raise_for_status()


def get_entity(
    urn: str,
    aspect: Optional[List],
    cached_session_host: Optional[Tuple[Session, str]] = None,
) -> Dict:
    if not cached_session_host:
        session, gms_host = get_session_and_host()
    else:
        session, gms_host = cached_session_host

    encoded_urn = urllib.parse.quote(urn)
    endpoint: str = f"/entities/{encoded_urn}"

    if aspect is not None:
        endpoint = endpoint + "?aspects=List(" + ",".join(aspect) + ")"

    response = session.get(gms_host + endpoint)
    return response.json()
