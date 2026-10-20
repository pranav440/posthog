"""Copy self-managed warehouse tables into the object storage sources, free of charge.

A self-managed table is read in place from a customer bucket. Each one gets an object storage source
for its bucket and a schema that reads exactly the table's files, and then one non-billable sync. The
sync's last step points the existing table row at the copy in PostHog storage (see
`DataWarehouseTable.adopt_into_source`), so the row id and the name stay and existing queries keep
working. Until that step the row reads the customer bucket as before, so a failed or stalled copy
changes nothing a query sees. The schema stays turned off, so nothing is billed until the user turns
syncing on.

A table made from an uploaded file is read in place from PostHog's own bucket. It gets a file upload
source of its own that reads the same file. A file the legacy `file` action stored under `managed/` is
first copied to an upload of its own, because a file upload source reads only the `file_uploads/`
path shape. The legacy file is deleted after the copy syncs, when no other table reads it.

Progress is kept on the rows themselves: a marker in the source's `connection_metadata` and in the
table's `options`. Running the migration again therefore continues where it stopped.
"""

import uuid
import dataclasses
from collections.abc import Callable, Iterable
from enum import StrEnum
from typing import Any, Literal, cast

from django.conf import settings
from django.db import transaction
from django.db.models import CharField, Q, Value
from django.db.models.functions import Cast, Concat
from django.utils import timezone

import boto3
import structlog
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

from posthog.dataclasses import frozen
from posthog.models.team import Team

from products.access_control.backend.facade.api import object_ids_with_rules
from products.data_warehouse.backend.facade.api import delete_external_data_schedule, sync_external_data_job_workflow
from products.warehouse_sources.backend.ad_hoc_sync import (
    SchedulePauseError,
    SyncStillRunningError,
    WorkflowStartError,
    trigger_ad_hoc_sync,
)
from products.warehouse_sources.backend.facade.contracts import UploadReadSettings
from products.warehouse_sources.backend.file_upload_sources import (
    UploadedFile,
    copy_legacy_upload,
    create_source_for_upload,
    file_upload_source,
    upload_for_migration,
    upload_job_inputs,
    upload_schema_metadata,
)
from products.warehouse_sources.backend.file_uploads import (
    FILE_FORMAT_TO_TABLE_FORMAT,
    FILE_UPLOADS_FOLDER,
    LEGACY_UPLOADS_FOLDER,
    HostedUpload,
    LegacyUpload,
    UploadLocation,
)
from products.warehouse_sources.backend.models.credential import DataWarehouseCredential
from products.warehouse_sources.backend.models.external_data_destination import (
    ExternalDataSourceDestination,
    get_or_create_warehouse_destination,
)
from products.warehouse_sources.backend.models.external_data_job import ExternalDataJob
from products.warehouse_sources.backend.models.external_data_schema import (
    SCHEMA_PINNED_METADATA_KEY,
    ExternalDataSchema,
)
from products.warehouse_sources.backend.models.external_data_source import ExternalDataSource
from products.warehouse_sources.backend.models.table import SELF_MANAGED_MIGRATION_OPTION, DataWarehouseTable
from products.warehouse_sources.backend.presentation.views.external_data_source.helpers import (
    source_feature_flag_missing,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.common.object_store.self_managed import (
    SelfManagedCredential,
    SelfManagedLocation,
    StorageService,
    UnmappableTableError,
    needs_preserved_column_names,
    parse_self_managed_url,
    table_definition_for,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.common.object_store.settings import (
    CONNECT_TIMEOUT_SECONDS,
    MAX_REQUEST_ATTEMPTS,
    READ_TIMEOUT_SECONDS,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.common.object_store.source_base import (
    ObjectStorageSource,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.common.registry import SourceRegistry
from products.warehouse_sources.backend.temporal.data_imports.sources.file_upload.source import (
    FileUploadSource,
    UploadedFileFormat,
)
from products.warehouse_sources.backend.temporal.data_imports.sources.file_upload.storage import (
    SupersededUpload,
    superseded_metadata,
)
from products.warehouse_sources.backend.types import ExternalDataJobStatus

logger = structlog.get_logger(__name__)

MIGRATION_MARKER_KEY = SELF_MANAGED_MIGRATION_OPTION
WORKFLOW_ID_PREFIX = "self-managed-migration"
# The marker state between storing a workflow id and Temporal accepting the run.
_DISPATCHING = "dispatching"
DEFAULT_ABANDON_REASON = "Left self-managed by an operator."

# Resolves the region of an S3 bucket whose URL names none, or returns None when it can't.
BucketRegionResolver = Callable[[str, SelfManagedCredential], str | None]

# The upload format each table format of an uploaded file was read as.
_UPLOAD_FORMATS: dict[str, UploadedFileFormat] = {
    table_format: cast(UploadedFileFormat, file_format)
    for file_format, table_format in FILE_FORMAT_TO_TABLE_FORMAT.items()
}


class TableMigrationState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    MIGRATED = "migrated"
    FAILED = "failed"
    ABANDONED = "abandoned"
    SKIPPED = "skipped"


MigrationAction = Literal["none", "would_start", "started", "would_retry", "retried", "start_failed", "limit_reached"]
AbandonAction = Literal["would_abandon", "abandoned", "refused"]


@frozen
class TableMigrationStatus:
    table_id: str
    team_id: int
    name: str
    state: TableMigrationState
    reason: str | None = None
    source_id: str | None = None
    schema_id: str | None = None


@frozen
class TableMigrationOutcome:
    status: TableMigrationStatus
    action: MigrationAction = "none"
    workflow_id: str | None = None
    error: str | None = None


@frozen
class AbandonOutcome:
    table_id: str
    action: AbandonAction
    reason: str | None = None


@frozen
class MigrationOptions:
    team_id: int | None = None
    table_ids: tuple[str, ...] = ()
    live_run: bool = False
    retry_failed: bool = False
    # How many copies one run may start or restart. None starts every one it can.
    limit: int | None = None


@frozen
class _LegacyCopy:
    """A legacy `managed/` upload, and the upload the migration copies it to before the sync."""

    legacy: LegacyUpload
    target: UploadLocation


@frozen
class _TablePlan:
    """How one self-managed table maps onto an import source."""

    source: ObjectStorageSource[Any] | FileUploadSource
    schema_metadata: dict[str, Any]
    job_inputs: dict[str, Any] = dataclasses.field(repr=False)
    # Tables with the same group key share one source. An uploaded table gets a source of its own.
    group_key: str
    description: str
    shares_source: bool = True
    legacy_copy: _LegacyCopy | None = None


@frozen
class _Evaluation:
    table: DataWarehouseTable
    status: TableMigrationStatus
    plan: _TablePlan | None = None
    schema: ExternalDataSchema | None = None
    # A copy was started, but its run has not written a job yet. Starting it again would race it.
    awaiting_job: bool = False
    # The workflow id stored before an earlier start. A new start reuses it, so Temporal refuses a
    # second run while the first one exists, and a crash after dispatch can't copy the table twice.
    stored_workflow_id: str | None = None


def resolve_s3_bucket_region(bucket: str, credential: SelfManagedCredential) -> str | None:
    """Ask S3 for the bucket's region with the table's own key.

    S3 names the region in the `x-amz-bucket-region` header of a HEAD request, also when it
    answers with a redirect or a 403, so a key that may only read objects still finds it.
    """
    if not credential.access_key.strip() or not credential.access_secret.strip():
        return None
    client = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=credential.access_key.strip(),
        aws_secret_access_key=credential.access_secret.strip(),
        config=BotoConfig(
            connect_timeout=CONNECT_TIMEOUT_SECONDS,
            read_timeout=READ_TIMEOUT_SECONDS,
            retries={"max_attempts": MAX_REQUEST_ATTEMPTS, "mode": "standard"},
        ),
    )
    try:
        response = client.head_bucket(Bucket=bucket)
    except ClientError as e:
        response = e.response
    except BotoCoreError:
        return None
    headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
    region = headers.get("x-amz-bucket-region")
    return region if isinstance(region, str) and region else None


def _object_storage_sources() -> dict[StorageService, ObjectStorageSource[Any]]:
    return {
        source.self_managed_service: source
        for source in SourceRegistry.get_all_sources().values()
        if isinstance(source, ObjectStorageSource)
    }


def _marker(options: object) -> dict[str, Any]:
    if isinstance(options, dict):
        marker = options.get(MIGRATION_MARKER_KEY)
        if isinstance(marker, dict):
            return marker
    return {}


def _credential_of(table: DataWarehouseTable) -> SelfManagedCredential | None:
    credential = table.credential
    if credential is None:
        return None
    return SelfManagedCredential(access_key=credential.access_key or "", access_secret=credential.access_secret or "")


class _Planner:
    """Maps tables onto sources, caching the per-team flag check and the per-bucket region lookup."""

    def __init__(self, resolve_region: BucketRegionResolver) -> None:
        self._resolve_region = resolve_region
        self._sources = _object_storage_sources()
        self._upload_source = file_upload_source()
        self._flag_missing: dict[tuple[int, str], bool] = {}
        self._regions: dict[tuple[str, str], str | None] = {}
        self._tables_with_rules: dict[int, set[str]] = {}

    def plan(self, table: DataWarehouseTable) -> _TablePlan | str:
        """The plan for a table, or the reason the table can't be migrated."""
        if self._has_access_rules(table):
            # The new source would get default access, and a member the table's rules shut out
            # could read the same files through a schema of that source.
            return "the table has its own access rules, which the source PostHog would create can't carry"
        credential = _credential_of(table)
        if credential is None:
            hosted = table.hosted_upload
            if hosted is None:
                return "the table has no credential"
            return self._plan_upload(table, hosted)
        try:
            location = parse_self_managed_url(table.url_pattern)
            source = self._sources.get(location.service)
            if source is None:
                return f"no import source reads '{location.service}' storage"
            if self._team_lacks_flag(table.team, source):
                return f"the team doesn't have the '{source.requires_team_feature_flag}' feature flag"
            if location.service == "s3" and location.region is None:
                region = self._region(table, location, credential)
                if region is None:
                    return "the URL doesn't name the bucket's region, and S3 didn't return it"
                location = dataclasses.replace(location, region=region)
            definition = table_definition_for(
                table.name, location, table.format, table.columns, table.csv_allow_double_quotes
            )
            job_inputs = source.job_inputs_for_self_managed(location, credential)
        except UnmappableTableError as e:
            return str(e)
        return _TablePlan(
            source=source,
            schema_metadata=definition.schema_metadata(),
            job_inputs=job_inputs,
            group_key=location.group_key,
            description=f"Self-managed tables in {location.bucket}",
        )

    def _has_access_rules(self, table: DataWarehouseTable) -> bool:
        if table.team_id not in self._tables_with_rules:
            self._tables_with_rules[table.team_id] = object_ids_with_rules(
                team_id=table.team_id, resource="warehouse_table"
            )
        return str(table.id) in self._tables_with_rules[table.team_id]

    def _plan_upload(self, table: DataWarehouseTable, hosted: HostedUpload) -> _TablePlan | str:
        """A table that reads a file the team uploaded gets a file upload source of its own."""
        source = self._upload_source
        if self._team_lacks_flag(table.team, source):
            return f"the team doesn't have the '{source.requires_team_feature_flag}' feature flag"
        file_format = _UPLOAD_FORMATS.get(table.format)
        if file_format is None:
            return f"an uploaded '{table.format}' file has no import equivalent"
        upload = UploadedFile(location=upload_for_migration(table, hosted), file_format=file_format)
        columns = table.columns or {}
        read_settings = UploadReadSettings(columns=columns, allow_double_quotes=table.csv_allow_double_quotes)
        return _TablePlan(
            source=source,
            schema_metadata=upload_schema_metadata(
                table.name, upload, read_settings, needs_preserved_column_names(columns.keys())
            ),
            job_inputs=upload_job_inputs(upload, table.name),
            group_key=f"upload/{table.pk}",
            description=f"Uploaded file for {table.name}"[:400],
            shares_source=False,
            legacy_copy=_LegacyCopy(legacy=hosted, target=upload.location)
            if isinstance(hosted, LegacyUpload)
            else None,
        )

    def _team_lacks_flag(self, team: Team, source: ObjectStorageSource[Any] | FileUploadSource) -> bool:
        key = (team.pk, str(source.source_type))
        if key not in self._flag_missing:
            self._flag_missing[key] = source_feature_flag_missing(source, team)
        return self._flag_missing[key]

    def _region(
        self, table: DataWarehouseTable, location: SelfManagedLocation, credential: SelfManagedCredential
    ) -> str | None:
        key = (str(table.credential_id), location.bucket)
        if key not in self._regions:
            try:
                self._regions[key] = self._resolve_region(location.bucket, credential)
            except Exception:
                logger.warning("Could not resolve the S3 bucket region", table_id=str(table.id), exc_info=True)
                self._regions[key] = None
        return self._regions[key]


def _uploaded_table_filter() -> Q:
    """Credential-less tables that read a file in PostHog's bucket, in either upload folder.

    Narrowed to the table's own team folder of each upload folder, so tables PostHog writes to the
    bucket itself (materialized views) stay out. `DataWarehouseTable.hosted_upload` then checks the
    exact key.
    """
    domain = settings.DATAWAREHOUSE_BUCKET_DOMAIN
    if not domain or not settings.DATAWAREHOUSE_BUCKET:
        return Q(pk__in=[])

    def team_folder(folder: str) -> Concat:
        return Concat(
            Value(f"https://{domain}/{folder}/team_"),
            Cast("team_id", CharField()),
            Value("/"),
            output_field=CharField(),
        )

    in_upload_folder = Q(url_pattern__startswith=team_folder(FILE_UPLOADS_FOLDER)) | Q(
        url_pattern__startswith=team_folder(LEGACY_UPLOADS_FOLDER)
    )
    return Q(external_data_source__isnull=True, credential__isnull=True) & in_upload_folder


def _select_tables(team_id: int | None, table_ids: Iterable[str]) -> list[DataWarehouseTable]:
    """Self-managed tables, plus every table the migration has touched, oldest first.

    A table that a file upload source already imports is left out: it is not self-managed, it is
    only waiting for its own first sync.
    """
    queryset = (
        DataWarehouseTable.raw_objects.exclude(deleted=True)
        .filter(
            Q(external_data_source__isnull=True, credential__isnull=False)
            | _uploaded_table_filter()
            | Q(options__has_key=MIGRATION_MARKER_KEY)
        )
        .select_related("credential", "team")
        .order_by("created_at", "id")
    )
    if team_id is not None:
        queryset = queryset.filter(team_id=team_id)
    ids = list(table_ids)
    if ids:
        queryset = queryset.filter(id__in=ids)
    tables = list(queryset)
    imported_elsewhere = set(
        ExternalDataSchema.objects.filter(table_id__in=[table.id for table in tables], deleted=False)
        .exclude(source__deleted=True)
        .exclude(source__connection_metadata__has_key=MIGRATION_MARKER_KEY)
        .values_list("table_id", flat=True)
    )
    return [table for table in tables if table.id not in imported_elsewhere]


def _migration_schemas(tables: list[DataWarehouseTable]) -> dict[str, ExternalDataSchema]:
    schemas = (
        ExternalDataSchema.objects.filter(table_id__in=[table.id for table in tables])
        .exclude(deleted=True)
        .exclude(source__deleted=True)
        .filter(source__connection_metadata__has_key=MIGRATION_MARKER_KEY)
        .select_related("source")
        .order_by("created_at")
    )
    return {str(schema.table_id): schema for schema in schemas}


def _latest_jobs(schemas: Iterable[ExternalDataSchema]) -> dict[str, ExternalDataJob]:
    schema_ids = [schema.id for schema in schemas]
    if not schema_ids:
        return {}
    jobs = ExternalDataJob.objects.filter(schema_id__in=schema_ids).order_by("schema_id", "-created_at")
    latest: dict[str, ExternalDataJob] = {}
    for job in jobs:
        latest.setdefault(str(job.schema_id), job)
    return latest


def _evaluate(
    table: DataWarehouseTable,
    schema: ExternalDataSchema | None,
    job: ExternalDataJob | None,
    planner: _Planner,
) -> _Evaluation:
    marker = _marker(table.options)

    def status(state: TableMigrationState, reason: str | None = None) -> TableMigrationStatus:
        return TableMigrationStatus(
            table_id=str(table.id),
            team_id=table.team_id,
            name=table.name,
            state=state,
            reason=reason,
            source_id=str(schema.source_id) if schema is not None else marker.get("source_id"),
            schema_id=str(schema.id) if schema is not None else marker.get("schema_id"),
        )

    if table.external_data_source_id is not None:
        return _Evaluation(table=table, status=status(TableMigrationState.MIGRATED), schema=schema)
    if marker.get("state") == TableMigrationState.ABANDONED:
        return _Evaluation(table=table, status=status(TableMigrationState.ABANDONED, marker.get("reason")))

    planned = planner.plan(table)
    plan = planned if isinstance(planned, _TablePlan) else None
    if schema is None or job is None:
        if plan is None:
            return _Evaluation(table=table, status=status(TableMigrationState.SKIPPED, str(planned)), schema=schema)
        stored_workflow_id = marker.get("workflow_id") if schema is not None else None
        dispatching = bool(stored_workflow_id) and marker.get("state") == _DISPATCHING
        awaiting_job = bool(stored_workflow_id) and not dispatching
        if dispatching:
            reason = "a start was not confirmed; the next run starts it with the same workflow id"
        else:
            reason = "waiting for the copy to start" if awaiting_job else "not started"
        return _Evaluation(
            table=table,
            status=status(TableMigrationState.PENDING, reason),
            plan=plan,
            schema=schema,
            awaiting_job=awaiting_job,
            stored_workflow_id=stored_workflow_id if isinstance(stored_workflow_id, str) else None,
        )
    if job.status == ExternalDataJobStatus.RUNNING:
        return _Evaluation(table=table, status=status(TableMigrationState.RUNNING), plan=plan, schema=schema)
    if job.status == ExternalDataJobStatus.COMPLETED:
        reason = "the copy finished, but the table still reads its own bucket"
    else:
        reason = job.latest_error or f"the copy ended with status {job.status}"
    if plan is None:
        reason = f"{reason}; it can't be retried because {planned}"
    return _Evaluation(table=table, status=status(TableMigrationState.FAILED, reason), plan=plan, schema=schema)


def evaluate_tables(
    team_id: int | None = None,
    table_ids: Iterable[str] = (),
    resolve_region: BucketRegionResolver = resolve_s3_bucket_region,
) -> list[TableMigrationStatus]:
    return [evaluation.status for evaluation in _evaluate_all(team_id, table_ids, _Planner(resolve_region))]


def _evaluate_all(team_id: int | None, table_ids: Iterable[str], planner: _Planner) -> list[_Evaluation]:
    tables = _select_tables(team_id, table_ids)
    schemas = _migration_schemas(tables)
    jobs = _latest_jobs(schemas.values())
    evaluations: list[_Evaluation] = []
    for table in tables:
        schema = schemas.get(str(table.id))
        job = jobs.get(str(schema.id)) if schema is not None else None
        evaluations.append(_evaluate(table, schema, job, planner))
    return evaluations


def _update_table_marker(table: DataWarehouseTable, **changes: Any) -> None:
    """Merge into the table's migration marker under a row lock.

    The import pipeline writes `options` when it adopts the table, so the marker is merged into
    the stored value rather than written from the copy loaded at the start of the run.
    """
    with transaction.atomic():
        locked = DataWarehouseTable.raw_objects.select_for_update().get(pk=table.pk)
        options = dict(locked.options or {})
        options[MIGRATION_MARKER_KEY] = {**_marker(options), **changes}
        locked.options = options
        locked.save(update_fields=["options"])
    table.options = options


def _upload_source(table: DataWarehouseTable, plan: _TablePlan) -> ExternalDataSource:
    """A source of the table's own. A legacy file it no longer reads is deleted after the copy."""
    connection_metadata: dict[str, Any] = {MIGRATION_MARKER_KEY: {"group_key": plan.group_key}}
    if plan.legacy_copy is not None:
        connection_metadata = {
            **connection_metadata,
            **superseded_metadata(SupersededUpload(upload=plan.legacy_copy.legacy, superseded_at=timezone.now())),
        }
    source = create_source_for_upload(table, plan.job_inputs, connection_metadata=connection_metadata)
    if table.created_at < source.created_at:
        ExternalDataSource.objects.filter(pk=source.pk).update(created_at=table.created_at)
        source.created_at = table.created_at
    return source


def _find_or_create_source(table: DataWarehouseTable, plan: _TablePlan) -> ExternalDataSource:
    """The group's source for the table's team, credential and bucket. Runs under the group lock."""
    if not plan.shares_source:
        return _upload_source(table, plan)
    existing = (
        ExternalDataSource.objects.filter(
            team_id=table.team_id,
            source_type=plan.source.source_type,
            **{
                f"connection_metadata__{MIGRATION_MARKER_KEY}__group_key": plan.group_key,
                f"connection_metadata__{MIGRATION_MARKER_KEY}__credential_id": str(table.credential_id),
            },
        )
        .exclude(deleted=True)
        .order_by("created_at")
        .first()
    )
    source = existing or ExternalDataSource.objects.create(
        source_id=str(uuid.uuid4()),
        connection_id=str(uuid.uuid4()),
        destination_id=str(uuid.uuid4()),
        team_id=table.team_id,
        status=ExternalDataSource.Status.RUNNING,
        source_type=plan.source.source_type,
        api_version=plan.source.default_version,
        job_inputs=plan.job_inputs,
        description=plan.description,
        access_method=ExternalDataSource.AccessMethod.WAREHOUSE,
        connection_metadata={
            MIGRATION_MARKER_KEY: {
                "group_key": plan.group_key,
                "credential_id": str(table.credential_id),
            }
        },
    )
    if existing is not None:
        return existing
    ExternalDataSourceDestination.objects.for_team(table.team_id).create(
        team_id=table.team_id, source_id=source.pk, destination=get_or_create_warehouse_destination(table.team_id)
    )
    # A source's first week of syncs is free (`FREE_HISTORICAL_WINDOW`). Dating a new source to the
    # table it is made for keeps the user's own syncs of long-standing tables billed as usual. Tables
    # are planned oldest first, so that is the oldest table of the run. An existing source keeps its
    # date, because moving it would change which of its past syncs fall in the free window.
    if table.created_at < source.created_at:
        ExternalDataSource.objects.filter(pk=source.pk).update(created_at=table.created_at)
        source.created_at = table.created_at
    return source


def _ensure_schema(table: DataWarehouseTable, plan: _TablePlan) -> ExternalDataSchema:
    """The table's migration schema, created with its source when there is none yet.

    The credential row is the group lock: every table of one source shares it, and nothing else
    locks it, so two runs can't create two sources or two schemas for the same table. An uploaded
    table has no credential and a source of its own, so its own row is the lock.
    """
    with transaction.atomic():
        if table.credential_id is not None:
            DataWarehouseCredential.objects.select_for_update().get(pk=table.credential_id)
        else:
            DataWarehouseTable.raw_objects.select_for_update().get(pk=table.pk)
        existing = (
            ExternalDataSchema.objects.filter(team_id=table.team_id, table_id=table.id)
            .exclude(deleted=True)
            .filter(source__connection_metadata__has_key=MIGRATION_MARKER_KEY)
            .exclude(source__deleted=True)
            .select_related("source")
            .first()
        )
        if existing is not None:
            return existing
        source = _find_or_create_source(table, plan)
        if (
            ExternalDataSchema.objects.filter(team_id=table.team_id, source_id=source.pk, name=table.name)
            .exclude(deleted=True)
            .exists()
        ):
            raise UnmappableTableError(f"another table in this bucket already uses the name '{table.name}'")
        schema = ExternalDataSchema.objects.create(
            name=table.name,
            team_id=table.team_id,
            source=source,
            table=table,
            should_sync=False,
            sync_type=ExternalDataSchema.SyncType.FULL_REFRESH,
            sync_type_config={"schema_metadata": {**plan.schema_metadata, SCHEMA_PINNED_METADATA_KEY: True}},
        )
    _update_table_marker(table, source_id=str(source.pk), schema_id=str(schema.pk), state="planned")
    return schema


class _CopyAlreadyStartedError(Exception):
    """Temporal already holds a run with the stored workflow id, so this start added none."""


def _new_workflow_id(schema: ExternalDataSchema) -> str:
    return f"{schema.id}-{WORKFLOW_ID_PREFIX}-{uuid.uuid4().hex[:12]}"


def _start_copy(
    client: Client, table: DataWarehouseTable, schema: ExternalDataSchema, stored_workflow_id: str | None
) -> str:
    # Temporal accepted this id before, so a run with it exists or existed.
    confirmed = stored_workflow_id is not None and _marker(table.options).get("state") == "triggered"
    workflow_id = stored_workflow_id or _new_workflow_id(schema)
    if not confirmed:
        # Stored before the start, so a command that stops between the start and the next write
        # still knows the id, and its next start can't add a second copy.
        _update_table_marker(table, state=_DISPATCHING, workflow_id=workflow_id, triggered_at=None)
    # The schedule is created paused, because a run fired by the schedule is billable. The ad hoc
    # run then sees a paused schedule and leaves it paused when it finishes.
    sync_external_data_job_workflow(schema, create=True, should_sync=False, trigger_immediately=False)
    try:
        trigger_ad_hoc_sync(
            client,
            schema,
            billable=False,
            reset_pipeline=False,
            workflow_id_prefix=WORKFLOW_ID_PREFIX,
            workflow_id=workflow_id,
        )
    except WorkflowStartError as e:
        if not isinstance(e.__cause__, WorkflowAlreadyStartedError):
            raise
        if confirmed:
            raise _CopyAlreadyStartedError(
                f"the workflow {workflow_id} is still running or already ended without a job; check it in Temporal"
            ) from e
    _update_table_marker(table, state="triggered", triggered_at=timezone.now().isoformat())
    return workflow_id


def migrate_self_managed_tables(
    options: MigrationOptions,
    client: Client | None = None,
    resolve_region: BucketRegionResolver = resolve_s3_bucket_region,
    pause: Callable[[], None] | None = None,
) -> list[TableMigrationOutcome]:
    """Plan, and on a live run start, the copy of every selected self-managed table.

    A dry run reads the same rows and reports the same decisions, but writes nothing. `pause` runs
    between two starts so a large batch does not reach the workers at once.
    """
    if options.live_run and client is None:
        raise ValueError("A live run needs a Temporal client")
    planner = _Planner(resolve_region)
    outcomes: list[TableMigrationOutcome] = []
    started = 0
    for evaluation in _evaluate_all(options.team_id, options.table_ids, planner):
        state = evaluation.status.state
        # A start that never wrote a job counts as failed for a retry, because its run is gone.
        is_retry = options.retry_failed and (state == TableMigrationState.FAILED or evaluation.awaiting_job)
        is_start = state == TableMigrationState.PENDING and not evaluation.awaiting_job
        if evaluation.plan is None or not (is_start or is_retry):
            outcomes.append(TableMigrationOutcome(status=evaluation.status))
            continue
        if options.limit is not None and started >= options.limit:
            outcomes.append(TableMigrationOutcome(status=evaluation.status, action="limit_reached"))
            continue
        if not options.live_run:
            started += 1
            outcomes.append(
                TableMigrationOutcome(status=evaluation.status, action="would_start" if is_start else "would_retry")
            )
            continue
        if started and pause is not None:
            pause()
        started += 1
        outcomes.append(_start(client, evaluation, is_retry))
    return outcomes


def _copy_legacy_upload(legacy_copy: _LegacyCopy) -> None:
    try:
        copy_legacy_upload(legacy_copy.legacy, legacy_copy.target)
    except Exception as e:
        raise UnmappableTableError(f"the uploaded file couldn't be copied: {e}") from e


def _start(client: Client | None, evaluation: _Evaluation, is_retry: bool) -> TableMigrationOutcome:
    assert client is not None and evaluation.plan is not None
    table = evaluation.table
    plan = evaluation.plan
    # A retry after a finished job needs a new workflow id: Temporal refuses the id of a run that ended.
    stored_workflow_id = (
        evaluation.stored_workflow_id if evaluation.status.state == TableMigrationState.PENDING else None
    )
    try:
        if plan.legacy_copy is not None:
            _copy_legacy_upload(plan.legacy_copy)
        schema = evaluation.schema or _ensure_schema(table, plan)
        workflow_id = _start_copy(client, table, schema, stored_workflow_id)
    except (
        UnmappableTableError,
        SchedulePauseError,
        WorkflowStartError,
        SyncStillRunningError,
        _CopyAlreadyStartedError,
    ) as e:
        logger.warning("Could not start a self-managed table copy", table_id=str(table.id), error=str(e))
        return TableMigrationOutcome(status=evaluation.status, action="start_failed", error=str(e))
    except Exception as e:
        # A Temporal or database error on one table must not stop the batch or hide its outcomes.
        logger.warning("Could not start a self-managed table copy", table_id=str(table.id), exc_info=True)
        return TableMigrationOutcome(status=evaluation.status, action="start_failed", error=f"{type(e).__name__}: {e}")
    logger.info(
        "Started a self-managed table copy",
        table_id=str(table.id),
        team_id=table.team_id,
        schema_id=str(schema.id),
        workflow_id=workflow_id,
    )
    return TableMigrationOutcome(
        status=dataclasses.replace(evaluation.status, schema_id=str(schema.id), source_id=str(schema.source_id)),
        action="retried" if is_retry else "started",
        workflow_id=workflow_id,
    )


def abandon_tables(
    table_ids: Iterable[str],
    reason: str = DEFAULT_ABANDON_REASON,
    team_id: int | None = None,
    live_run: bool = False,
) -> list[AbandonOutcome]:
    """Leave tables self-managed for good, and remove what the migration created for them.

    A table's source goes too when the table was the last one it held. A table that is already
    copied, or whose copy is running, is refused: the first can't be undone here, and the second
    would adopt the table after the abandon.
    """
    ids = list(table_ids)
    tables = {str(table.id): table for table in _select_tables(team_id, ids)}
    schemas = _migration_schemas(list(tables.values()))
    jobs = _latest_jobs(schemas.values())
    outcomes: list[AbandonOutcome] = []
    for table_id in ids:
        table = tables.get(table_id)
        if table is None:
            outcomes.append(AbandonOutcome(table_id=table_id, action="refused", reason="no such self-managed table"))
            continue
        schema = schemas.get(table_id)
        job = jobs.get(str(schema.id)) if schema is not None else None
        if table.external_data_source_id is not None:
            outcomes.append(AbandonOutcome(table_id=table_id, action="refused", reason="the table is already migrated"))
        elif job is not None and job.status == ExternalDataJobStatus.RUNNING:
            outcomes.append(AbandonOutcome(table_id=table_id, action="refused", reason="the table's copy is running"))
        elif schema is not None and job is None and _marker(table.options).get("workflow_id"):
            outcomes.append(
                AbandonOutcome(
                    table_id=table_id,
                    action="refused",
                    reason="the table's copy was started and has no job yet; wait until --status shows the job",
                )
            )
        elif not live_run:
            outcomes.append(AbandonOutcome(table_id=table_id, action="would_abandon"))
        else:
            _abandon(table, schema, reason)
            outcomes.append(AbandonOutcome(table_id=table_id, action="abandoned", reason=reason))
    return outcomes


def _abandon(table: DataWarehouseTable, schema: ExternalDataSchema | None, reason: str) -> None:
    removed_source_id: str | None = None
    # The marker is written with the deletion, so a failure after it can't leave a table that the
    # next run reads as pending and copies again.
    with transaction.atomic():
        if schema is not None:
            # Unlinked first, so deleting the schema can never reach the customer's table.
            schema.table = None
            schema.save(update_fields=["table", "updated_at"])
            schema.soft_delete()
            source = schema.source
            if (
                not ExternalDataSchema.objects.filter(team_id=table.team_id, source_id=source.pk)
                .exclude(deleted=True)
                .exists()
            ):
                source.soft_delete()
                removed_source_id = str(source.pk)
        _update_table_marker(
            table,
            state=TableMigrationState.ABANDONED.value,
            reason=reason,
            abandoned_at=timezone.now().isoformat(),
        )
    if schema is not None:
        try:
            delete_external_data_schedule(str(schema.id))
        except Exception:
            # The schedule is paused and its schema is deleted, so it can't start a copy.
            logger.warning(
                "Could not delete the schedule of an abandoned migration schema",
                table_id=str(table.id),
                schema_id=str(schema.id),
                exc_info=True,
            )
    logger.info(
        "Abandoned a self-managed table migration",
        table_id=str(table.id),
        team_id=table.team_id,
        removed_source_id=removed_source_id,
    )


def describe_status(status: TableMigrationStatus) -> str:
    reason = f" ({status.reason})" if status.reason else ""
    return f"table={status.table_id} team={status.team_id} name={status.name} state={status.state}{reason}"
