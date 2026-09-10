# db_Metadata — Data Dictionary

**Database:** `db_Metadata` (Fabric SQL Database, `kalwvg5capkefegg5d6gkpgeza-f62dpkihcteudnn7gmui3r7wb4.database.fabric.microsoft.com,1433`)
**Schemas:** `orch` (job/task orchestration control data — the Wave Runner MDF framework's metadata), `log` (execution run-history and audit tables)

> **Source note:** This dictionary was compiled from the `db_Metadata.SQLDatabase` SQL project's git-tracked table definitions (`orch/Tables/*.sql`, `log/Tables/*.sql`), cross-checked against the current `orch_metadata.xlsx` workbook contents for the `orch` schema's lookup/reference data. Neither of this session's two execution environments (this cloud container, and the Linux shell bridged to this computer) has outbound network access to the Fabric SQL endpoint, so a live `INFORMATION_SCHEMA` query could not be run directly — see the note at the end of this document if you'd like that verified from inside a Fabric notebook, where a working non-interactive connection already exists (`nb_Metadata_Sync.ipynb`).

---

## Schema: `orch`

Tables that define *what should run* — jobs, the tasks that make them up, and the small reference tables that constrain and resolve them. These four tables are the ones kept in sync with `orch_metadata.xlsx` via `nb_Metadata_Sync`.

### orch.Jobs

One row per orchestrated Job — a top-level unit of work (e.g., a "wave" of related tasks). Defines the job's own execution policy (timeout, retries, schedule) and links it to its own registered Fabric object identity.

| Column | Data Type | Purpose |
|---|---|---|
| JobName | VARCHAR(200) NOT NULL | **Primary key.** Unique name identifying the job (e.g., `Ingest_ContosoDW_Bronze`). |
| Include | BIT NOT NULL (default 1) | Soft-disable switch — job is skipped by the orchestrator when false, without deleting the row. |
| TimeoutInSeconds | INT NOT NULL | Maximum time, in seconds, the job as a whole may run before being considered timed out. |
| Retries | INT NOT NULL | Number of times to retry the job on failure. |
| RetryIntervalInSeconds | INT NOT NULL | Wait time, in seconds, between retry attempts. |
| ScheduledStartUTC | TIME(0) NULL | Time of day (UTC) the job is scheduled to start, if it runs on a fixed daily schedule. |
| ParametersJson | NVARCHAR(MAX) NULL | JSON blob of job-level parameters passed to whatever orchestrates the job. |
| Dependencies | NVARCHAR(MAX) NULL | JSON array of other `JobName` values this job depends on. |
| WorkspaceName | VARCHAR(200) NULL | Fabric workspace this job's own object identity belongs to; paired with `JobName` as a foreign key into `ObjectIDs`. |
| Environment | NVARCHAR(100) NULL | Deployment environment label (e.g., DEV/TEST/PROD) this job definition applies to. |
| LoggingLevel | TINYINT NOT NULL (default 1) | **Foreign key** to `log.LoggingLevel` — controls how verbosely this job's runs are logged. |

**Constraints:** `PRIMARY KEY CLUSTERED (JobName)`; `FOREIGN KEY (LoggingLevel) REFERENCES log.LoggingLevel`; `FOREIGN KEY (WorkspaceName, JobName) REFERENCES orch.ObjectIDs (WorkspaceName, ObjectName)`.

### orch.Tasks

One row per individual unit of work (a Task) belonging to a Job — e.g., a single notebook invocation copying one source table. This is the work list `orch.spGetNextWave` reads to determine what's ready to run next, based on each task's declared `Dependencies`.

| Column | Data Type | Purpose |
|---|---|---|
| TaskName | VARCHAR(200) NOT NULL | **Primary key.** Unique name for the task (e.g., `CopyBronze_DimCustomer`). |
| Include | BIT NOT NULL (default 1) | Soft-disable switch — task is skipped by the orchestrator when false, without deleting the row. |
| JobName | VARCHAR(200) NOT NULL | **Foreign key** to `orch.Jobs` — which job this task belongs to. |
| ObjectName | VARCHAR(200) NOT NULL | Name of the Fabric object (notebook, pipeline, etc.) that actually performs the task's work. |
| WorkspaceName | VARCHAR(200) NULL | Fabric workspace `ObjectName` lives in; paired with `ObjectName` (and, separately, with `JobName`) as foreign keys into `ObjectIDs` so the orchestrator can resolve real Fabric object/workspace IDs. |
| TimeoutInSeconds | INT NOT NULL | Maximum time, in seconds, this individual task may run. |
| Retries | INT NOT NULL | Retry count on failure for this task. |
| RetryIntervalInSeconds | INT NOT NULL | Wait time, in seconds, between retries. |
| ParametersJson | NVARCHAR(MAX) NULL | JSON blob of per-task parameters (e.g., source schema/table, destination workspace/lakehouse) passed into the invoked object at runtime. |
| Dependencies | NVARCHAR(MAX) NULL | JSON array of `TaskName` values that must have already succeeded before this task is eligible to run; consumed by `spGetNextWave` to compute the next wave of ready tasks. |
| TaskType | NVARCHAR(50) NOT NULL | **Foreign key** to `orch.TaskType` — the kind of object being invoked (e.g., `Notebook`), which also determines whether tasks of this type may run in parallel. |
| System | NVARCHAR(100) NULL | Free-text label for the source/target system this task relates to (e.g., `ContosoDW`) — descriptive/organizational, not enforced by a constraint. |
| Layer | NVARCHAR(100) NULL | Free-text label for the data layer this task populates (e.g., `Bronze`) — descriptive/organizational, not enforced by a constraint. |
| LoggingLevel | TINYINT NOT NULL (default 1) | **Foreign key** to `log.LoggingLevel` — controls logging verbosity for this task's runs. |

**Constraints:** `PRIMARY KEY CLUSTERED (TaskName)`; `FOREIGN KEY (JobName) REFERENCES orch.Jobs`; `FOREIGN KEY (LoggingLevel) REFERENCES log.LoggingLevel`; `FOREIGN KEY (TaskType) REFERENCES orch.TaskType`; `FOREIGN KEY (WorkspaceName, JobName) REFERENCES orch.ObjectIDs (WorkspaceName, ObjectName)`; `FOREIGN KEY (WorkspaceName, ObjectName) REFERENCES orch.ObjectIDs (WorkspaceName, ObjectName)`.

### orch.TaskType

Small lookup table enumerating the valid kinds of Task and whether that kind may run in parallel with others in the same wave.

| Column | Data Type | Purpose |
|---|---|---|
| TaskType | NVARCHAR(50) NOT NULL | **Primary key.** Name of the task type (e.g., `Notebook`). |
| AllowParallel | BIT NOT NULL (default 1) | Whether tasks of this type may execute concurrently with other ready tasks in the same wave, or must run one at a time — checked by `spGetNextWave` to decide the wave's overall execution mode. |

**Current data** (from `orch_metadata.xlsx`, last pulled from the database): one row — `Notebook`, `AllowParallel = True`.

### orch.ObjectIDs

Small lookup table mapping a human-readable (WorkspaceName, ObjectName) pair to the real Fabric GUIDs needed to actually invoke that object via the Fabric REST API. Lets `Jobs` and `Tasks` reference objects by friendly name while still being resolvable to real IDs at runtime.

| Column | Data Type | Purpose |
|---|---|---|
| WorkspaceName | VARCHAR(200) NOT NULL | Part of the composite primary key. Friendly name of the Fabric workspace (e.g., `MDF_CentralHub`). |
| ObjectName | VARCHAR(200) NOT NULL | Part of the composite primary key. Friendly name of the Fabric object within that workspace (e.g., a notebook's name). |
| ObjectID | VARCHAR(200) NOT NULL | The object's actual Fabric item GUID, used by the orchestrator to call the Fabric API directly. |
| WorkspaceID | VARCHAR(200) NULL | The workspace's actual Fabric GUID. |

**Constraints:** `PRIMARY KEY CLUSTERED (WorkspaceName, ObjectName)`.

**Current data** (from `orch_metadata.xlsx`, last pulled from the database): two rows, both in workspace `MDF_CentralHub` — `nb_CopyTableToBronze` and `nb_RefreshObjectIDs`, both still carrying placeholder `ObjectID` values (`PENDING_DEPLOYMENT_REPLACE_ME_...`) pending real deployment IDs.

---

## Schema: `log`

Tables that record *what actually happened* — run history and detailed execution diagnostics. Populated by the `log.spLog*`/`log.spInsert*` stored procedures, not maintained by hand and not part of the Excel sync.

### log.RunLog

The master run-history table — one row per executed task/pipeline run attempt, recording what ran, when, and its outcome. Everything else in the `log` schema hangs off a `RunLog` row via `RunLogId`. Both `orch.spGetNextWave` and `orch.spGetRunStatus` read this table (filtered by `RunID`/`JobName`/`Status`) to determine which tasks have already succeeded and what's left to do.

| Column | Data Type | Purpose |
|---|---|---|
| RunLogId | BIGINT IDENTITY(1,1) NOT NULL | **Primary key.** Auto-incrementing surrogate key for this run-attempt row. |
| RunID | NVARCHAR(200) NOT NULL | Identifier grouping every `RunLog` row belonging to one overall orchestration run (e.g., one execution of a job across all its tasks). |
| JobName | NVARCHAR(200) NULL | Which job this run belongs to. |
| ObjectPath | NVARCHAR(400) NULL | Path/location of the object that ran (e.g., notebook or pipeline path). |
| TaskName | NVARCHAR(200) NULL | Which task this run attempt corresponds to. |
| StartTime | DATETIME2(0) NULL | When this run attempt started. |
| StopTime | DATETIME2(0) NULL | When this run attempt ended. |
| ExitValue | NVARCHAR(MAX) NULL | Raw exit value/output returned by the executed object. |
| ErrorMessage | NVARCHAR(MAX) NULL | Error message text, if the run failed. |
| Status | NVARCHAR(50) NULL | Outcome of the run (e.g., `Succeeded`, `Failed`) — the value `spGetNextWave`/`spGetRunStatus` filter on to compute what's already done. |
| TaskType | NVARCHAR(50) NULL | The type of task that ran, denormalized here for convenience/reporting. |

**Constraints/indexes:** `PRIMARY KEY CLUSTERED (RunLogId)`; nonclustered index on `(RunID, TaskName)`; nonclustered index on `(StartTime)` — both sized for exactly the lookups `spGetNextWave`/`spGetRunStatus` perform.

### log.ActivityRunEvent

Detailed, pipeline-activity-level log capturing the full diagnostic detail of a single Fabric/Data Factory pipeline activity run (inputs, outputs, error detail, retry info) — a richer child record of one `log.RunLog` row. Populated via `spInsertActivityRunEvent` / `spLogActivityRunEvent`.

| Column | Data Type | Purpose |
|---|---|---|
| RunLogId | BIGINT NOT NULL | **Foreign key** to `log.RunLog` — the parent run-attempt this activity detail belongs to. |
| LoggingLevel | TINYINT NOT NULL (default 1) | **Foreign key** to `log.LoggingLevel` — verbosity level this event was logged at. |
| ActivityRunId | NVARCHAR(100) NOT NULL | **Primary key.** Native Fabric/ADF-generated identifier for this specific activity run. |
| PipelineName | NVARCHAR(200) NULL | Name of the pipeline the activity belongs to. |
| PipelineRunId | NVARCHAR(100) NULL | Native pipeline run identifier. |
| ActivityName | NVARCHAR(200) NULL | Name of the activity within the pipeline. |
| ActivityType | NVARCHAR(100) NULL | Type of the activity (e.g., `ExecutePipeline`, `Notebook`). |
| LinkedServiceName | NVARCHAR(200) NULL | Linked service used by the activity, if any. |
| Status | NVARCHAR(50) NULL | Outcome status of this specific activity run. |
| ActivityRunStart | DATETIME2(7) NULL | Precise start timestamp of the activity run. |
| ActivityRunEnd | DATETIME2(7) NULL | Precise end timestamp of the activity run. |
| DurationInMs | INT NULL | Duration of the activity run, in milliseconds. |
| InputJson | NVARCHAR(MAX) NULL | Raw JSON of the activity's inputs, for diagnostics. |
| OutputJson | NVARCHAR(MAX) NULL | Raw JSON of the activity's outputs, for diagnostics. |
| ErrorCode | NVARCHAR(100) NULL | Error code, if the activity failed. |
| ErrorMessage | NVARCHAR(MAX) NULL | Error message text. |
| ErrorFailureType | NVARCHAR(100) NULL | Classification of the failure (e.g., UserError, SystemError). |
| ErrorTarget | NVARCHAR(200) NULL | What component/target the error was attributed to. |
| ErrorDetails | NVARCHAR(MAX) NULL | Extended error detail/stack information. |
| RetryAttempt | INT NULL | Which retry attempt number this run represents. |
| IterationHash | NVARCHAR(200) NULL | Hash identifying a specific iteration (e.g., for a ForEach activity). |
| UserPropertiesJson | NVARCHAR(MAX) NULL | Custom user properties attached to the activity, as JSON. |
| RecoveryStatus | NVARCHAR(50) NULL | Whether/how the activity was recovered after failure. |
| IntegrationRuntimeNames | NVARCHAR(MAX) NULL | Which integration runtime(s) executed the activity. |
| ExecutionDetailsJson | NVARCHAR(MAX) NULL | Full raw execution-detail JSON from the platform. |
| ResourceId | NVARCHAR(1000) NULL | Azure resource ID of the pipeline/factory this activity ran under. |

**Constraints:** `PRIMARY KEY CLUSTERED (ActivityRunId)`; `FOREIGN KEY (LoggingLevel) REFERENCES log.LoggingLevel`; `FOREIGN KEY (RunLogId) REFERENCES log.RunLog`.

### log.JobRunEvent

A run-event record specifically for job-level invocations (as distinct from individual pipeline activities) — tracking a job instance's own lifecycle/status. A one-to-one child record of one `log.RunLog` row. Populated via `spInsertJobRunEvent` / `spLogPipelineRunEvent`.

| Column | Data Type | Purpose |
|---|---|---|
| RunLogId | BIGINT NOT NULL | **Primary key and foreign key** to `log.RunLog` — the parent run-attempt this job event belongs to (one-to-one, since it also serves as this table's own primary key). |
| LoggingLevel | TINYINT NOT NULL (default 1) | **Foreign key** to `log.LoggingLevel` — verbosity level this event was logged at. |
| JobInstanceId | NVARCHAR(100) NULL | Identifier for this specific instance/execution of the job. |
| ItemId | NVARCHAR(100) NULL | Identifier of the Fabric item that was invoked. |
| JobType | NVARCHAR(50) NULL | Type/category of job. |
| InvokeType | NVARCHAR(50) NULL | How the job was invoked (e.g., scheduled, manual, triggered). |
| Status | NVARCHAR(50) NULL | Outcome status of this job run. |
| RootActivityId | NVARCHAR(100) NULL | Identifier of the top-level activity that initiated this job. |
| StartTimeUtc | DATETIME2(7) NULL | Start timestamp (UTC) for the job run. |
| EndTimeUtc | DATETIME2(7) NULL | End timestamp (UTC) for the job run. |
| FailureReason | NVARCHAR(MAX) NULL | Reason for failure, if any. |

**Constraints:** `PRIMARY KEY CLUSTERED (RunLogId)`; `FOREIGN KEY (LoggingLevel) REFERENCES log.LoggingLevel`; `FOREIGN KEY (RunLogId) REFERENCES log.RunLog`.

### log.LoggingLevel

Small lookup table enumerating valid logging verbosity levels, referenced by `orch.Jobs`, `orch.Tasks`, `log.ActivityRunEvent`, and `log.JobRunEvent` to control how much detail gets logged.

| Column | Data Type | Purpose |
|---|---|---|
| LoggingLevel | TINYINT NOT NULL | **Primary key.** Numeric verbosity level. |
| LoggingLevelName | NVARCHAR(20) NOT NULL | Human-readable name for the level. |

**Note:** this table's seed rows (the actual level numbers and names in use) aren't tracked in the git-synced SQL project or in `orch_metadata.xlsx` — they exist only live in the database. Worth a live query if you want the actual defined levels documented here too.

---

## Entity relationships at a glance

- `orch.Jobs` 1—* `orch.Tasks` (a job has many tasks)
- `orch.Tasks.TaskType` → `orch.TaskType` (each task has one type)
- `orch.Tasks`/`orch.Jobs` → `orch.ObjectIDs` (both resolve their Fabric object identity through this table)
- `orch.Jobs`/`orch.Tasks`/`log.ActivityRunEvent`/`log.JobRunEvent` → `log.LoggingLevel` (shared verbosity control)
- `log.RunLog` 1—* `log.ActivityRunEvent`, `log.RunLog` 1—1 `log.JobRunEvent` (both are detail children of a run-history row)

## Not yet included

`orch.JobDependencies` — discussed but not yet created in the live database at the time this dictionary was compiled, so it isn't included above. Re-run this inventory once it exists.

## Verifying against the live database

If you'd like this cross-checked directly against the live schema rather than the git-tracked project files, the fastest path is a short cell run inside `nb_Metadata_Sync.ipynb` (which already has a working non-interactive connection to this exact server) querying `INFORMATION_SCHEMA.COLUMNS` for both schemas — ask and I'll write that cell.
