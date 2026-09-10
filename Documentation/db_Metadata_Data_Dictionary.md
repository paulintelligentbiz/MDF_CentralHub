# db_Metadata — Data Dictionary

**Database:** `db_Metadata` (Fabric SQL Database, `kalwvg5capkefegg5d6gkpgeza-f62dpkihcteudnn7gmui3r7wb4.database.fabric.microsoft.com,1433`)
**Schemas:** `orch` (job/task orchestration control data — the Wave Runner MDF framework's metadata), `log` (execution run-history and audit tables)

> **Source note:** This dictionary was compiled from the `db_Metadata.SQLDatabase` SQL project's git-tracked table definitions (`orch/Tables/*.sql`, `log/Tables/*.sql`), cross-checked against the current `orch_metadata.xlsx` workbook contents for the `orch` schema's lookup/reference data. Neither of this session's two execution environments (this cloud container, and the Linux shell bridged to this computer) has outbound network access to the Fabric SQL endpoint, so a live `INFORMATION_SCHEMA` query could not be run directly — see the note at the end of this document if you'd like that verified from inside a Fabric notebook, where a working non-interactive connection already exists (`nb_Metadata_Sync.ipynb`).

---

## Schema: `orch`

Tables that define *what should run* — jobs, the tasks that make them up, the dependency graph between them, and the small reference tables that constrain and resolve them. The four core tables (`Jobs`, `Tasks`, `TaskType`, `ObjectIDs`) are the ones kept in sync with `orch_metadata.xlsx` via `nb_Metadata_Sync`; the dependency tables are database-only (not part of the Excel sync).

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
| Dependencies | NVARCHAR(MAX) NULL | JSON array of other `JobName` values this job depends on. (See also `orch.JobDependencies`, below, for the newer relational form of job-to-job precedence.) |
| WorkspaceName | VARCHAR(200) NULL | Fabric workspace this job's own object identity belongs to; paired with `JobName` as a foreign key into `ObjectIDs`. |
| Environment | NVARCHAR(100) NULL | Deployment environment label (e.g., DEV/TEST/PROD) this job definition applies to. |
| LoggingLevel | TINYINT NOT NULL (default 1) | **Foreign key** to `log.LoggingLevel` — controls how verbosely this job's runs are logged. |

**Constraints:** `PRIMARY KEY CLUSTERED (JobName)`; `FOREIGN KEY (LoggingLevel) REFERENCES log.LoggingLevel`; `FOREIGN KEY (WorkspaceName, JobName) REFERENCES orch.ObjectIDs (WorkspaceName, ObjectName)`.

A new lookup procedure, `orch.spGetJob`, returns a Job's full row by `JobName` — `pl_Orchestrator_Top_Level` calls it at the start of a run so it can read `LoggingLevel` and pass it into `log.spLogJobRunEvent`, rather than logging always at the default level.

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
| ParametersJson | NVARCHAR(MAX) NULL | JSON blob of per-task parameters (e.g., source schema/table, destination workspace/lakehouse) passed into the invoked object at runtime — this is what makes each task's source and destination fully data-driven rather than tied to any notebook-level configuration. |
| Dependencies | NVARCHAR(MAX) NULL | JSON array of `TaskName` values that must have already succeeded before this task is eligible to run; consumed by `spGetNextWave` to compute the next wave of ready tasks. (See also `orch.TaskDependencies`, below, for the newer relational form of task-to-task precedence.) |
| TaskType | VARCHAR(50) NOT NULL | **Foreign key** to `orch.TaskType` — the kind of object being invoked (e.g., `Notebook`), which also determines whether tasks of this type may run in parallel. *(Changed from `NVARCHAR(50)` to `VARCHAR(50)` to match `orch.TaskType.TaskType`.)* |
| System | NVARCHAR(100) NULL | Free-text label for the source/target system this task relates to (e.g., `ContosoDW`) — descriptive/organizational, not enforced by a constraint. |
| Layer | NVARCHAR(100) NULL | Free-text label for the data layer this task populates (e.g., `Bronze`) — descriptive/organizational, not enforced by a constraint. |
| LoggingLevel | TINYINT NOT NULL (default 1) | **Foreign key** to `log.LoggingLevel` — controls logging verbosity for this task's runs. |

**Constraints:** `PRIMARY KEY CLUSTERED (TaskName)`; `FOREIGN KEY (JobName) REFERENCES orch.Jobs`; `FOREIGN KEY (LoggingLevel) REFERENCES log.LoggingLevel`; `FOREIGN KEY (TaskType) REFERENCES orch.TaskType`; `FOREIGN KEY (WorkspaceName, JobName) REFERENCES orch.ObjectIDs (WorkspaceName, ObjectName)`; `FOREIGN KEY (WorkspaceName, ObjectName) REFERENCES orch.ObjectIDs (WorkspaceName, ObjectName)`.

`orch.spGetNextWave` already selects each ready task's `LoggingLevel` into `TasksJson`; the two Wave Runner pipelines now forward `item().LoggingLevel` into `pl_Task_Executor` as a pipeline parameter, which passes it into every `log.spLogTaskRunEvent`/`log.spLogActivityRunEvent` call for that task.

### orch.TaskType

Small lookup table enumerating the valid kinds of Task and whether that kind may run in parallel with others in the same wave.

| Column | Data Type | Purpose |
|---|---|---|
| TaskType | VARCHAR(50) NOT NULL | **Primary key.** Name of the task type (e.g., `Notebook`). *(Changed from `NVARCHAR(50)` to `VARCHAR(50)`.)* |
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

### orch.DependencyCondition

Small lookup table enumerating the valid outcome-conditions that a dependency can be gated on — i.e., what state a prerequisite Job/Task must have reached before the dependent one is allowed to run. Referenced by both `orch.JobDependencies` and `orch.TaskDependencies`.

| Column | Data Type | Purpose |
|---|---|---|
| DependencyCondition | VARCHAR(20) NOT NULL | **Primary key.** Name of the allowed condition — `Succeeded`, `Failed`, or `Completed`. |

**Constraints:** `PRIMARY KEY CLUSTERED (DependencyCondition)`.

### orch.JobDependencies

Relational (one-row-per-edge) form of job-to-job precedence — the Data Factory "activity dependency" equivalent at the Job level: which job must reach which condition before a dependent job becomes eligible. This coexists with the older JSON-array `orch.Jobs.Dependencies` column.

| Column | Data Type | Purpose |
|---|---|---|
| JobName | VARCHAR(200) NOT NULL | Part of the composite primary key. The upstream/prerequisite job. |
| DependentJobName | VARCHAR(200) NOT NULL | Part of the composite primary key. The downstream job that waits on `JobName`. |
| DependsOn | VARCHAR(20) NOT NULL | **Foreign key** to `orch.DependencyCondition` — the condition `JobName` must reach (`Succeeded`, `Failed`, or `Completed`) before `DependentJobName` is eligible to run. |

**Constraints:** `PRIMARY KEY CLUSTERED (JobName, DependentJobName)`; `FOREIGN KEY (DependsOn) REFERENCES orch.DependencyCondition`; `FOREIGN KEY (JobName) REFERENCES orch.Jobs`. *(Note: `DependentJobName` does not currently carry its own foreign key to `orch.Jobs` — worth confirming whether that's intentional or an oversight.)*

### orch.TaskDependencies

Relational (one-row-per-edge) form of task-to-task precedence — the task-level counterpart to `orch.JobDependencies`, and the same shape as the older JSON-array `orch.Tasks.Dependencies` column.

| Column | Data Type | Purpose |
|---|---|---|
| TaskName | VARCHAR(200) NOT NULL | Part of the composite primary key. The upstream/prerequisite task. |
| DependentTaskName | VARCHAR(200) NOT NULL | Part of the composite primary key. The downstream task that waits on `TaskName`. |
| DependsOn | VARCHAR(20) NOT NULL | **Foreign key** to `orch.DependencyCondition` — the condition `TaskName` must reach before `DependentTaskName` is eligible to run. |

**Constraints:** `PRIMARY KEY CLUSTERED (TaskName, DependentTaskName)`; `FOREIGN KEY (DependsOn) REFERENCES orch.DependencyCondition`; `FOREIGN KEY (TaskName) REFERENCES orch.Tasks`. *(Same note as `JobDependencies`: `DependentTaskName` has no FK of its own back to `orch.Tasks`.)*

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

Detailed, activity-level log capturing the full diagnostic detail of a single Fabric/Data Factory pipeline activity run (inputs, outputs, error detail, retry info) — a richer child record of one `log.RunLog` row, always logged from within `pl_Task_Executor` (i.e., in the context of one Task's execution). One row per activity attempt: opened by `spLogActivityRunEvent` (start — creates the `RunLog` + `ActivityRunEvent` rows together) and closed by `spInsertActivityRunEvent` (end/failure — `UPDATE`s that same row by `RunLogId`, and adds a closing `log.RunLog` entry). `pl_Task_Executor` now wraps each of the four real `TaskType` branches (`CopyJob`, `Notebook`, `StoredProcedure`, `Dataflow`) individually this way, on both the success and failure path.

> **Fixed:** `spInsertActivityRunEvent` used to `INSERT` a second row reusing the same `RunLogId` the start call had already used — since nothing called it with a real `RunLogId` before, this hadn't surfaced yet, but it would have failed the moment anything did (no unique-per-call `RunLogId` was ever guaranteed). It's now an `UPDATE` keyed on `RunLogId`.

| Column | Data Type | Purpose |
|---|---|---|
| RunLogId | BIGINT NOT NULL | **Foreign key** to `log.RunLog` — the parent run-attempt this activity detail belongs to. |
| LoggingLevel | TINYINT NOT NULL (default 1) | **Foreign key** to `log.LoggingLevel` — verbosity level this event was logged at. |
| ActivityRunId | NVARCHAR(100) NOT NULL | **Primary key.** Native Fabric/ADF-generated identifier for this specific activity run. |
| TaskName | NVARCHAR(200) NULL | *(Renamed from `PipelineName`.)* Name of the task this activity belongs to. |
| TaskInstanceId | NVARCHAR(100) NULL | *(Renamed from `PipelineRunId`.)* Instance/run identifier of the `pl_Task_Executor` pipeline invocation that produced this activity — i.e., this task's own execution instance ID (`@pipeline().RunId` at the task-executor level). |
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

A run-event record specifically for job-level invocations (as distinct from individual pipeline activities) — tracking a job instance's own lifecycle/status. A one-to-one child record of one `log.RunLog` row.

| Column | Data Type | Purpose |
|---|---|---|
| RunLogId | BIGINT NOT NULL | **Primary key and foreign key** to `log.RunLog` — the parent run-attempt this job event belongs to (one-to-one, since it also serves as this table's own primary key). |
| JobName | VARCHAR(200) NOT NULL | **New column. Foreign key** to `orch.Jobs` — which job this run event belongs to. |
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

**Constraints:** `PRIMARY KEY CLUSTERED (RunLogId)`; `FOREIGN KEY (JobName) REFERENCES orch.Jobs`; `FOREIGN KEY (LoggingLevel) REFERENCES log.LoggingLevel`; `FOREIGN KEY (RunLogId) REFERENCES log.RunLog`.

One row per Job run: opened by `spLogJobRunEvent` (start — creates the `RunLog` + `JobRunEvent` rows together, called from `pl_Orchestrator_Top_Level`'s "Log Job Start", now also passed the Job's own `LoggingLevel` via a new `orch.spGetJob` lookup) and closed by `spInsertJobRunEvent` (end/failure — `UPDATE`s that same row by `RunLogId`, and adds a closing `log.RunLog` entry). The orchestrator pipeline now has a genuine failure branch ("Log Job Failure", `dependencyConditions: ["Failed"]` on the task-processing loop) in addition to the success path, so a thrown exception or timeout no longer leaves the row stuck at `Status = 'Running'` forever.

> **Fixed:** `spInsertJobRunEvent` used to `INSERT` a second row with the same `RunLogId` `spLogJobRunEvent` had already used for this table's primary key — an unconditional primary-key violation the moment "Log Job End" ran. It's now an `UPDATE` keyed on `RunLogId` (which also incidentally resolves the earlier `@JobName` gap, since the row's `JobName` is set once at start and never needs to be re-supplied at close).

### log.TaskRunEvent

A run-event record specifically for individual Task invocations — the task-level counterpart to `JobRunEvent`, added when the "Pipeline" naming was retired from these event tables (a `Task` in this framework is executed via a Fabric pipeline, `pl_Task_Executor`, but the tracking table is keyed to the Task, not to "a pipeline" as its own concept). A one-to-one child record of one `log.RunLog` row.

| Column | Data Type | Purpose |
|---|---|---|
| RunLogId | BIGINT NOT NULL | **Primary key and foreign key** to `log.RunLog` — the parent run-attempt this task event belongs to (one-to-one, since it also serves as this table's own primary key). |
| JobName | VARCHAR(200) NOT NULL | **Foreign key** to `orch.Jobs` — which job this task belongs to. |
| TaskName | VARCHAR(200) NOT NULL | **Foreign key** to `orch.Tasks` — which task this run event belongs to. |
| LoggingLevel | TINYINT NOT NULL | **Foreign key** to `log.LoggingLevel` — verbosity level this event was logged at. |
| Status | NVARCHAR(50) NULL | Outcome status of this task run. |
| StartTimeUtc | DATETIME2(7) NULL | Start timestamp (UTC) for the task run. |
| EndTimeUtc | DATETIME2(7) NULL | End timestamp (UTC) for the task run. |

**Constraints:** `PRIMARY KEY CLUSTERED (RunLogId)`; `FOREIGN KEY (JobName) REFERENCES orch.Jobs`; `FOREIGN KEY (TaskName) REFERENCES orch.Tasks`; `FOREIGN KEY (LoggingLevel) REFERENCES log.LoggingLevel`; `FOREIGN KEY (RunLogId) REFERENCES log.RunLog`.

One row per Task invocation: opened by the new `spLogTaskRunEvent` (start — creates the `RunLog` + `TaskRunEvent` rows together, called from `pl_Task_Executor`'s "Log Task Start") and closed by the new `spInsertTaskRunEvent` (end/failure — `UPDATE`s that row by `RunLogId`, and adds a closing `log.RunLog` entry with `Status = 'Succeeded'`/`'Failed'`, which is what `orch.spGetNextWave`/`orch.spGetRunStatus` actually query to detect completion). `pl_Task_Executor` now has both "Log Task Success" and a genuine "Log Task Failure" branch — previously there was no failure path at all here, so a failed task never wrote a `Failed` row to `log.RunLog`, and the orchestrator's `Until` loop would simply spin until its 12-hour timeout.

> **Note:** `JobName`/`TaskName` here are validated against `orch.Jobs`/`orch.Tasks` rather than against `log.JobRunEvent`, since a foreign key needs a unique target and `JobRunEvent` has no unique key on `JobName` alone. `RunLogId` is what actually ties a `TaskRunEvent` row back to its sibling `JobRunEvent` row — both point at the same `log.RunLog` row.

### log.LoggingLevel

Small lookup table enumerating valid logging verbosity levels, referenced by `orch.Jobs`, `orch.Tasks`, `log.ActivityRunEvent`, `log.JobRunEvent`, and `log.TaskRunEvent` to control how much detail gets logged.

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
- `orch.JobDependencies` — edges between `orch.Jobs` rows, each gated on an `orch.DependencyCondition`
- `orch.TaskDependencies` — edges between `orch.Tasks` rows, each gated on an `orch.DependencyCondition`
- `orch.Jobs`/`orch.Tasks`/`log.ActivityRunEvent`/`log.JobRunEvent`/`log.TaskRunEvent` → `log.LoggingLevel` (shared verbosity control)
- `log.JobRunEvent` → `orch.Jobs` (`JobName` foreign key)
- `log.TaskRunEvent` → `orch.Jobs` (`JobName`) and `orch.Tasks` (`TaskName`)
- `log.RunLog` 1—* `log.ActivityRunEvent`, `log.RunLog` 1—1 `log.JobRunEvent`, `log.RunLog` 1—1 `log.TaskRunEvent` (all are detail children of a run-history row)

## Verifying against the live database

If you'd like this cross-checked directly against the live schema rather than the git-tracked project files, the fastest path is a short cell run inside `nb_Metadata_Sync.ipynb` (which already has a working non-interactive connection to this exact server) querying `INFORMATION_SCHEMA.COLUMNS` for both schemas — ask and I'll write that cell.
