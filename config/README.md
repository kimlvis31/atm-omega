# Configuration and terminal operation

The core and all exchange order execution remain **paper only** in this release. GUI installation is optional. The terminal and GUI call the same authenticated local API.

## Observation datasets

The research page can inspect an existing OHLCV dataset, select stored auxiliary
sources, and submit `export_auxiliary`. The owner-only job accepts:

```json
{
  "candle_dataset": "data/example-dataset",
  "selections": [{
    "instrument": "binance:usdm:BTCUSDT",
    "channel": "open_interest",
    "source": "binance:usdm:BTCUSDT:open_interest_history"
  }]
}
```

Optional `as_of_ms` is a non-future availability cutoff; omission uses actual
request time. The worker snapshots candle files and reads selected revisions in
one database snapshot. These two sources are not one atomic historical snapshot.
Limit: eight sources and a total of 10,000 requested minute slots. Source names
must match actual stored collection identities; an empty selection result does
not invent observations. The complete dataset manifest retains the cutoff,
including selected sources with zero observations, under hashed
`export_provenance`. Raw provider payloads are not included by this DB path.

Terminal export uses the same composition, without contacting an exchange:

```powershell
run.bat export --instrument binance:usdm:BTCUSDT --candle-dataset data/example-dataset --auxiliary-source open_interest=binance:usdm:BTCUSDT:open_interest_history --output data/example-observations
```

On Linux use `bash run.sh export`. `--output` must be a new directory in this
mode. Normal CSV and `--raw` JSONL export keep their existing file behavior.
Join original candles and observations before generating joint Monte Carlo
datasets. The NN module owns preprocessing and can read `row.auxiliary`; unchanged
OHLCV datasets still provide ordinary `MarketRow` values. Auxiliary bundles are
retrospective learning inputs, not causal live-market evidence. Selecting or
inspecting data does not automatically start MC or training.

## Committed inference owner API

The initially collapsed NN panel provides a manual CPU workflow over committed
PostgreSQL candles. It remains separate from retrospective shadow and currently
has `strategy_delivery_enabled=false` and `live_orders_enabled=false`.
No model is automatically activated at core startup. External Python still needs
explicit trust; a worker process does not provide a security sandbox.

`causal-inference` in the owner console reads `/api/causal-inference`. All controls
use `request POST /api/causal-inference JSON`; read an operation using
`request GET /api/causal-inference/ID`. The exact envelope is:

```json
{"id":"my-activation-1","kind":"activate","parameters":{
  "core_epoch":"replace-with-current-snapshot-epoch",
  "revision":1,
  "training_job_id":"replace-with-complete-train-job-32hex",
  "deployment_id":null,
  "trusted_module":true,
  "contract":{
    "schema_id":"omega.ohlcv.1m.v1","features":["volume"],"lookback":1,
    "input_shape":[null],"output_shape":[null],"state_shape":null,
    "state_version":null,"allow_no_signal":false
  },
  "bindings":[{"name":"NN.score","path":["values",0]}]
}}
```

Use the actual snapshot epoch/revision and completed job ID. The illustrated
contract belongs to the standard-library demo's single normalized volume input;
confirm the selected module's actual schema, ordered features and shapes.
Arbitrary model paths, caller-provided batches/states/hashes or device selection
are not accepted here. Registered artifacts are checked before activation.

Every operation includes `core_epoch` and the current control `revision`:

| Kind | Additional parameters |
| --- | --- |
| `activate` | `training_job_id`, `deployment_id` (null for new, recorded ID for manual recovery), `trusted_module:true`, `contract`, `bindings` |
| `reset_stream` | `deployment_id`, `stream`, `instrument`, `state_policy` (`reset` or `preserve`) |
| `predict_committed` | `deployment_id`, `stream`, `instrument`, aligned `open_time_ms`, `timeout_ms` (1,000–30,000) |
| `pause` / `retire` | `deployment_id` |

Operation IDs accept 1–96 ASCII letters/digits/underscore/hyphen. A repeated ID
with identical content returns its receipt without rerunning; changed content
conflicts. An unconfirmed response is checked by GET using that original ID.
Admission timeout remains unknown and is never silently executed later.

The current conservative model cutoff includes actual activation verification
time. Wait until a minute beginning after that time has closed before requesting
it. Read-completion time is a conservative input-availability lower bound because
stored reception time alone does not prove commit time. Exported/MC/repaired
datasets cannot substitute for the actual committed inference input.

Normal NoSignal advances the explicitly saved recurrent state once and returns
`complete`, `no_signal:true`, `feature:null`. Expired or failed requests cannot
publish a feature. `preserve` uses only known, actual saved state/cursor; unknown
dispatch or pending outcomes require a fresh reset. Reopening the core does not
automatically execute pending operations, load a model or resume a stream.

Limits: one active deployment, 128 streams, eight worker slots, 32 queued controls,
32 recorded deployments and 256 operation receipts. New operations stop at254,
reserving two slots for pause/retire; IDs are not discarded to make room. This is
a bounded manual-control history. The virtual margin host uses a separate
bounded internal event mailbox for explicitly bound streams; those events do not
consume the manual control revision or256 receipts. GUI display accounts
conservatively for response travel time and hides expired values. Receipt history
is not current eligibility.

## Core input and separate logs

Windows: run `core.bat` (or `core.bat --demo` for explicitly synthetic chart data). Linux: `bash core.sh` / `bash core.sh --demo`. With interactive stdin the core terminal becomes an input/response console. Type `help` or `--help` inside it. `core.bat --help` / `bash core.sh --help` lists startup switches.

Use `--no-console` for an unattended foreground service. Open `console.bat` / `bash console.sh` in another terminal to attach to that core. Exiting the attached console leaves the core running; exiting the console embedded in the foreground core closes that core. Closing the GUI leaves the core running. Existing paper positions/orders are recorded, but local management stops when the core stops.

Logs are written to `logs/<category>/...jsonl`. Open `logs.bat --follow` / `bash logs.sh --follow` in another terminal. `--category system` selects one category; `--lines 30` selects the initial tail length per file (1–200). Ctrl+C stops the log follower. It does not stop the core. The follower reads at most 32 recent files per scan, skips files removed by retention, and waits for a complete JSONL record before printing it.

The startup file is `master.config.toml` in the project root; copy [master.config.example.toml](master.config.example.toml) to create it. Changes to this file take effect on the **next core start**.

```toml
[console]
interactive = true
log_autostart = false

[gui]
autostart = false
single_instance = true

[references]
refresh_leverage_on_start = true
```

`log_autostart=true` opens a separate visible log terminal at startup. Windows uses a new console; Linux requires an available desktop terminal (`x-terminal-emulator` or `gnome-terminal`). In a headless session, start `logs --follow` yourself. `core --logs` requests a log window for that run. `core --console` / `--no-console` override automatic input-console selection; `--no-gui` overrides GUI autostart.

## Console commands

### Full virtual margin ledger

The trade-history page keeps its recent-128 summary and adds a separate full-ledger
tab with 128-row pages, UTC start/end filters and ZIP export. The margin account
page can open that account's history directly. These are owner controls; paired
mobile devices cannot read this endpoint or export financial facts.

Inside the console:

```text
ledger account1 --limit 128
ledger account1 --offset 128 --limit 128 --snapshot TOKEN_FROM_FIRST_PAGE
ledger-export account1 "C:/Exports/account1.zip" --start-ms 0 --end-ms 180000
```

Use `/home/user/account1.zip` on Linux. Output parents must already exist.
The GET path is `/api/margin-trading/accounts/ACCOUNT/ledger`, with optional
`offset` (0..10000), `limit` (1..1000), `snapshot`, `start_ms` and `end_ms`.
Time bounds are integer UTC milliseconds, start inclusive and end exclusive;
rows remain in recorded sequence order. A new query freezes a prefix; subsequent
pages use its opaque snapshot so later appends cannot enter the export. A
different core epoch, source/account lineage, filter or modified prefix requires
a fresh snapshot. Unknown/unavailable state is not an empty ledger.

One atomically published ZIP contains `ledger.csv` and `manifest.json`. The CSV
preserves exact signed amount strings and full typed `fact_json` for fills,
collateral transfers and scenario funding. The manifest records the snapshot,
epoch, filters, row count and CSV SHA-256. A failed/cancelled read preserves an
existing output. Export does not delete facts, reset the account's finite ledger
capacity or create a complete recovery archive. Keep financial source records.

### Reuse registered account settings

The owner console command `margin-configuration ACCOUNT` reads confirmed registered
settings through `/api/margin-trading/accounts/ACCOUNT/configuration`. The response
includes the source account ID, canonical configuration SHA-256 and current core
epoch. An unavailable source has null settings and hash. Paired mobile devices
cannot read this owner endpoint.

This is a read, with no account activation or financial state copy. A new account
requires a distinct ID, explicit starting capital/time, current strategy/model
validation and fresh confirmations through ordinary configuration. Positions,
orders, ledger, model memory and execution progress are not copied. Source settings
and financial records remain intact. A GUI form that cannot represent the complete
source configuration rejects the copy rather than simplifying its tier tables.

### Selected IVP price profile

In Market, refresh database candles, open Settings → Price overlays, enable IVP
and apply the numerical settings. Click a closed candle to pin its interpreted
OHLCV volume profile to the right edge of the price plot. Clicking again clears
the selection. Price pan/zoom and window resizing keep the same price alignment.

An empty origin setting anchors the calculation to the first loaded minute for
that instrument and period. Set an explicit UTC origin or use the origin-reset
button when another history start is intended. Moving the viewport or receiving
later candles does not change a pinned historical calculation. Requests cover at
most 10,000 minutes and 2,048 price bins; seed, missing and zero-volume states are
shown separately. The status tooltip includes the origin, selected cutoff and
source checksum. Apply is temporary; Save explicitly stores the chart profile.

### Temporal probability analysis

Analysis → TPD provides separate `viewLength`, `nSamples`, `nSamplesMA`, interval
and line controls. It preserves Eta's signed return bias, rounded EMA, absolute
EMA and relative value. Despite its historical name, TPD can be negative or above
one; it is not a probability constrained to 0–1. The saved configuration group is
`temporal_probability`, and public outputs are `I_TPD_L_TPD`,
`I_TPD_L_TPDABSMA` and `I_TPD_L_TPDABSMAREL` for interval I and line L. Warmup
values remain unavailable until each calculation has enough closed bars.

### Neural evidence budget

The NN panel displays cached host evidence accounting. Configure limits in the
project-root `master.config.toml`; they take effect on the **next core start**:

```toml
[neural_evidence]
max_bytes = 536870912
max_records = 100000
max_pins = 2048
warning_percent = 80
```

Native integer bounds: bytes 4194304..8589934592, records 16..1000000,
pins 1..65536, warning percentage 1..99. These counters cover charged causal
input/proof records, including uncertain reservations; they do not measure free
disk space, all prediction files, training artifacts or total project size.
Each new reservation needs conservative 2 MiB headroom. Consumption receipts
have an additional `max_pins * 2048` bytes and `max_pins` records allowance.
Existing evidence and pin receipts are never automatically deleted.

`ready`, `warning`, `full` and `unavailable` distinguish confirmed accounting
from unknown state. Warnings are deduplicated per core run. Displayed admission
flags are advisory sufficient-headroom estimates, not financial authorization;
the owning operation still checks its actual capacity. Reducing a budget may
block new evidence while preserving readable existing proofs. An existing
registry exceeding the absolute limits including receipt allowance, or the new
pin limit, cannot reopen; restore adequate startup limits rather than deleting
proofs. Budget settings cannot be hot-patched through runtime settings.

| Input | Behavior |
|---|---|
| `help` / `--help` | List commands without connecting to a service |
| `status` | Read core, accounts, strategy, feed, incidents and reference status |
| `settings` | Read runtime values and their current revision |
| `set notifications.speech true` | Change one runtime preference |
| `set notifications.speech_language "ko_KR"` | Save the speech language; JSON strings need double quotes |
| `pause` | Stop subsequent new entries; keep already submitted orders |
| `resume` | Request paper resume with the current epoch, revision and a short expiry; safety gates still apply |
| `modules` / `jobs` / `database` / `analysis` | Read module/job/database/analysis-library data |
| `operation <id>` | Query a command whose outcome is unknown; do not blindly resubmit it |
| `command <kind> <JSON object>` | Submit an owner command with a generated unique operation ID |
| `request GET /api/<path>` | Query any owner API workflow |
| `request POST /api/<path> <JSON object>` | Submit a database action or research job through its dedicated API |
| `exit` | Leave the current console |

For scripts, use `run.bat console exec --command "status"` / `bash run.sh console exec --command "status"`. The console prints an operation ID **before** sending a control command. A network timeout is an unknown outcome, not a failed operation that is safe to repeat. It never automatically retries writes. Changes with an outdated settings/library revision are rejected; refresh and inspect the current values before submitting a new change.

Examples typed **inside** the console:

```text
command paper.account.create {"account_id":"paper1","balance":"10000"}
command paper.strategy.add {"instance":"qty1","account_id":"paper1","module_id":"target_quantity","instrument":"binance:spot:BTCUSDT","budget":"1000","configuration":{"quantity":"0.001"}}
command paper.feed.configure {"enabled":true,"start_ms":<latest closed minute open in ms>,"warmup_minutes":240}
request GET /api/paper/feed
request GET /api/modules
request GET /api/analysis-library
request GET /api/reference/leverage
command notifications.speech.probe {}
command notifications.speech.preview {}
```

Replace the angle-bracket timestamp with an actual UTC-aligned integer. Enabling the DB feed does not enable its collector or resume paused entries. Missing/old data and incomplete analysis preparation block new entries. Historical priming never creates fills or entries; separate candle processing keeps existing positions protected during unfinished short warmup. Manual `paper.step` and automatic feed cannot supply the portfolio simultaneously. Data-only readiness recovery does not clear a user's pause or restart recovery requirement.

Use `request POST /api/database/actions` with `{id,kind,parameters}` for `catalog.refresh`, `collection.set`, `gaps.refetch` and `peer.import`. Use `request POST /api/jobs` with `{kind,parameters}` for `export`, `monte_carlo`, `train`, `simulate` or `nn_inspect`. Inspect job status with `/api/jobs/<id>` and open its recorded result file externally. These API paths provide collection, account/strategy configuration, analysis-library edits and research without a GUI. Desktop charts and graphical viewers are optional ways to inspect the same stored data.

An unconfirmed queued-job cancellation keeps its queue reservation until the
original record is confirmed. At the queue limit, new work is refused rather
than forgetting the uncertain cancellation. A schedule whose stop record cannot
be saved continues to report an unconfirmed shutdown; reopening the program is
a recovery boundary, not confirmation that the earlier save succeeded.

## Runtime settings

Runtime preferences are stored in `state/settings.json`. Change them through the console or GUI, rather than editing a file while its core is running. Every update supplies the revision obtained from `settings`. `set` fetches that revision immediately before the patch; competing updates are rejected.

| Field | Default | Supported values / effect |
|---|---|---|
| `logging.level` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`; diagnostic filtering |
| `logging.retention_days` | `30` | Integer 1–3650; cleanup owns diagnostic category JSONL files |
| `logging.max_file_bytes` | `5000000` | Integer 4096–100000000; diagnostic file rotation |
| `notifications.sound` | `true` | Boolean; core beep for new/revised incidents and critical repeats |
| `notifications.repeat_critical_s` | `30` | Integer 5–3600; unacknowledged critical repeat interval |
| `notifications.speech` | `false` | Boolean; optional core-owned speech worker, independent of GUI lifetime |
| `notifications.speech_language` | `auto` | `auto`, `ko_KR`, `en_US`; an installed matching voice is required |
| `notifications.speech_voice` | `""` | Empty for automatic selection, or the installed voice ID from the probe (max 256 characters) |
| `notifications.speech_rate` | `0` | Finite number −1..1 |
| `notifications.speech_volume` | `0.8` | Finite number 0..1; 0 is muted |
| `training.recording` | `standard` | `light`, `standard`, `detailed`; defaults for subsequently prepared jobs |
| `training.optional_recording_failure` | `continue` | `continue` or `stop`; selected optional detail recording failure policy |
| `simulation.backend` | `auto` | `auto`, `cpu`, `gpu`; GPU is currently an explicit CPU fallback with a recorded reason |
| `simulation.intrabar` | `interpolation` | `conservative`, `interpolation`, `random`; verify the chosen job's explicit path/spec before submission |
| `simulation.seed` | `0` | Integer 0..2^63−1; subsequent job default |
| `execution.mode` | `static` | `static` validates known venue rules and reports unchecked dynamic rules; `strict` blocks new risk when required dynamic references or filter support are missing |
| `execution.max_age_ms` | `60000` | Integer 1–86400000; maximum age of freshly observed symbol rules for new risk |
| `execution.apply_to_manual` | `false` | Apply venue-rule checks to manually supplied paper candles too; DB-fed paper already requests venue rules |
| `analysis.input_storage_mb` | `1024` | Integer 16–16384; decimal MB quota for durable CA input segments; lowering it blocks new records when full, without deleting referenced inputs |

New jobs record their explicit parameters; changing a default does not mutate a running or finished job. The current desktop copies supported device/seed/recording defaults into its draft; a job's explicit specification is authoritative. `simulation.intrabar` is retained as a preference but does not replace a job's explicit `path`/case routes automatically. Conservative ambiguity resolution remains unavailable where the simulator reports it unsupported.

Settings → Runtime provides typed forms for logging, training, simulation, execution and analysis storage, alongside the advanced JSON editor. Save only changed fields against the displayed revision. A changed core or an uncertain write requires an explicit reload or operation lookup before retrying.

Venue-rule fetches run on the market worker; paper callbacks only read the in-memory cache. Missing, future or stale rules block new risk. Known permitted reductions can use recorded rules, with their unchecked conditions reported. Accepted pending orders retain their original rule snapshot. These checks do not turn the leverage-1 full-fill paper model into live trading or a liquidation model.

Independent CA execution records native source revisions/checksums and the actual knowledge cutoff in `state/analysis-inputs/`. Historical warmup is explicitly retrospective; later observations are never presented as data known at their event time. Interrupted chunks replay their frozen inputs before advancing the checkpoint. The same local database destination is required. A missing source pin, changed destination or unavailable input blocks progress. Storage quota exhaustion does not automatically delete analysis history.

For terminal auxiliary workflows, use `auxiliary` to list jobs and `request GET /api/auxiliary-jobs/JOB_ID` to inspect one. Submit a bounded job with `request POST /api/auxiliary-jobs` and an explicit `{id,kind,parameters}` JSON object. Supported kinds are `trade_flow`, `current_oi`, `history_oi`, `long_short_ratio`, and explicit `trade_flow_repair`. A repair's parameters are `instrument`, `parent_job_id`, `parent_execution_id`, `open_time_ms`, plus optional `limit` (1–1000, default 500) and `max_pages` (1–20, default 20). The parent must be the current partial trade-flow job and the chosen minute must have a committed partial/missing pin. The worker checks the original pin again before repair and never rebases it automatically. Resume/cancel use `request POST /api/auxiliary-jobs/JOB_ID/resume` or `/cancel` with `{execution_id: "CURRENT_EXECUTION_ID"}` (quote JSON keys). After an uncertain submission, query the same job ID before making another request.

The GUI's **Settings → Notifications / speech** provides forms, engine/voice discovery, saved-setting preview and unavailable/error status. Speech needs optional Qt components and an OS speech engine even for a headless core; use `setup --speech` (an alias of `--gui`) to install the Qt dependency group. This does not start the desktop. No network speech service is used. Preview deliberately requests a short generic spoken notification; save settings before previewing. Missing engines/languages and engine failures are shown, never treated as successful speech. Selecting speech off cancels the owned worker and queued speech. An OS engine's `Speaking` state does not prove audible output on the user's speakers.

Python wheels do not install the operating system's speech/audio libraries or voices. A minimal Linux desktop/container can therefore run the GUI while speech remains unavailable. The optional speech worker reports this condition; injected test engines do not require those native speech libraries.

## Bootstrap and DB boundaries

`master.config.toml` also validates `runtime`, `resources`, `safety` and `paths` from the example. `spawn` is the supported process method and safety policies cannot be weakened. Some bootstrap resource/path/timeout values remain design parameters and are not wired to every worker; validation alone does not mean they control that component. Actual fixed worker bounds are enforced in their implementations. `gui.single_instance` remains enforced even if that bootstrap value is changed.

DB settings live separately in `database.config.toml`; see [database.config.example.toml](database.config.example.toml). They specify the PostgreSQL host/port/database/user and a local password file. Compose uses the matching local credentials/volume configuration. Restart the relevant collector/core after DB connection changes. Do not paste API keys or passwords into runtime settings. Market storage is PostgreSQL/TimescaleDB; the runtime JSON files store configuration and control state.

## Continuous public auxiliary input collection

Use Database → continuous auxiliary collection (initially collapsed), or the
console's `auxiliary-schedule` command. Preview strategy demand or add the selected
symbol to a draft, then explicitly Apply it. Spot supports trade flow; USD-M also
supports native open-interest history and long/short samples. Desired subscriptions
are distinct from observed coverage and numerical readiness. A restarted core
retains desired symbols but requires explicit reapplication; it makes no startup
auxiliary requests on their behalf.

Owner API: `GET /api/auxiliary-schedule`; `POST /api/auxiliary-schedule/configure`
with `{"expected_revision":0,"subscriptions":[{"instrument":"binance:usdm:LTCUSDT","channels":["trade_flow","metric"]}]}`.
Alternatively send `{"expected_revision":0,"from_strategies":true}` after reviewing
the current demand. Optional `live_start_ms` must be an aligned minute within the
last ten minutes. Stop with `POST /api/auxiliary-schedule/stop` and
`{"expected_generation":CURRENT_GENERATION}`. An uncertain native task resumes
with `POST /api/auxiliary-schedule/resume` and `instrument`, `channel` and
`expected_generation`; native channel names are `trade_flow`,
`open_interest_history`, and `long_short_ratio`. Replace the placeholders with
integers from a fresh GET. Query after an unconfirmed POST; never repost blindly.
These routes are unavailable to registered mobile devices.

The worker reuses public request budgets, raw archives, fixed-window journals and
transactional PostgreSQL writes. Network cooldowns use bounded retries. Storage
uncertainty preserves the original response for explicit recovery. Native sample
publication has a finite grace period; missing data is not a zero observation.
This first scheduler has a total 10,000 child-journal limit and retains its source
records rather than silently deleting them; reaching capacity blocks further
collection. This scheduler handles trade flow and native statistics; the separate
Depth panel/API owns explicit live-book collection. Automatic historical repair
is not provided by either subscription path.

DB-fed paper strategies use `state/paper-inputs` independently of CA's
`state/analysis-inputs`. The configured analysis storage quota applies to each
consumer namespace; it is not a global disk ceiling. A paper state commit retains
the consumed input head together with the feature/model checkpoints and candle
cursor. Late revisions do not change previously consumed inputs. Auxiliary gaps
gate new risk while existing pending fills and available candle protection keep
running. Query-only data faults can recover on subsequent minutes; missing/corrupt
consumed references or unconfirmed input recording require explicit recovery.
An unfinished short warmup does not hold an existing position's price clock.
Cold symbols join the common frame only after current history preparation and
source-anchor validation. Long warmup still requires a flat, paused portfolio.

Paper decisions made while catching up never fill in a minute predating their
actual decision time. If a decision occurs after a minute has opened, the first
full eligible minute is later. Limit touches retain conservative close timing.

## Leverage and maintenance-margin reference

The startup refresh uses a separate account-scoped reference service. The generator's `/fapi/v1/leverageBracket` is a signed **USER_DATA** read endpoint ([Binance account API](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/account)). It is not an anonymous market-data request, and brackets/coefficients can be account-specific.

Current core construction has **no authorized account provider**. Startup therefore reports `auth_required` without using API keys or contacting a private endpoint. The read-only adapter and validated atomic cache are implemented and tested with synthetic responses; actual authenticated refresh has not been run. Existing cache age and refresh outcome are separate fields. Old generator files are not automatically treated as current or valid for an account. This reference service does not activate trading. Existing v1 paper accounts remain at leverage 1; the separate opt-in margin v2 host uses explicitly confirmed manual bracket assumptions and does not claim an authenticated exchange bracket.

## Manual leveraged virtual scenarios (experimental)

`run.bat margin --help` / `bash run.sh margin --help` opens a separate local
USDM cross/isolated full-fill scenario console. Register the supplied synthetic
example once with `run.bat margin --create config/examples/margin-account.json`.
Inside that session, enter:

```text
resume
submit config/examples/margin-order.json
pause
step config/examples/margin-frame.json
status
exit
```

The previously submitted order fills even after entry pause. This example charges
0.1 USDT, leaves the cross wallet at 899.9 USDT and the separate isolated wallet
at 100 USDT. It is synthetic scenario data, not a market-price example. Reopen
with `run.bat margin --account margin-demo`; every new session starts paused.
Do not recreate a used account: a different initial state or existing ledger is
rejected. A missing registered wallet/journal is an error and never a fresh balance.

Use `help` for status, explicit resume/pause, cancel, submit, step and reconcile.
`--execute "COMMAND"` can be repeated for a bounded noninteractive session. A
failed or uncertain operation is not automatically resent; inspect status and
explicitly reconcile its original frozen facts. Two processes cannot own the
same scenario account. The account and immutable primary/fallback journal live
in separate `state/margin-v2*` namespaces, and are independent of v1 paper state
and market PostgreSQL data.

These scenario inputs supply their own marks, bracket tiers and candles. The
console labels them `manual_scenario`; venue constraints and actual exchange mark
prices are not verified. It does not run TS modules or collect DB data, and adds
no real-order, funding, liquidation or partial-fill execution. The GUI's existing
v1 paper accounts still use leverage 1. The separate Virtual margin v2 page and
owner API can run TS modules against committed DB candles, as described below;
the manual scenario console itself does not. Read the example JSON files before using
your own scenario; amounts are exact decimal strings, times are integer
milliseconds, unknown keys/duplicate keys/nonfinite values are rejected.

## Recording and worker recovery

The state API distinguishes user/control pause, paper pause, data readiness and recording health. `core.effective_entries_allowed` reports the global entry gate; it does not guarantee that an individual strategy has completed warmup or has sufficient budget. A paper recording fault emits a critical incident and requires a fresh, explicit resume even after writes recover. An older resume request cannot clear that requirement. A data-source-only outage retains its separate automatic recovery policy.

If a research child remains alive after cancellation/termination deadlines, the research service reports an unknown computation and stops accepting jobs. `state/research-unconfirmed.json` preserves a quarantine across core restarts; it is not an instruction to kill a process by its recorded PID. Existing job/artifact files are not automatically reconciled while quarantined. The rest of the core can remain available. After independently confirming that the old worker has stopped, preserve the marker and related records for inspection, then perform explicit maintenance before enabling research again. Do not remove the marker while a worker may still be writing. If the marker itself cannot be saved, persistence of this quarantine across a process restart cannot be guaranteed.

## Strategy checkpoints, charts and optional neural examples

`command paper.strategy.recover {"instance":"tef1"}` restores a compatible saved calculation checkpoint and pauses new entries. Pending paper orders must be explicitly settled or cancelled first. A stateful checkpoint behind portfolio history is refused. `command paper.strategy.recover {"instance":"tef1","reset_model":true}` explicitly discards calculation/stop-latch memory, preserving positions, fills and cash; it also pauses entries. Prepare analysis again before resuming. The Trading page supplies these controls with selection validation and confirmation for reset. Private runtime checkpoints are excluded from ordinary strategy-detail responses.

Chart **Apply** changes the current session; **Save** records named profiles and the comparative workspace. Changing label timezone leaves UTC aggregation unchanged. Indicator calculation and visibility are separate switches.

The three SI slots can now select from VOL, MFI, MMACD and DMIxADX. DMI starts disabled; choose its sample count and display type (`DMIxADX`, `ABSMA`, or `ABSMAREL`). Switching only display type does not recalculate or fetch data. DMI uses the core's common producer against a bounded committed database range, for installed fixed periods from 1m through 1d. It is unavailable for synthetic/manual charts and 3d/week/month periods. Missing history and warmup remain empty values. The result is explicitly a retrospective display calculation, with the input cutoff and actual production time; it does not claim those results were known at each historical close.

Old chart profiles are migrated in memory to version 3 with their existing three slots and settings preserved; PSAR/SWING price overlays default to off. The workspace file is written only on explicit Save. Closing the core expires display jobs; a new connection rejects old-epoch replies. Cache eviction clears an expired result and offers explicit recalculation.

For terminal access, submit `request POST /api/chart-series` with `{id,parameters}` and query `request GET /api/chart-series/JOB_ID`. Parameters are `instrument`, `start_ms`, `end_ms`, `period_minutes`, `kind: "DMIxADX"` and `nSamples` (2–2000). Quote all JSON keys. The range contains at most 10,000 closed source minutes; the worker reads it in 512-minute chunks and resets after gaps. Queue capacity is eight, result cache is bounded, and the latest 128 request identities are retained within the current core lifetime. After an uncertain write, query its ID rather than reposting it.

Install optional CPU NN dependencies with `setup.bat --gui --neural-cpu` or `bash setup.sh --gui --neural-cpu`. Choose PyTorch MLP/GRU on the NN page, select a dataset and CPU, then start training. For a completed run, select **Continue this training**, review the new draft and submit separately. Epochs are the total target, not an increment. Resume requires the same frozen dataset/splits, seed, module and compatible configuration. If Monte Carlo produced the original training dataset, resume uses that generated dataset and disables regeneration. Original run files and checkpoints remain unchanged. No live trading deployment occurs.


## Experimental CPU NN shadow inference

Training completion does not activate a model. The core offers an owner-only
Paper-shadow workflow through the Neural Network page's collapsed deployment
panel, the console's `shadow-models` command and `/api/shadow-models`.
Predictions are recorded for inspection and never become TS orders. Training,
candidate verification and activation are separate explicit actions.

`GET /api/shadow-models` reads cached controller/candidate/operation state.
Submit `POST /api/shadow-models` with `{"id":"UNIQUE_OPERATION_ID","kind":"KIND","parameters":{...}}`
and inspect `GET /api/shadow-models/UNIQUE_OPERATION_ID`. Successful submission
queues work; wait for that operation's `completed` state. `unknown`, `failed`
and `expired` are not successful predictions. An identical operation ID/payload
queries the original operation; changing its payload is rejected. Do not blindly
resubmit an unknown operation under another ID.

| Kind | Parameters |
| :--- | :--- |
| `inspect_training` | `training_job_id` |
| `prepare_fixture` | `training_job_id`, `contract`, `fixture_batch`, `trusted_module:true`; optional `fixture_expected` |
| `activate` | `candidate_id`, `expected_revision` |
| `reset_stream` | `stream`, `instrument`, `reason`, `expected_revision` |
| `resume` | `expected_revision` |
| `pause` | `expected_revision`, `reason` |
| `predict` | `expected_revision`, `stream`, `instrument`, `sequence`, `open_time_ms`, `as_of_ms`, `deadline_ms`, `input_digest`, `batch`, `schema_id`, `features`, `lookback` |
| `predict_raw` | Exactly `expected_revision`, `stream`, `instrument`, `sequence`, `open_time_ms`, `deadline_ms`, `dataset`, `retrospective_confirmed:true` |
| `refresh` | Empty object |

Take `expected_revision` from a fresh GET's `deployment.document.revision`.
The training job must be complete and belong to this core's research root.
Preparation freezes its module/checkpoint and runs the explicit fixture in the
existing independent `nn_predict` research child. Only a verified candidate can
be explicitly activated. Start a stream with an explicit reset before predicting.
For the built-in **linear demo only**, a compatible contract is:

```json
{
  "schema_id": "omega.ohlcv.1m.v1",
  "features": ["normalized_volume"],
  "lookback": 1,
  "input_shape": [null],
  "output_shape": [null],
  "state_shape": null,
  "state_version": null,
  "allow_no_signal": false
}
```

Its fixture can be `[0.25]` and a later prediction batch `[0.5]`. Other modules,
including the PyTorch example, need their own actual preprocessing/schema/shape
contract. This is not an automatic converter for arbitrary trained models.
`trusted_module:true` explicitly permits the already frozen Python module to run
in the research child; it is not an operating-system security sandbox.

Prediction times are integer UTC milliseconds. Use a closed minute, actual input
availability no later than the current time, a future deadline within 60 seconds,
the exact feature order/lookback, and a SHA-256 source-input digest. The host uses
its actual clock, validates output/state shape and finite values, and rejects
duplicate, late or wrong-epoch results. RNN state advances only with an accepted
request; reset is explicit. Numeric `predict` accepts supplied batches. The
retrospective raw path below prepares a frozen OHLCV window; neither path
automatically binds live database features or trading strategies.

Candidate hashing/copying and artifact checks run in a dedicated bounded worker;
GETs do not load a model or read files. Model code executes only in research
children. Queue capacity is eight and operation history is 128, with no silent
deletion of referenced artifacts. Checkpoints are bounded to 512MiB. Missing or
damaged metadata is preserved and blocks that shadow namespace. A restarted core
keeps the candidate but pauses shadow inference and invalidates old stream work;
reset/resume is manual. Historical operation responses retain
`operation_core_epoch` separately from the current HTTP `core_epoch`.

This path is CPU-only and does not provide a resident inference pool, automatic
GPU/CPU migration, Triton acceleration, online-learning deployment or live orders.
The ordinary paused paper/data core can remain available if this optional
namespace cannot start. Do not treat a shadow prediction as an entry permission.

### Retrospective raw-data shadow input

`predict_raw` is owner-only, explicit retrospective research. First train a module
with the optional `prepare_inference(rows, request, context)` capability, verify
its fixture, activate that candidate, reset the instrument's stream and resume
the deployment. Demo and PyTorch module version 1.1 implement this hook using
restored training preprocessing, without refitting or calling the training
transform that constructs targets. An older frozen run is unchanged; train a
new run with the new module to gain the capability. Arbitrary external modules
must implement their own compatible hook; prepared numeric `predict` remains
available for modules without it. Only run Python modules you trust.

Submit the normal owner operation envelope with `kind:"predict_raw"`. Its
`parameters` object must contain exactly these eight fields:

| Field | Meaning |
| :--- | :--- |
| `expected_revision` | Current deployment revision from a fresh GET |
| `stream` | The initialized stream's original name |
| `instrument` | Exact canonical instrument, for example `binance:usdm:BTCUSDT` |
| `sequence` | Next sequence after the last accepted prediction |
| `open_time_ms` | UTC-aligned opening time of the final closed one-minute bar |
| `deadline_ms` | Future actual UTC deadline, at most 60 seconds from submission |
| `dataset` | Local normalized dataset directory or its manifest path |
| `retrospective_confirmed` | Must be `true`; acknowledges the retrospective-only scope |

Select an unrepaired normalized OHLCV dataset. The host takes exactly the active
contract's contiguous lookback ending at that minute, entirely within one frozen
train/validation/test split. After an accepted prediction, the reference must
advance by exactly one minute. Gaps, duplicate rows and future/unclosed input fail;
no missing row or future target is added. Monte Carlo/generated and all repaired
datasets are unsupported in this raw-shadow path, including causal interpolation.
Their existing training/replay features are unchanged.

Do not supply `batch`, `as_of_ms` or hashes to `predict_raw`. The host records the
dataset manifest, rows, selected window, batch, source/checkpoint and prepared
artifact identities automatically. The historical `reference_time_ms` is the
last bar close; `snapshot_available_at_ms`/`as_of_ms` use the actual preparation
snapshot time. Current datasets have no per-row historical availability proof.
This is not a causal historical backtest or a live-market feed.

Preparation (`nn_prepare`) and prediction run in separate research children;
their common original deadline, candidate/stream epochs and state identity must
still match before acceptance. Preparation alone advances no stream cursor or
RNN state. Inspect the same operation ID after an unknown acknowledgement; do not
automatically submit another ID. Restart retains records but pauses the stream
and retires old work: inspect status, explicitly reset/resume and choose a new
request. No TS entry permission, resident inference, GPU/Triton acceleration or
online-learning deployment is provided by this path.

External modules may declare a fixed `describe()["input"]["raw_lookback"]`
(a native integer from1 to4096) independently of the prepared model tensor's
`lookback`. The module's `prepare_inference` hook can aggregate those raw minutes
or flatten them; the host does not implement that transformation. The selected
window and `used_rows` must identify all requested raw rows in order, while the
prepared batch must match the separate model contract. Without this declaration,
raw length defaults to the model lookback. Old records retain their original
fields and hashes. Configuration-dependent raw window declarations are not yet
supported.

Raw preparation accepts at most100000 dataset rows,32MiB per dataset source/rows
file, and256KiB per complete prepared record/file including its selected window.
Module source snapshots for inspect/train/predict are limited to1MiB; completed
run metadata and resume/prediction checkpoints are limited to2MiB and512MiB.
Snapshot reads require regular files with no linked/reparse path components,
use bounded streams and verify hashes before/after exclusive copies. Existing
artifacts are preserved. Unsupported size/path inputs fail explicitly; these
limits are not a guarantee that every model fits the operating PC's memory.

## Extended virtual trading and data controls

### Automatic virtual margin v2 sessions

Open **가상 증거금 v2 / Virtual margin v2** in the extra navigation menu. The
original nine dashboard icons and v1 accounts remain available. Create a separate
virtual wallet, add strategy rows, choose cross/isolated collateral, leverage,
budget, quantity rules and the TS configuration. The single-tier bracket form is
an explicitly confirmed **manual assumption**, with its own UTC observation time;
it is not an account-specific exchange bracket. Creating an account does not
resume it. Global entry control and local account resume remain separate.

This host reads committed whole-account 1m frames, evaluates the registered TS
and settles previously submitted orders at their first eligible future bar.
It preserves pending orders during entry pause. Freshness/graph failures limit
new risk while the available pending-order/FSL clock continues. Restart pauses
and does not automatically fetch or trade. Known open positions/pending orders
on restart generate a critical incident; explicitly restore the feed and resume.
Recording conflicts require explicit reconciliation and then a separate resume.

The default model uses OHLCV proxy marks, full fills, zero spread/slippage and
configured quote fees. Scenario funding and the separately versioned execution
cost policy below are optional.
Automatic v2 sessions support committed `trade_flow` for NES-dependent TS modules
with an independent `evaluate_protection` method. Selected native observation pins,
availability cutoff and database identity stay fixed across retries and restarts.
Missing or partial input restricts new entries; accepted orders and independent
local protection continue. OHLCV-only sessions keep their existing behavior.
V2 depth/metric inputs, WOI/MST/STS graphs and auxiliary input for shared margin
research replay remain unsupported. Actual venue funding payments, liquidation,
hedge and live orders are not implemented. The v1 auxiliary-aware paper path is
separate. No v1 balances are converted or discarded.

Console `margin-trading` reads cached state. Owner API `GET /api/margin-trading`
returns the current `core_epoch`; submit `POST /api/margin-trading` with
`{id, core_epoch, kind, parameters}` and inspect `GET /api/margin-trading/ID`.
`configure` parameters are `account` (the margin-account example schema),
`strategies` (rows with `id,instrument,module,configuration,analysis,constraints`),
`start_ms`, `brackets` (the margin-frame example schema) and optional `fee_rate`.
Accounts using external inputs may also set `external_feature_wait_ms` (native
integer0–30000, default15000). The structured NN wait control records this in the
account draft; configurations without external inputs do not add the field.
Other kinds are `pause`, `resume`, `reconcile` with `{account_id}`, and
`update_brackets` with `{account_id,brackets}`. Old-core requests cannot resume a
new core; old operation IDs remain lookup-only/idempotent after restart.

`resume_strategy` takes `{account_id,strategy_id}` and explicitly releases the
selected TS module's local stop latch. The account is durably paused first and
stays paused; use a separate account resume when ready. Global core pause remains
independent. Accepted orders are preserved, including an in-progress protective
reduction. Pending/unconfirmed host frames, recording conflicts and shutdown
recovery block this operation. The GUI exposes a separate strategy selector and
button, enabled only when `capabilities.module_local_resume` is true. An uncertain
reply is queried with the original operation ID, without resubmitting the callback.
This control does not repair failed models or manufacture missing inputs.

One core supports at most10 such virtual wallets and100 strategy rows in total;
this bound is not a long-duration/heavy-model throughput guarantee. Queue/history
limits are8/128 with no silent deletion. Existing wallet facts, intent journal,
TS checkpoints and frozen host frames use separate `state/margin-*` paths.
Status includes the latest128 ledger rows and an explicit `summary.ledger_total`;
that partial display envelope is rejected by the authoritative financial decoder.
Durable full facts are retained. A TS used here must implement detached finite
JSON `checkpoint()` and exact `restore(checkpoint)` in addition to the ordinary
TS interface; missing/inconsistent capability is rejected before wallet creation.

### Scenario funding in virtual margin and shared replay

Virtual margin v2 configuration and the `simulate_margin` research job accept an
optional `funding` object. Omit the field to retain the existing no-funding model.
The structured forms provide a separate, initially disabled funding area with an
explicit JSON selection and read-only preview. They require the core's exact
`minute_boundary_scenario_rate_v1` capability; an older core is not assumed to
understand the option. Loading or previewing a file does not start a trade or job.

The [example schedule](examples/margin-funding.json) contains historical synthetic
minute timestamps and reference prices. Adjust them to the configured account,
instruments and dataset; it is not a current-market rate file. The selected file
itself is never rewritten. A schedule has `schema_version: 1`,
`model: "linear_usdt_scenario_rate_v1"` and ordered, unique `points` containing:

- `instrument`: configured Binance linear USDT USD-M instrument key.
- `event_time_ms`: exact UTC minute settlement boundary, at or after session start.
- `source_available_at_ms`: when this scenario source is known, from settlement
  through the end of that same minute, inclusive. Later sources are rejected by
  this first model rather than silently treated as zero.
- `reference_time_ms`: exactly the settlement boundary. Supply that reference
  price explicitly; the host does not substitute a later candle high/low/close.
- `rate` and `reference_price`: finite decimal strings; rate is a signed fraction
  within -1..1, and price is positive. `"0.0001"` is 0.01 percent.

These are declared scenario assumptions. Using a published market rate does not
establish a real account payment or an independently verified settlement price.
Cashflow is `-signed_quantity × reference_price × rate`, without leverage
division, using the pinned USDT 1e-8/ROUND_HALF_EVEN policy. The previous minute's
close precedes funding, then the current minute's open phase runs. Positions
created at that current open do not pay the preceding boundary's funding.

The source availability is retained separately from the account-effective frozen
cutoff. Retrying or restarting does not assign a new timestamp, quantity or rate.
Zero cashflow at an explicitly scheduled boundary is a recorded fact; an absent
schedule point produces no funding fact. Cross and isolated wallets remain
separate; funding can create debt and never clamps cash or automatically tops up
isolated collateral. Existing accepted orders keep their original identity and
eligibility; actual fill admission uses the updated confirmed account.

History and shared-replay results distinguish received, paid and zero funding.
`funding_net` is signed cashflow, separate from trade realized PnL and fees; it is
not the older unleveraged model's positive funding-cost field. Request JSON with
decimal strings and normalized schedule/checksums are retained separately. This
snapshot does not recreate the selected file's original whitespace or wire bytes.

The parser supports at most 10000 points, with additional existing request/storage
limits: margin operation parameters 131072 bytes and HTTP 262144 bytes. Those limits
are not raised by selecting funding. Oversized or incompatible input is rejected
while preserving the existing draft. The core does not auto-fetch future rates,
replace a running schedule or migrate an existing account to a different policy.
Shared replay also requires settlements inside its selected dataset range.

This adds no actual exchange-payment collection, liquidation, spread/slippage,
partial fills or execution latency to margin v2. A confirmed fallback ledger
blocks new risk while retaining supported protection. If both record paths leave
funding unconfirmed, the original pending fact is preserved for reconciliation;
subsequent fills cannot be claimed safe without a confirmed financial prefix.

### Typed committed NN inputs in virtual strategies

Activate a trusted completed model and explicitly reset its instrument-bound
stream on the Neural Network page. In Virtual margin v2, select `neural_signal`,
its numeric threshold/target quantity/local close-stop settings, and a matching
registered input. The demo's deadband holds the observed position; it does not
set a zero target. The model's Python module owns preprocessing and network state.
This is a demonstration signal policy, not evidence of profitable trading.

Owner `GET /api/external-feature-bindings` lists exact cached binding documents.
Add selected documents as an opt-in strategy row's `external_features` list (up to
eight). The current GUI selects one exact instrument/field-set binding; advanced
API configurations can supply several nonoverlapping bindings. Do not invent or
edit binding hashes. Core configuration checks the registered producer/contract
and exact field ownership. Existing rows without external inputs retain their
original data format. Module manifests may annotate configuration properties
with `x-omega-external-feature: {namespace: "NN", required: true}` for generic
structured GUI selection, without loading TS Python in the desktop process.

A bound TS must implement `evaluate_protection(context)` independently of normal
external signal evaluation. It may hold, cancel an owned pending order, or reduce
the observed position. The host disables entries in this path and rejects any
expanding proposal. Exact candle content, source/model/stream epochs, numerical
fields, availability and TTL are frozen before a decision. A durable financial
pin and a short current-authority lease are required before first dispatch of
new risk. Missing/expired/NoSignal inputs do not invalidate accepted original
orders or local protection. Late results cannot rewrite consumed financial bars.

The model cutoff includes activation verification time: start with a newly
eligible minute after that cutoff. Core restart does not reactivate a model or
stream. Current margin configurations fix their producer/contract bindings;
hot model rebinding into an existing wallet is not implemented. Read-only shadow
or retrospective MC outputs cannot substitute for committed causal evidence.

The host offers exact committed candles through a bounded FIFO of128 streams,
with one event per stream and up to8 active worker requests. A busy stream must
finish before its next minute can be admitted; there is no hidden RNN input drop
or parallel mutation of one stream. Per-request inference is bounded at30seconds.
Financial processing uses the configured bounded grace outside its mutation
fence. Before waiting, a detached module protection preview checks the actual
account/candle context without advancing strategy/analysis state or saving files.
Pending orders/frames, shutdown settlement, uncertain state, or a needed local
reduction/cancellation bypass that wait. Safe existing positions may wait for NN
input; a model with no independent preview capability takes the conservative
immediate path. An unusable result at the cutoff freezes a protection-only frame.
This does not guarantee throughput for100 heavy NN strategies; overload, stale
queued data or a lost stream sequence remain explicit recovery conditions.

Current host-managed evidence/input limits are512MiB,100000 immutable records
and2048 active financial pins. These are fixed initial bounds, separate from
training artifacts, model checkpoints and arbitrary writes by trusted Python.
Consumed receipts release live authorization without deleting audit evidence.
TTL expiry is never deletion permission, and automatic evidence GC/archival is
not implemented. Quota exhaustion blocks new inference; preserve/archive the
original evidence before changing operating capacity. Current bindings and
accepted financial orders are not silently discarded to make room.

### Explicit live depth input

Database → the initially collapsed **호가 / Depth** panel provides an explicit
subscription draft, start, stop, status and uncertain-state resume. Price/quote
precision inputs are manual assumptions unless checked against provider metadata;
confirm them explicitly. A requested stop is not confirmed until `running=false`
and resource cleanup has no error. Restart never silently reconnects.

The worker synchronizes public Spot/USD-M snapshots and diff updates, preserves
raw messages before publication and stores a latest-snapshot minute profile
`binance_depth_ws_v1`. It does not average or sum book snapshots. Depending on
provider-clock alignment, the first minute can be partial or missing. Stream gaps,
insufficient ±5% price-range coverage and unavailable bins also remain
partial/missing. They cannot produce a ready WOI/STS signal. Historical
depth is not manufactured, interpolated or automatically repaired. Strategies and
independent analysis can consume fixed depth revisions alongside their other
required inputs; already consumed input manifests do not change after repair.

Console `depth` reads status. Owner `GET /api/depth` supplies `core_epoch` and
`generation`; `POST /api/depth` requires
`{id,core_epoch,expected_generation,kind,parameters}`. `start` parameters are
`{subscriptions:[{instrument,price_precision,quote_precision}],resume:false}`;
use `resume:true` only for explicit recovery. `stop` parameters are `{}`. Query
the same command at `/api/depth/ID` after uncertain delivery; do not create a new
request automatically. Default bounds are8 symbols,64MiB raw bytes,8192 raw
references and10000 minute records per configured worker policy. Quota exhaustion
stops that work rather than silently deleting originals or claiming continuity.

### Deployment controls and recorded chart/NN views

The Neural Network page now includes a collapsed **NN 후보·배포·Shadow 추론**
panel. Select a completed training job, explicitly inspect/apply its actual
contract, confirm shapes and trust, supply prepared numeric input, verify the
fixture, select the verified candidate and activate it. Activation is separate
from training completion. `inspect_training` is an asynchronous shadow operation
with `{training_job_id}`; it reads bounded metadata on the worker and does not
load a checkpoint/model in the GUI or shared HTTP lock. External/GRU shapes not
declared by a machine-readable contract need explicit confirmation. Prepared
numeric input and the **원시 데이터 → 회고 추론 / Raw data / retrospective inference**
tab are separate choices. The latter
uses a compatible frozen module's inference hook and the restrictions above;
this retrospective route does not provide automatic TS entry, resident inference,
GPU/Triton or online deployment. The separate committed-input owner and explicit
virtual bindings above provide the bounded CPU strategy path.

Chart settings provide default-off line-0 PSAR dashes and confirmed SWING price
overlays. SWING uses only pivots confirmed by the fixed selected candle's close and
clears when selection is cleared; gaps are not connected. They share core
producers with analysis. Profile v3 adds typed overlay settings; v1/v2 migration
is in memory until explicit save. `/api/chart-series` accepts `PSAR` with
`start,acceleration,maximum,price_precision`, or `SWING` with `swing_range`, in
addition to the existing range/instrument/period/kind fields. Its result remains
retrospective display data with actual availability and gap segments.

The NN3D artifact viewer shows recorded edges only, a bounded structure-step
selector and node metadata. It does not invent adjacent-layer connections or
render full weight/activation tensors. Large artifact files retain the viewer's
explicit size limit; use exported run records for more detailed external tools.

## Fixed history scans and staged warmup

Run `run.bat backfill --help` / `bash run.sh backfill --help` for public REST history scans. Always specify `--root` and `--db-config`, then either a market/symbol and aware UTC range or `--resume <plan_id>`. `--to-utc` is exclusive; both boundaries must be exact minutes. No core or account connection is required. Checkpoints live under `state/backfill/`, use destination identity without a password, and retain the fixed range and page size. Repeating the same range resumes it; completed plans retain any gaps and need an explicit separate repair. This tool is not an Eta DB migration.

`paper.feed.configure` accepts `warmup_minutes` 1–525600, within an aligned one-year maximum. More than 2000 minutes requires the whole portfolio paused, flat and without pending orders. Progress is checkpointed in 512-minute chunks; `command paper.feed.continue {}` continues the fixed plan after restart/manual strategy recovery. It does not pick a newer target. A new configure creates a new plan. Incomplete indicator/model preparation remains blocked; no warmup orders or automatic user-pause clearing occur. A strategy with existing model state is not historically replayed into that state; reset/replay is explicit when exact continuation is impossible.

The GUI's local `state/gui.preferences.json` stores `font: {mode, family, path, size_px}` separately from core runtime settings. Modes are `default`, `installed`, and `file`; body size is 10–18 pixels. Use Settings → GUI font / TTF and its explicit Apply/Reset controls. Missing files fall back visibly while preserving the original preference for repair. TTF/OTF registration is process-local; speech settings above control TTS independently.

Legacy TEF model checkpoints (v1) did not record evidence of consumed closed-interval anchors. Their trackers are preserved, but a model needing those anchors waits for new valid interval closes before ordinary model-based entry, resize or reversal decisions. This can last until the next daily close for BSCALPHA. The strategy detail reports `model_history_ready=false` with a warning; a manual resume does not manufacture readiness. Configured local FSL protection remains independent and active within the available paper state. An FSL that was not configured is not added automatically. For a flat portfolio, explicitly reset and prepare a sufficient observed history to avoid waiting; do not replay history into an existing held model or discard its protection latches. Models without required `CLOSED_i` anchors do not acquire this additional wait. New v2 checkpoints retain the consumed-anchor evidence.

### Device state and installable browser shell

Paired `GET /api/state` requires the current server-side `read` permission.
The additive `margin_portfolio` field has `schema_version: 1` and
`scope: "virtual_margin_v2"`. Its bounded accounts include cached signed positions,
consumed `cursor_ms`, account entry pause/readiness and pending-order counts.
`positions: null`, `state: "unavailable"`, or `summary.complete: false` cannot
establish a flat account. These are virtual cached ledgers, not venue queries or
current price/PnL estimates. V1 `accounts` are separate; unconfirmed v1 financial
state also uses unknown positions. Devices cannot access owner margin endpoints,
ledgers, session configuration, model bindings or arbitrary filesystem paths.
Current device permissions cover the whole core, not a per-account allowlist.

`pause` blocks future entries while retaining submitted orders. Global `resume`
does not restore v2 account/session/feed/recovery gates. The browser disables
resume until a fresh permitted observation; after an uncertain command it only
queries the original operation ID and never resubmits automatically.

The `/mobile/` manifest/service worker support an installable public shell in
compatible secure contexts. Only exact public static GET paths enter its cache.
API responses, bearer credentials, pairing codes and commands are not cached;
there is no offline command queue or background sync. Host/Origin restrictions
and the `127.0.0.1` bind remain in place. A physical phone cannot directly reach
this loopback server over LAN. The static shell alone does not provision remote
HTTPS/private-network access, native mobile packages or push notifications;
separate opt-in HTTPS-origin and Web Push adapters are available.

`mobile.https_origin` defaults to empty (non-loopback access disabled). In Runtime
settings or the core console, use `set mobile.https_origin "https://omega.private.example"`
with your exact trusted private HTTPS proxy origin; configure TLS, private-network
access and its local upstream separately. Pair the device using an owner-issued
code; authentication and device permissions still apply. With the optional `push`
extra installed in the Core environment, explicitly start with
`--web-push --push-subject mailto:owner@example.com`, replacing the example with your contact.
The sender defaults OFF; the paired browser must separately allow and register
notifications. Local VAPID/subscription credentials under `secrets/` stay out of
Git and shared diagnostics. `accepted_by_service` means HTTP201 acceptance, not
phone arrival or acknowledgment. Physical TLS/Android/iOS/external connectivity
and actual delivery remain unverified; no native package or Omega-hosted relay
is supplied. See the [README mobile guidance](../README.md#core-capabilities).

### Shared margin execution cost policy

The optional `cost_policy` document in [margin-cost-policy.json](examples/margin-cost-policy.json)
selects `linear_usdt_ohlcv_cost_v1` for both virtual margin `configure.parameters`
and shared replay job parameters. Keep the key absent for the existing strict
model; an explicitly all-zero policy still selects the versioned cost model.
Funding and the separate `fee_rate` remain independent settings. Existing
accounts/configurations cannot be silently replaced or converted.

Every policy field in the example is required. Rates are bounded decimal strings
with a leading digit, such as `"0.001"`; floats/NaN and boolean delays are rejected.
Slippage is in [0,0.25], execution/mark bounds in [0,0.5], and both integer delays
in [0,86400000] milliseconds. `limit_price_policy` is `adverse_then_cap_v1`.
Derived prices outside the current18-digit/exponent range reject before a
financial intent is stored; this model does not silently round to a venue tick.

The original observed mark stays unchanged. Entries reserve the declared
execution/mark envelope, including maximum valuation, fees and opening loss,
before intent acknowledgement. Pre-fill checks use current confirmed collateral
and the original bounds; they never enlarge an existing request. Funding or a
tier change can still prevent a new entry. An out-of-band quote is not permission
to increase reserve automatically. Confirmed reductions retain their separate
known-position protections rather than acquiring an entry-bound restriction.

BUY uses raw price*(1+slippage), SELL raw price*(1-slippage). LIMIT fills are capped
to their original limit after the existing touch/phase check; slippage cannot
manufacture a touch. Final price and its once-rounded fee are recorded in the
ordinary FillEvent. Quote summaries report raw/final prices and an explanatory
slippage amount; that amount is **not another wallet debit**. A scenario price
may lie outside the candle's OHLC and is not presented as an observed venue fill.

Eligibility is fixed once as the first minute boundary at or after original
issue+selected delay. Even1ms after an exact boundary can shift activation to the
next minute. Entry and reducing orders have separate delay settings; the example
keeps reducing delay0. A nonzero reducing delay also delays a protective/flatten
fill, while protection decisions still run. Entry pause retains submitted orders.
End-of-data keeps remaining pending orders/positions instead of forcing a close.

Results preserve raw/canonical policy identities and only confirmed applied quote
records. Missing/unknown quote evidence fails explicit cost reporting. Raw
policy edits require a new configuration/run; restart never reprices an accepted
intent. This is a full-fill OHLCV scenario model, without actual venue latency,
partial fills, impact, liquidation or GPU execution.

### Standalone SMA, WMA and Bollinger analysis

Saved analysis templates accept `window_averages` definitions with
`analysis_code: "SMA"` or `"WMA"`, and `bollinger` definitions with
`MAType: "SMA"`, `"WMA"` or `"EMA"`. Each row requires `interval_id` 0–11
(fixed minute intervals 1m through 1d). Defaults are `nSamples: 10`,
`line_index: 0`, `price_precision: 8`, and Bollinger `bandWidth: 2.0`.
Sample counts are 2–20000; line indices 0–9; price precision 0–16.
There are at most 64 analysis definitions; both price-window groups share
one 100000-entry retained-window budget including their bounded history.

These producers use the observed closed 1m source and shared higher-bar
aggregation. An incomplete initial higher-bar prefix cannot seed an
indicator. A current higher-bar preview does not advance the committed
calculation until that bar closes. Gaps require repair/replay; a held
legacy value is not manufactured as a ready observation.

Eta arithmetic is retained: SMA/WMA use their sliding sums, and Bollinger
uses deviation around the selected rounded moving average with divisor N.
A finite signed or zero `bandWidth` is accepted. Negative widths preserve
the historical named fields even when `BOLLOW` exceeds `BOLHIGH`; zero
width produces equal bands. Existing EMA/MMACD definitions retain their
previous configuration and checkpoint meanings. Structured analysis
settings expose the new definitions; chart overlays are a separate feature.

### Retiring a flat virtual margin account

Retirement is an explicit, permanent end to one virtual margin account lineage.
First pause the account and confirm that its positions, pending orders and staged
decisions are empty. The owner rechecks the actual pause flag and confirmed
source/strategy state; a cached empty table or a data-failure pause is insufficient.
Retirement establishes its own host barrier and disables that account's feed.
It never cancels orders, flattens positions, transfers balances, forgives debt or
resumes a strategy. Existing balances and the complete original ledger remain
preserved. Retired account IDs cannot be reused; copying settings requires a new
ID, new capital/start settings and fresh consent.

The GUI's account controls include separate Active/Retired selection, an explicit
retirement inspection/confirmation, original-request queries and archive loading.
A first successful native close followed by a missing acknowledgement remains
unconfirmed until an explicit retry. The original request ID and observation hash
are retained; a new request is not submitted automatically. After a core restart,
valid fenced retirement is recovered before any financial Session opens. The GUI
can correct a pending result from a fresh matching retired receipt; a retirement
still pending under a different core epoch requires the explicit console workflow.

Headless commands are also available through the core input terminal or
`console.bat exec --command "..."` / `bash console.sh exec --command "..."`:

```text
margin-retirement ACCOUNT
margin-retire ACCOUNT OBSERVATION_SHA --confirm --id retire-001
request GET /api/margin-trading/retire-001
margin-history ACCOUNT RECEIPT_SHA --id read-001
margin-history-status read-001
ledger ACCOUNT
margin-configuration ACCOUNT
```

Use the observation hash from the current inspection. Retry an uncertain retirement
only with its original ID and parameters, after inspecting its current owner state.
Across a core restart the explicit console retry uses the current epoch wrapper
while preserving the original operation and fence. Ordinary ledger/configuration
GETs never open archive files: a cold or evicted archive returns unavailable until
an explicit history-load job completes and reports `cached: true`.

At most one retirement transaction is in flight. Retiring accounts continue to
count toward the 10 active accounts and 100 active strategy rows until the verified
retired receipt and registry update are acknowledged. Up to 128 retired/retiring
reservations and the existing 128 durable control receipts are retained without
automatic deletion. Archive documents are bounded to 16 MB and compact plans to
1 MB; oversized or inconsistent data is rejected before closing the original owner.
The archive reader has one worker, at most eight ephemeral read jobs and one cached
immutable account. Eviction removes only memory; original financial files and
retirement receipts remain on disk. A load job is read-only and cannot reactivate
an account. Initial metadata reports no guessed wallet amount or implied transfer.

### Execute only a strategy's required analysis

Virtual margin v2 and shared-margin research drafts provide **필수 지표만 실행 /
Execute required indicators only** for each strategy. The checkbox starts OFF.
Apply a validated typed saved-CA profile and connect to a core advertising this
feature before enabling it. The complete CA remains in the library, copied
strategy and recorded configuration. Independent Analysis-page evaluation keeps
the complete profile. Collection subscriptions continue serving their independently
selected consumers.

For a configuration file or console submission, add `analysis_execution` to the
existing strategy object with this exact value, retaining its other fields:

```json
{"version": 1, "policy": "required_fields"}
```

This explicit option compiles the TS module's declared required outputs into real
indicator/line and closed-bar dependencies. For example, MMACD can run without
querying unused MST/OI/long-short or WOI/depth inputs in the copied full profile.
EMA and MMACD components are selected independently; other selected lines retain
their shared computation kernels. A required MST line keeps its own sample-age
policy, including zero meaning disabled. Required missing/stale inputs still gate
new entries, while accepted orders and local protection retain their existing
execution and risk rules.

The policy requires a typed profile such as `moving_averages`/`market_sentiment`,
with all definitions still validated. Existing shorthand analysis settings keep
their previous behavior without the option. Registered NN-only consumers may use
explicit known empty groups after external-input ownership is verified. The option
does not register a provider or authorize an NN delivery; missing/foreign bindings
are rejected before financial creation.

Existing sessions keep their original settings and checkpoint codec. The new plan,
full canonical profile identity and actual TS descriptor are frozen in the opted-in
session configuration/hash. Changing the policy, full CA or module contract rejects
adoption of an existing checkpoint. Apply the choice in an explicit new session
configuration; the full saved CA remains available for review.

한국어: 가상 증거금과 공통 증거금 연구의 전략별 **필수 지표만 실행**은 기본 꺼짐입니다.
지원 코어에 연결하고 검증한 typed CA를 적용한 뒤 선택합니다. 전체 CA와 수집 구독은
보존하며, 해당 전략이 요구하는 실제 계산·라인·봉 의존성만 실행합니다. 모든 CA 설정의
검증과 NN 입력 권한 확인은 유지합니다. 기존 세션은 기존 설정으로 유지되며 변경한
정책·CA·전략 계약으로 기존 체크포인트를 열면 거절합니다.



### Local database target and port

For a fresh installation, choose another local port with `run.bat db init --port 54328` or `bash run.sh db init --port 54328`. `db configure --port 54328` prepares the files without starting Docker. Initialization uses the port and database name in `database.config.toml` for both Compose and the application connection. Windows may reserve the default port; choose an available port explicitly.

An existing different port is never overwritten by `--port`. To change it, stop the Core, collectors and database, edit `port` in `database.config.toml`, then run `db init` again. Existing credentials and the persistent volume are preserved; changing a port does not reset stored data.

