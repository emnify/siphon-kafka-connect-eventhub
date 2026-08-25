# Code review findings and improvement backlog

Review of the whole connector, 2026-08-25, against `master` at `62b04b7`.

Findings are split into what this branch **fixed** and what is **still open**. Every "fixed" item has a
regression test; the test name is given so the behaviour cannot quietly revert.

Severity: **S1** data loss, security, or a silently wrong result · **S2** outage or a broken feature ·
**S3** correctness or maintenance risk · **S4** polish.

---

## Fixed on this branch

### S1 — JWT authentication never worked

`EventHubClientProvider` selected the auth provider with `authenticationProvider == EventHubSinkConfig.JWT_AUTHENTICATION_PROVIDER`.
Reference equality on a `String` that arrives from a parsed config map is never true — the value is not interned.
Every connector configured with `eventhub.authentication=JWT` silently authenticated with the connection string's
shared access key instead.

Confirmed empirically before the fix: the value equals `"JWT"` and `== "JWT"` is `false`.

*Fixed:* `EventHubSinkConfig.isJwtAuthentication()` uses `equals`.
*Test:* `EventHubSinkConfigTest.jwtAuthenticationIsDetectedByValue`.

### S1 — the connection string was logged in clear text

`log.info("connection string = {}", connectionString)` on every task start wrote the `SharedAccessKey` to the
worker log. Two further sites used `%s` placeholders with SLF4J, which takes `{}` — those did not interpolate, so
they leaked nothing, but they also logged nothing useful.

*Fixed:* the property is `ConfigDef.Type.PASSWORD`, so Connect masks it in the REST API and its own logging; the
connector logs the namespace and Event Hub name only; the broken format strings are gone.
*Test:* `EventHubSinkConfigTest.connectionStringIsMaskedInToString`,
`EventHubConnectionStringTest.errorMessagesNeverEchoTheConnectionString`,
`CustomerFacingErrorContractTest.noDiagnosisEverLeaksACredential`.

### S1 — record values were written into exception messages

`EventDataExtractor` built `"Unable to process record=" + struct.toString()`. Record values routinely carry
personal data, and that message reached the worker log and any dead letter header.

*Fixed:* the message names the topic and schema, never the value.
*Test:* `EventDataExtractorTest.conversionFailuresDoNotEchoTheRecordValue`.

### S2 — every transient failure killed the task

`waitForAllUploads` wrapped everything in `ConnectException`, which Kafka Connect treats as fatal. A single
throttling response or a service-side link detach — the ordinary weather of Event Hubs — failed the task until an
operator restarted it. This is the defect the branch is named for.

*Fixed:* `EventHubErrors` classifies failures and `put()` raises `RetriableException` for transient ones. See
[the two-layer retry model](docs/explanation/retry-model.md).
*Test:* `EventHubErrorsTest` (14 cases), `EventHubSinkTaskTest.transientSendFailuresAskConnectToRetryTheBatch`.

### S2 — misconfiguration was not detected until the first record

The v5 SDK builds a producer client lazily, and the v3 one connected eagerly but reported failures poorly. Either
way a task with a rejected credential could sit in `RUNNING` indefinitely.

*Fixed:* `start()` probes the connection with `createBatch()` and fails immediately, with an actionable message,
on a configuration fault; transient faults are retried for `eventhub.start.timeout`.
*Test:* `EventHubSinkTaskTest.rejectedCredentialsFailFastWithAnActionableMessage` and seven neighbours.

### S2 — customer-facing error messages were being dropped

ecds-api recovers the message shown to a customer from the task's stack trace with a regex requiring
`Caused by: org.apache.kafka.connect.errors.ConnectException:` (`src/client/connect.ts`). Connect wraps `put()`
failures into that shape but records `start()` failures verbatim, so **every startup failure was displayed as
"Error state: Unknown error"** — exactly the failures a customer most needs to see.

*Fixed:* startup failures are re-wrapped so the required `Caused by:` line exists; permanent failures use exactly
`ConnectException`, never a subclass; messages are single-line and prefixed with an `EVENTHUB_*` token following
the `errorToken` convention in ecds-api's `src/common/errors.ts`.
*Test:* `CustomerFacingErrorContractTest`, which applies ecds-api's actual regex to a real rendered stack trace.

### S2 — `Time` logical types crashed on every record

`Time.fromLogical` returns milliseconds since midnight. The converter passed that to `Instant.ofEpochMilli` and
then `LocalDate.from`, which throws `DateTimeException` because an `Instant` has no date fields. Any record with
an `org.apache.kafka.connect.data.Time` field failed the task.

Confirmed empirically before the fix:
`DateTimeException: Unable to obtain LocalDate from TemporalAccessor: 1970-01-01T01:00:00Z`.

*Fixed:* rendered as a time of day via `LocalTime.ofNanoOfDay`.
*Test:* `OutputJsonFormatterTest.timeLogicalTypeSerializesAsATimeOfDay`.

### S2 — one AMQP send per record

`put()` issued a separate `send` per record. A 500-record poll meant 500 round trips.

*Fixed:* records are packed into `EventDataBatch` objects and sent per batch.
*Test:* `EventHubSinkTaskTest.packsAllRecordsOfABatchIntoASingleSend`, `.sealsAFullBatchAndStartsANewOne`.

### S2 — `json.date.pattern` and `json.datetime.pattern` were silently ignored

Both were defined, documented and parsed. `OutputJsonFormatter.configure` had the two assignments commented out,
and the formatters were `static final`. A connector configured with either got the default format and no warning.

*Fixed:* both applied, plus new `json.time.pattern` and `json.timestamp.zone`; an invalid pattern now fails at
startup naming the property.
*Test:* `OutputJsonFormatterTest.configuredDatePatternsAreApplied`, `.invalidPatternIsRejectedAtConfigureTime`.

### S3 — a static executor shared across every connector in the worker

`EventHubClientProvider` held a `static ScheduledThreadPoolExecutor(4)`, shared by every task of every connector
in the JVM and never shut down. Four threads was also an arbitrary cap on AMQP reactor work, and the non-daemon
threads pinned the plugin classloader after a connector was deleted.

*Fixed:* a per-task, daemon, named pool sized to the producer pool, shut down in `stop()`.

### S3 — the first send failure abandoned the rest

`waitForAllUploads` threw on the first failed future, leaving the remaining sends running against producers that
`stop()` was about to close, and reporting whichever failure arrived first.

*Fixed:* all futures are awaited, all failures collected, and a permanent failure always beats a transient one.
*Test:* `EventHubSinkTaskTest.aPermanentFailureWinsOverAConcurrentTransientOne`.

### S3 — `put()` could block indefinitely

There was no overall deadline and no configurable operation timeout. With retries, one `put()` could exceed
`max.poll.interval.ms` and trigger a consumer group rebalance — which makes the next `put()` slower, and is
self-sustaining.

*Fixed:* `eventhub.send.timeout` bounds `put()`, `eventhub.client.try.timeout` bounds each attempt, the defaults
nest inside Kafka's 300s default, and the task warns at startup if a custom config inverts the ordering.
*Test:* `EventHubSinkConfigTest.defaultTimeoutsNest`.

### S3 — `InterruptedException` was swallowed

`findValidRootCause` converted it to an `IOException` without restoring the thread's interrupt flag, so a
shutdown request could be lost. The same method also walked the cause chain with an unguarded
`while (cause != null)`, which loops forever on a self-referencing chain.

*Fixed:* the interrupt flag is restored on every path; the cause walk uses an identity set.
*Test:* `EventHubErrorsTest.cyclicCauseChainTerminates`.

### S3 — `version()` returned `null`

`getPackage().getImplementationVersion()` was null because the build wrote no `Implementation-Version`. The
Connect REST API reported the literal string `null`.

*Fixed:* `maven-jar-plugin` stamps the manifest; `version()` falls back to `"unknown"`.
*Test:* `EventHubSinkConnectorTest.versionIsNeverNull`. Verified in the packaged jar.

### S3 — the plugin would not be discovered by Kafka 4

There was no `META-INF/services/org.apache.kafka.connect.sink.SinkConnector`. Kafka 3.6 warns about
reflection-only discovery; Kafka 4 defaults `plugin.discovery` to `service_load`.

*Fixed:* the service file is present in both packaged artifacts.

### S3 — every task shared one mutable config map

`taskConfigs` added the same `Map` instance `maxTasks` times.

*Fixed:* a defensive copy per task, and the connector's own copy is unmodifiable.
*Test:* `EventHubSinkConnectorTest.eachTaskGetsItsOwnConfigCopy`, `.mutatingTheCallersMapAfterStartDoesNotChangeTaskConfigs`.

### S3 — no configuration validation

`clients.per.task` accepted `0`, producing a queue that blocked forever. `eventhub.authentication` accepted any
string and silently fell back to SAS.

*Fixed:* `Range` and `ValidString` validators throughout, and the connector validates once at create time so the
REST API rejects a typo instead of every task failing.
*Test:* `EventHubSinkConfigTest.invalidEnumeratedValuesAreRejectedAtCreateTime`, `.nonPositiveClientCountIsRejected`.

### S3 — the JWT token path was derived from an SDK-supplied string

`TokenFilesystemProvider.getResourcePath` computed the file path with
`resource.substring(resource.indexOf("/"))`, which produced `//host/entity` and worked only because the path
normaliser collapsed the leading slashes. The v5 SDK passes AAD-style scopes rather than a resource path, so this
derivation would have broken outright.

*Fixed:* the path is derived from the connection string the connector already parses, preserving the same on-disk
layout, and is overridable with `eventhub.auth.token.dir`.
*Test:* `FilesystemTokenCredentialTest.tokenPathFollowsTheNamespaceAndEntityPathLayout`.

### S3 — the token file was re-read on every authentication, on the common pool

`CompletableFuture.supplyAsync` with no executor put blocking file I/O on the JVM-wide `ForkJoinPool.commonPool`.

*Fixed:* the token is cached until five minutes before its `exp` claim, and the read is off the reactor loop.
*Test:* `FilesystemTokenCredentialTest.aValidTokenIsCachedRatherThanRereadPerAuthentication`,
`.aTokenNearingExpiryIsReloadedFromDisk`.

### S3 — `Files.readAllLines(...).get(0)` on an empty token file

Threw `IndexOutOfBoundsException`, which is not retriable and gives no clue what happened.

*Fixed:* reported as an `IOException` naming the file, which classifies as retriable — correct for a sidecar
mid-rotation.
*Test:* `FilesystemTokenCredentialTest.anEmptyTokenFileIsReportedRatherThanReturnedAsAnEmptyToken`.

### S3 — `stop()` did not wait for clients to close

`EventHubClient.close()` is asynchronous; the returned future was discarded.

*Fixed:* `EventHubProducerClient.close()` is synchronous, failures are logged rather than swallowed, and the send
pool is drained first.

### S2 — an end-of-life log4j 1.x dependency, unused

`log4j:apache-log4j-extras:1.2.17` was a `compile` dependency with no reference anywhere in the source. log4j 1.x
has been end-of-life since 2015 and carries CVE-2019-17571 among others. It was being packaged into both
artifacts.

*Fixed:* removed.

### S2 — the retired SDK dragged in a decade-old transitive tree

`com.microsoft.azure:azure-eventhubs:3.3.0` is the final release of a retired artifact. It pulled in Guava
24.1.1, OkHttp 3.12.6, adal4j 1.6.4, gson 2.8.0, json-smart 2.3, Jackson 2.10.5.1 and joda-time 2.9.9 — several
with known CVEs, and Jackson at `compile` scope where it would collide with the worker's own.

*Fixed:* migrated to `com.azure:azure-messaging-eventhubs:5.21.6`. The dependency tree is now six direct entries;
`mvn dependency:tree` shows the whole legacy stack gone.

### S3 — `slf4j-api` was bundled into the shaded artifact

The transitive 1.7.36 copy was packaged, shadowing the worker's own binding.

*Fixed:* declared `provided` and excluded from the SDK. Verified: zero `org/slf4j/*.class` entries in the shaded
jar.

### S3 — broken and deprecated CI

`publish.yml` used `::set-env`, which GitHub disabled in 2020 — the publish workflow could not have worked.
`build.yml`'s branch filter `'*'` does not match `feature/x`. Both used `actions/checkout@v2` and
`actions/setup-java@v1`, on end-of-life Node runtimes. `distributionManagement` and `settings.xml` still pointed
at Bintray, shut down in May 2021.

*Fixed:* modern actions, `GITHUB_ENV`, `'**'` branch filter, plus pull-request builds; the deploy target is now a
`codeartifact.url` property with a matching server id in `settings.xml`.

### S4 — build hygiene

`maven-compiler-plugin` was declared twice with conflicting versions; `maven-assembly-plugin`,
`maven-dependency-plugin` and `maven-surefire-plugin` had no pinned version, so builds were not reproducible; a
`<resources>` block pointed at a directory that did not exist.

*Fixed:* every plugin version pinned as a property, duplicate declaration removed, `maven.compiler.release`
instead of separate `source`/`target`.

### S3 — `Decimal` serialized as base64 with no way out

`{"amount":"BNI="}` rather than `{"amount":12.34}`. This matches Connect's own `JsonConverter` default, so it was
never a regression, but it surprises most consumers and there was no way to ask for anything else.

*Fixed:* `json.decimal.format`, accepting `BASE64` (the default, unchanged behaviour) or `NUMERIC`. Case
insensitive; anything else is rejected at startup rather than on the first record. `NUMERIC` preserves the
schema's scale — a `Decimal(4)` holding `12.3400` emits `12.3400` — and always uses plain notation, so a negative
scale emits `100` and never `1E+2`.

The default stays `BASE64` deliberately: changing it would rewrite the payload shape under every existing
consumer, and a JSON number wide enough to exceed a double loses precision in consumers that parse into
IEEE 754. Consumers that want a number now opt in.

*Tests:* `OutputJsonFormatterTest.decimalIsSerializedAsANumberWhenNumericFormatIsSelected`,
`.decimalFormatIsCaseInsensitive`, `.numericDecimalKeepsPlainNotationForANegativeScale`,
`.numericDecimalPreservesTrailingZeroesFromTheSchemaScale`, `.base64RemainsTheDefaultDecimalFormat`,
`.anUnknownDecimalFormatIsRejectedAtConfigurationTime`.

### S4 — no dependency vulnerability scanning

Nothing failed the build on a known CVE, which is how a decade-old Guava survived in the tree.

*Fixed:* three pieces.

- `.github/dependabot.yml` opens weekly upgrade PRs, grouped so `com.azure:*` moves together — bumping
  `azure-messaging-eventhubs` while its `azure-core` transitives lag is how version skew inside the SDK starts.
  `connect-api` and `connect-json` are ignored on purpose: they are `provided` and must track the *lowest*
  Connect runtime, not the newest release.
- A `security-scan` Maven profile runs `dependency-check-maven`, failing at CVSS ≥ 7.0, with
  `.owasp-suppressions.xml` for findings that are genuinely not exploitable here. Suppressions carry an expiry
  date, so an inherited one eventually fails the build rather than living forever.
- `.github/workflows/security.yml` runs it weekly, on demand, and on PRs that touch the dependency tree —
  uploading SARIF to the Security tab and the HTML report as an artifact.

Deliberately **not** on the default build or the Jenkins pipeline, which is where the original suggestion pointed:
a CVE published overnight would fail a build for reasons unrelated to the commit under test, and every developer
would pay the NVD feed download. A scheduled scan reports the same thing without that coupling.

*Documented:* [Scan dependencies for known CVEs](docs/how-to/scan-dependencies-for-cves.md). There is no
regression test — the finding set changes as CVEs are published, so a pinned assertion would be false precision.

### S2 — customer-facing errors were dropped by ecds-api's extraction *(fixed in ecds-api)*

`_parseException` in `ecds-api/src/client/connect.ts` only recognised
`Caused by: org.apache.kafka.connect.errors.ConnectException:` and the webhook sink's
`kafka.connect.http.sink.errors$*`. Everything else — a `ConfigException`, a `RetriableException`, any subclass,
any framework-level failure — matched nothing, was filtered out, and reached the customer as
`Error state: Unknown error`.

This connector bends to fit that pattern, and `CustomerFacingErrorContractTest` keeps it fitting. But the regex
was the wrong place for the contract, and every other connector in the estate shared the trap.

*Fixed in `ecds-api`, branch `CHL-3023-connect-error-extraction`:* extraction is now three passes, most specific
first, so a trace that already worked keeps producing exactly what it produced before —

1. a `Caused by:` from a connector that follows the message contract (unchanged, greedy `.+` included: for the
   webhook sink that greed is load-bearing, because it skips the retry-wrapper prose and lands on the underlying
   HTTP error);
2. the first `Caused by:` of any type, falling back to the class name when the exception carries no message;
3. the first non-empty line of the trace, with a leading `com.example.SomethingException: ` stripped so it reads
   as prose.

Only an absent or entirely blank trace now yields nothing. Messages are also trimmed — the old `(.*)` capture
began after the colon, so every message was displayed with a leading space.

*Tests* (in `ecds-api`): `should fall back to any Caused by when no connector-specific one matches`,
`should fall back to the first line when there is no Caused by at all`,
`should name the exception type when the cause carries no message`,
`should keep a message that is not a Java class name intact`,
`should drop blank traces but keep the others`.

*Still open there:* the structured half of the original suggestion — surfacing the `EVENTHUB_*` token as a field
on `ConnectorStatusSummary` instead of leaving consumers to parse prose. That changes the shape of a customer-
facing API response, so it needs the ecds-api owners' agreement rather than a drive-by commit.

---

## Breaking change to be aware of

`org.apache.kafka.connect.data.Date` fields used to serialize as an **integer** — days since the epoch — because
the logical converter's result fell through to the `INT32` branch. They now serialize as a formatted date string,
which is what `json.date.pattern` always implied and what `Timestamp` already did.

```diff
- {"createdOn": 18417}
+ {"createdOn": "2020-06-04"}
```

This is a wire-format change. It is called out in [the upgrade guide](docs/how-to/upgrade-from-v3.md); check any
consumer that parses those fields before deploying.

---

## Still open

### S3 — Kafka record headers are not propagated

Headers are dropped. Event Hubs application properties are the natural destination, and carrying
`(topic, partition, offset)` would let a consumer deduplicate — which matters because the connector is
at-least-once.

*Suggested:* `eventhub.headers.propagate` (default `false` to preserve current behaviour), mapping Connect headers
to `EventData.getProperties()`.

### S3 — no proxy configuration

`AMQP_WEB_SOCKETS` works, but `ProxyOptions` is not exposed, so an authenticated proxy cannot be configured
without JVM-wide system properties on the worker.

*Suggested:* `eventhub.proxy.host`, `.port`, `.username`, `.password` (the last as `PASSWORD`).

### S3 — no custom endpoint support

`EventHubClientBuilder.customEndpointAddress` is not exposed, which blocks Azure Private Link and sovereign-cloud
endpoints.

*Suggested:* `eventhub.custom.endpoint`.

### S3 — the connector emits no metrics of its own

Kafka Connect's own sink-task metrics cover throughput and lag, but nothing reports batch fill ratio, retry rate,
dead letter rate, or Event Hubs throttling frequency — the numbers you actually want when tuning.

*Suggested:* a small JMX MBean, or the `azure-core-metrics-opentelemetry` integration.

### S3 — no integration test against a real or emulated Event Hub

All 117 tests are unit tests with mocked producers. The AMQP path, real credentials and real throttling are
unexercised. The Azure Event Hubs emulator (in Docker) or a dedicated test namespace would close this.

*Suggested:* a Testcontainers-based suite behind a Maven profile, so the default build stays offline.

### S3 — timestamps carry no zone offset

The default `ISO_LOCAL_DATE_TIME` emits `2020-06-04T13:36:17.159` with no `Z`. Correct-by-configuration, but a
consumer cannot tell the zone from the value. The default is kept for compatibility.

*Suggested:* document `json.datetime.pattern=yyyy-MM-dd'T'HH:mm:ss.SSS'Z'` more prominently, and consider making
it the default in a future major version. Fractional-second width also varies (`.5` vs `.159`) under the ISO
formats.

### S4 — the package name squats on Microsoft's namespace

`com.microsoft.azure.eventhubs.kafka.connect.sink` is not Microsoft's code, and it split-packages with the SDK
jar. The `groupId` is `org.github.xjrk58`, from the upstream fork.

*Suggested:* rename to `com.emnify.kafka.connect.eventhubs` in a major version. It changes `connector.class` in
every deployed configuration, so it needs coordinating with ecds-api and EMB.

### S4 — `eventhub.serialization` has only one legal value

Retained and validated, but it configures nothing. Either implement a second format (Avro, raw passthrough) or
deprecate the property.

### S4 — the GitHub publish workflow duplicates Jenkins

Jenkins deploys to CodeArtifact on merges to `master`; `publish.yml` deploys on tags to whatever
`codeartifact.url` points at. Two release paths that can disagree.

*Suggested:* pick one. If Jenkins is authoritative, delete `publish.yml` and `settings.xml`.
