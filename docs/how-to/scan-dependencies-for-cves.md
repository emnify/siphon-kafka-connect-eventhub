# Scan dependencies for known CVEs

**Goal:** find out whether anything in the shaded jar has a published vulnerability, before a customer's security
team does.

This connector ships a `jar-with-dependencies`. Everything in the transitive tree ends up inside it, so a CVE in a
dependency is a CVE in this artifact. The previous build carried Guava 24.1.1, OkHttp 3.12.6 and json-smart 2.3
for years — not because anyone decided to, but because nothing was watching.

Two things watch now: Dependabot proposes upgrades, and an OWASP dependency-check scan reports what is currently
vulnerable.

## Run the scan locally

```bash
export NVD_API_KEY=...          # optional, but see below
mvn -B verify -Psecurity-scan
```

The build fails if any dependency has a finding at CVSS **7.0** or above — the floor of "High". Lower-scored
findings appear in the report without blocking. The report is written to:

- `target/dependency-check-report.html` — read this one;
- `target/dependency-check-report.sarif` — what CI uploads to the GitHub Security tab.

Get an API key from [NVD](https://nvd.nist.gov/developers/request-an-api-key). It is free and arrives by email.
Without one the NIST API throttles hard and a cold first run takes upwards of half an hour; with one it is a few
minutes. The feed is cached under `~/.m2/repository/org/owasp/dependency-check-data`, so subsequent runs are fast
either way.

The scan is **not** part of the default build. A CVE published overnight would otherwise fail a build for reasons
that have nothing to do with the commit under test, and every developer would pay the NVD download.

## Where it runs in CI

`.github/workflows/security.yml` runs the scan:

- weekly, Mondays at 04:17 UTC;
- on demand, via **Actions → security → Run workflow**;
- on a pull request that touches `pom.xml`, the suppression file, or the workflow itself.

Findings are uploaded to the repository's **Security → Code scanning** tab, and the HTML report is attached to the
run as an artifact — including when the scan failed, which is exactly when you want it.

Add `NVD_API_KEY` as a repository secret. Without it the workflow still passes, just slowly.

## Suppress a finding

Only when the finding is genuinely not exploitable here — not to make a red build green.

Edit `.owasp-suppressions.xml` and add an entry saying *why*, with an expiry date so it gets revisited rather than
inherited forever:

```xml
<suppress until="2026-12-31Z">
  <notes>CVE-XXXX-YYYY is in the Netty HTTP/2 codec; this connector only speaks AMQP over TLS.</notes>
  <packageUrl regex="true">^pkg:maven/io\.netty/netty\-codec\-http2@.*$</packageUrl>
  <cve>CVE-XXXX-YYYY</cve>
</suppress>
```

dependency-check fails the build once a suppression has expired, which is the point.

A false positive on the *identification* of a library — dependency-check matching the wrong CPE — is worth
reporting upstream as well as suppressing.

There is exactly one suppression in place today: **CVE-2026-33117**, an incorrect authentication-tag comparison in
the Azure SDK's Key Vault Keys library. NVD files it under the blanket `cpe:2.3:a:microsoft:azure_sdk_for_java`
CPE, so dependency-check attributes it to `azure-core`, `azure-core-amqp` and `azure-json` — none of which contain
the flaw, and this connector has no Key Vault dependency at all.

## Analyzers

Sonatype OSS Index is **disabled**. It now answers anonymous callers with `401 Unauthorized`, and analyzer
failures are fatal by design — `failOnError` defaults to true, which is correct, since a scan that silently
skipped half its analyzers is worse than a red build. Using OSS Index properly needs a Sonatype account and
credentials in `settings.xml`; NVD alone is the source until someone decides that is worth doing.

## Dependabot

`.github/dependabot.yml` opens upgrade pull requests weekly, grouped so related artifacts move together:

| Group | Contents | Why grouped |
|---|---|---|
| `azure` | `com.azure:*` | Bumping `azure-messaging-eventhubs` while its `azure-core` transitives lag is how version skew inside the SDK starts. |
| `test-tooling` | JUnit, Mockito | Never shipped; no reason for separate reviews. |
| `maven-plugins` | Maven and Codehaus plugins | Build-only churn. |

`org.apache.kafka:connect-api` and `connect-json` are **ignored on purpose**. They are `provided` — the Connect
runtime supplies them — so they must track the *lowest* runtime the plugin runs on, currently Apache Kafka 3.7.x
from `confluentinc/cp-kafka-connect:7.7.7`. Raising them is a deliberate decision tied to the runtime image, not a
dependency bump. See the comment on `kafka.version` in `pom.xml`.

## What the scan covers, and what it does not

The gate is on **what actually ships in the shaded jar**. Two scopes are excluded:

- `skipTestScope` drops JUnit and Mockito, which never leave the build.
- `skipProvidedScope` drops `connect-api`, `connect-json` and `kafka-clients`. The Connect runtime supplies these,
  and they are pinned to the *lowest* supported runtime on purpose, so we are not allowed to bump them. Gating on
  them would mean a permanently red build with no legal fix.

That exclusion is a reporting gap, not a safety one — those CVEs are real, they just belong to the
`cp-kafka-connect` image rather than to this plugin. Worth looking at when choosing a runtime image:

```bash
mvn -B verify -Psecurity-scan -Ddependency-check.skipProvidedScope=false
```

As of the last run that surfaces four Kafka 3.7.2 advisories, the most serious being CVE-2025-27818 (8.8, an
authenticated operator can point a connector's `sasl.jaas.config` at `LdapLoginModule`) and CVE-2026-35554 (8.7, a
producer buffer-pool race that can silently deliver messages to the wrong topic). Neither is fixable here; both
are arguments for the platform team to move the runtime image forward.

## Check what actually ships

To see the tree the shaded jar is built from:

```bash
mvn -B dependency:tree -Dscope=runtime
unzip -l target/azure-eventhub-connector-*-jar-with-dependencies.jar | grep -c '\.class$'
```

## See also

- [Build and deploy the connector](build-and-deploy.md) — the normal build, without the scan.
- [Improvement backlog](../../IMPROVEMENTS.md)
