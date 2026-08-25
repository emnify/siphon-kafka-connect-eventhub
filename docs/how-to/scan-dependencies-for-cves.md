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

## Check what actually ships

The scan covers compile and runtime scope; `skipTestScope` drops JUnit and Mockito, which never leave the build.
To see the tree the shaded jar is built from:

```bash
mvn -B dependency:tree -Dscope=runtime
unzip -l target/azure-eventhub-connector-*-jar-with-dependencies.jar | grep -c '\.class$'
```

## See also

- [Build and deploy the connector](build-and-deploy.md) — the normal build, without the scan.
- [Improvement backlog](../../IMPROVEMENTS.md)
