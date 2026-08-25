# Record to event mapping

How a `SinkRecord` becomes an Event Hubs `EventData`.

## Record value

| Kafka record value | Event body | Content type |
|---|---|---|
| `null` (tombstone) | *record skipped, nothing sent* | — |
| `byte[]` | the bytes, unchanged | not set |
| `ByteBuffer` | the buffer's bytes, unchanged | not set |
| `String` | UTF-8 bytes | not set |
| `Struct` (has a schema) | JSON, per the rules below | `application/json` |
| `EventData` | forwarded as-is | as supplied |
| anything else | *rejected* — `DataException`, dead lettered | — |

Whether a record arrives as a `Struct` or as `byte[]` is decided by the worker's `value.converter`, not by this
connector. `JsonConverter` with `schemas.enable=true` and `AvroConverter` both produce a `Struct`;
`ByteArrayConverter` produces `byte[]`.

## Record key

The key is only read when `eventhub.partition.key.source=record.key`.

| Kafka record key | Event Hubs partition key |
|---|---|
| `null` | none |
| `""` or `byte[0]` | none — Event Hubs rejects an empty partition key |
| `String` | the string |
| `byte[]` | decoded as UTF-8 |
| anything else | `toString()` |

## Record headers

Kafka record headers are **not** propagated to Event Hubs application properties. See the
[improvement backlog](../../IMPROVEMENTS.md).

## JSON output for schema'd records

The payload only — never Connect's `{"schema": .., "payload": ..}` envelope.

| Connect type | JSON |
|---|---|
| `INT8`, `INT16`, `INT32`, `INT64` | number |
| `FLOAT32`, `FLOAT64` | number |
| `BOOLEAN` | boolean |
| `STRING` | string |
| `BYTES` | base64 string |
| `ARRAY` | array; nulls preserved |
| `MAP` with string keys | object |
| `MAP` with non-string keys | array of `[key, value]` pairs |
| `STRUCT` | object |

### Logical types

| Logical type | JSON | Controlled by |
|---|---|---|
| `org.apache.kafka.connect.data.Decimal` | base64 of the unscaled big-endian bytes, or a number | `json.decimal.format` |
| `org.apache.kafka.connect.data.Date` | `"2020-06-04"` | `json.date.pattern` |
| `org.apache.kafka.connect.data.Time` | `"01:02:03.5"` | `json.time.pattern` |
| `org.apache.kafka.connect.data.Timestamp` | `"2020-06-04T13:36:17.159"` | `json.datetime.pattern`, `json.timestamp.zone` |

Three notes an integrator needs:

- **Timestamps carry no offset.** The default `ISO_LOCAL_DATE_TIME` emits `2020-06-04T13:36:17.159`, rendered in
  `json.timestamp.zone` (UTC by default). A consumer cannot tell the zone from the value. Set
  `json.datetime.pattern` to `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'` if you need it to be explicit.
- **Fractional seconds have variable width.** The ISO formats emit the minimum number of digits, so 500ms renders
  as `.5` and 159ms as `.159`. Configure an explicit pattern if a downstream parser needs fixed precision.
- **`Decimal` is base64 by default, not a number.** `{"amount":"BNI="}` rather than `{"amount":12.34}`. This
  matches Connect's own `JsonConverter` default, but it surprises most consumers. Set
  `json.decimal.format=NUMERIC` to emit a JSON number instead. The default is unchanged because switching it
  would rewrite the payload shape under every existing consumer.

  `NUMERIC` preserves the schema's scale, so a `Decimal(4)` holding `12.3400` emits `12.3400` rather than
  `12.34`, and plain notation is always used — a negative scale emits `100`, never `1E+2`. Note that a JSON
  number wide enough to exceed a double loses precision in consumers that parse into IEEE 754, which is the
  reason base64 is the safe default.

### Nulls

With `json.skip.null=true` (the default) a struct field or map value that is null is omitted from the object
entirely. With `json.skip.null=false` it is emitted as JSON `null`, and a *required* field with a null value is
rejected as a `DataException`.
