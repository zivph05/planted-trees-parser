# Planted Trees Parser

A lightweight service that:

1. **consumes** delimited text lines from **Kafka or RabbitMQ**  
2. **parses** each line into a typed JSON object according to a column schema  
3. **publishes** the resulting JSON to **RabbitMQ**

The service is **fully configured by a single YAML file**.  
The only runtime override is the optional environment variable **`CONFIG_PATH`**,
which lets you point to a non-default YAML location.


```bash
export CONFIG_PATH=/etc/planted/parser-prod.yaml
python -m planted_trees_parser
```

If `CONFIG_PATH` is unset the parser loads `config/parser-config.yaml`.

---

## 1 - Configuration file reference

Below is a *minimal* but exhaustive skeleton that lists every accepted key.
Delete the parts you don’t need and fill in real values.

```yaml
# config/parser-config.yaml   ← default path
input:                        # REQUIRED ─ pick exactly one block
  # ─────────────────── Kafka ───────────────────
  type: kafka
  brokers: ["localhost:9092"]   # at least one host:port
  topic: planted-trees.raw
  group_id: trees-parser        # optional (random UUID if omitted)
  auto_offset_reset: earliest   # earliest | latest (default: earliest)
  commit_interval_ms: 5000      # ms between auto-commits
  # user: my_kafka_user          # OPTIONAL (SASL/PLAIN)
  # password: my_kafka_pass

  # ──────────────── RabbitMQ (alternative) ────────────────
  # type: rabbitmq
  # host: rabbitmq
  # port: 5672
  # queue: trees.raw
  # prefetch_count: 200          # optional
  # user: guest                  # optional
  # password: guest

parser:                       # REQUIRED
  delimiter: "|"              # single character (default: "|")
  parse_to: json              # (currently only "json")

fields:                       # REQUIRED – order MUST match incoming line
  - name: tree_id
    type: int
  - name: species
    type: str
  - name: planted
    type: datetime
    format: "%Y-%m-%d"        # REQUIRED when type == datetime
  # Supported types: str | int | float | bool | datetime

output:                       # REQUIRED – only RabbitMQ for now
  type: rabbitmq
  host: rabbitmq
  port: 5672
  exchange: trees.parsed
  routing_key: trees
  user: guest                 # optional
  password: guest

logging:                      # OPTIONAL – defaults shown
  level: INFO                 # DEBUG | INFO | WARN | ERROR
````

### 1.1 - Key-by-key explanation

| YAML path                                       | Required   | Description                                                              |
|-------------------------------------------------|------------|--------------------------------------------------------------------------|
| **`input`**                                     | ✅          | Where the parser receives the messages                                   |
| `input.type`                                    | ✅          | `"kafka"` or `"rabbitmq"` – selects the block that must be present.      |
| `input.brokers` (kafka)                         | ✅          | List of bootstrap servers `host:port`.                                   |
| `input.topic` (kafka)                           | ✅          | Topic to consume.                                                        |
| `input.group_id` (kafka)                        | ➖          | Offsets are stored under this consumer-group ID (random if omitted).     |
| `input.auto_offset_reset` (kafka)               | ➖          | Start at `earliest` (default) or `latest` when no stored offset exists.  |
| `input.commit_interval_ms` (kafka)              | ➖          | Milliseconds between auto-commit operations (default **5000**).          |
| `input.user` / `input.password` (kafka)         | ➖          | Credentials if the cluster is secured.                                   |
| `input.host`, `input.port`, `input.queue` (rmq) | ✅          | Where to connect and which queue to consume.                             |
| `parser.delimiter`                              | ➖          | Character used to `split()` each incoming line (default ` \ `)           |
| `parser.parse_to`                               | fixed      | Must be `json` (future formats may be added).                            |
| **`fields[]`**                                  | ✅          | Ordered array describing every column.                                   |
| `name`                                          | ✅          | Key in the output JSON. NOTE: located under fields                       |
| `type`                                          | ✅          | `str`, `int`, `float`, `bool`, or `datetime`. NOTE: located under fields |
| `format` (datetime only)                        | ✅          | `strftime` pattern used for parsing. NOTE: located under fields          |
| **`output`**                                    | ✅          | Currently only a RabbitMQ destination.                                   |
| `output.exchange`, `output.routing_key`         | ✅          | Where the parser publishes each JSON document.                           |
| `output.user` / `output.password`               | ➖          | Credentials if the broker is secured.                                    |
| **`logging.level`**                             | ➖          | Sets global log verbosity (`INFO` by default).                           |

---

Below is an **add-on block** you can paste right after section **1.1 – Key-by-key
explanation**.
It explains how to represent **nested objects** with the existing flat-line
parser design and (optionally) how to “densify” them back into true JSON
sub-objects.

---

### 1.2 – Nested objects (dot-path keys)

The parser itself is intentionally simple: it splits one flat, delimited line
into *columns*.
If you need a nested JSON structure—e.g.

```json
{
  "phone": {
    "phone_number": "053-532-8989",
    "phone_type":   "Galaxy S24+"
  }
}
```

encode each leaf as its **own column** and use *dot-notation* in the
`fields:` list:

```yaml
fields:
  - name: tree_id
    type: int

  - name: species
    type: str

  - name: phone.phone_number   # ← dot path
    type: str

  - name: phone.phone_type     # ← dot path
    type: str
```

---

## 2 - Running locally

```bash
python -m planted_trees_parser   # uses config/parser-config.yaml unless CONFIG_PATH is set
```

_**NOTE: Make sure the relevant Kafka or RabbitMQ broker is reachable!!**_
