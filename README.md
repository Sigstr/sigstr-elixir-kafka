# SigstrElixirKafka

## Step 1

Add KafkaEx and SigstrElixirKafka to `mix.exs`:

```elixir
def deps do
  [
    # OTHER STUFF,
    {:kafka_ex, "~> 0.8"},
    {:sigstr_elixir_kafka, git: "git@bitbucket.org:sigstr/sigstr-elixir-kafka.git"}
  ]
end
```

## Step 2

`mix deps.update`

## Step 3

Add universal Kafka config values to `config.exs`:

```elixir
config :kafka_ex,
  disable_default_worker: true,
  use_ssl: false,
  kafka_version: "1.1"
```

Add your dev Kafka broker to `dev.exs`:

```elixir
config :kafka_ex, brokers: [{"localhost", 9092}]
```

## Step 4

Implement one or more GenConsumers in your project as described in the [KafkaEx docs](https://hexdocs.pm/kafka_ex/KafkaEx.GenConsumer.html#content).

## Step 5

Start SigstrKafkaMonitor in `application.ex`:

```elixir
kafka_genconsumers = [
  supervisor(KafkaEx.ConsumerGroup, [MyApp.MyGenConsumer, "my-consumer-group", ["my-topic"]])
]

children = [
  # OTHER STUFF,
  supervisor(SigstrKafkaMonitor, [kafka_genconsumers])
]

opts = [strategy: :one_for_one, name: MyApp.Supervisor]
Supervisor.start_link(children, opts)
```

Production Kafka brokers are specified by environment variable:
`KAFKA_SERVERS=broker1:9092,broker2:9093,broker3:9094`