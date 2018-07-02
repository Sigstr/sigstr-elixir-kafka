defmodule SigstrKafkaMonitor do
  use GenServer
  require Logger

  @restart_wait_seconds 60

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: SigstrKafka)
  end

  def up?() do
    Application.started_applications() |> Enum.any?(fn {app, _, _} -> app == :kafka_ex end) && SigstrKafka |> GenServer.call(:get_worker_ref) != nil
  end

  def produce(messages, topic) when is_binary(topic) and is_list(messages) do
    unless length(messages) <= 0 do
      if SigstrKafka |> GenServer.call({:produce, topic, messages}) == :error do
        Logger.warn("KafkaEx failed to produce messages to #{topic}. Waiting #{@restart_wait_seconds} seconds and trying again.")
        Process.sleep(@restart_wait_seconds * 1000)
        produce(messages, topic)
      end
    end
  end

  @impl true
  def init(child_specs) do
    children = %{}
    refs = %{}
    worker_ref = nil
    partition_counts = %{}

    Supervisor.start_link(
      [{DynamicSupervisor, name: SigstrKafkaMonitor.DynamicSupervisor, strategy: :one_for_one}],
      strategy: :one_for_one,
      name: SigstrKafkaMonitor.Supervisor
    )

    Process.send(self(), :start_worker, [])

    case System.get_env("KAFKA_SERVERS") do
      nil ->
        nil

      value ->
        brokers =
          value
          |> String.split(",")
          |> Enum.map(fn broker ->
            pieces = String.split(broker, ":")
            {port, _} = Integer.parse(List.last(pieces))
            {List.first(pieces), port}
          end)

        Application.put_env(:kafka_ex, :brokers, brokers)
    end

    Logger.debug("Using Kafka brokers: " <> inspect(Application.get_env(:kafka_ex, :brokers)))

    for child_spec <- child_specs do
      Process.send(self(), {:start_child, child_spec}, [])
    end

    {:ok, {children, refs, worker_ref, partition_counts}}
  end

  @impl true
  def handle_info(:start_worker, {children, refs, worker_ref, partition_counts}) do
    Application.start(:kafka_ex)

    worker_ref =
      case KafkaEx.create_worker(:kafka_ex, consumer_group: :no_consumer_group) do
        {:ok, pid} ->
          Logger.info("Monitoring KafkaEx worker at " <> inspect(pid))
          Process.monitor(pid)

        {:error, {:already_started, pid}} ->
          Logger.info("KafkaEx worker already running at " <> inspect(pid))
          Process.monitor(pid)

        {:error, error} ->
          Logger.warn("KafkaEx worker failed to start. Restarting in #{@restart_wait_seconds} seconds...")
          Logger.debug(inspect(error))
          Process.send_after(self(), :start_worker, @restart_wait_seconds * 1000)
          worker_ref
      end

    {:noreply, {children, refs, worker_ref, partition_counts}}
  end

  @impl true
  def handle_info({:start_child, child_spec}, {children, refs, worker_ref, partition_counts}) do
    Application.start(:kafka_ex)

    {children, refs, worker_ref, partition_counts} =
      case DynamicSupervisor.start_child(SigstrKafkaMonitor.DynamicSupervisor, child_spec) do
        {:ok, pid} ->
          Logger.info("KafkaEx monitoring #{inspect(child_spec)} at #{inspect(pid)}")
          ref = Process.monitor(pid)
          refs = Map.put(refs, ref, child_spec)
          children = Map.put(children, child_spec, pid)
          {children, refs, worker_ref, partition_counts}

        {:error, error} ->
          Logger.warn("KafkaEx #{inspect(child_spec)} failed to start. Restarting in #{@restart_wait_seconds} seconds...")
          Logger.debug(inspect(error))
          Process.send_after(self(), {:start_child, child_spec}, @restart_wait_seconds * 1000)
          {children, refs, worker_ref, partition_counts}
      end

    {:noreply, {children, refs, worker_ref, partition_counts}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, {children, refs, worker_ref, partition_counts}) do
    if ref == worker_ref do
      Logger.warn("KafkaEx worker went down. Restarting in #{@restart_wait_seconds} seconds...")
      Process.send_after(self(), :start_worker, @restart_wait_seconds * 1000)
      {:noreply, {children, refs, worker_ref, partition_counts}}
    else
      {child_spec, refs} = Map.pop(refs, ref)
      children = Map.delete(children, child_spec)
      Logger.warn("KafkaEx #{inspect(child_spec)} went down. Restarting in #{@restart_wait_seconds} seconds...")
      Process.send_after(self(), {:start_child, child_spec}, @restart_wait_seconds * 1000)
      {:noreply, {children, refs, worker_ref, partition_counts}}
    end
  end

  @impl true
  def handle_call(:get_worker_ref, _from, {children, refs, worker_ref, partition_counts}) do
    {:reply, worker_ref, {children, refs, worker_ref, partition_counts}}
  end

  @impl true
  def handle_call({:produce, topic, messages}, _from, {children, refs, worker_ref, partition_counts}) do
    unless worker_ref != nil && Application.started_applications() |> Enum.any?(fn {app, _, _} -> app == :kafka_ex end) do
      Logger.error("KafkaEx is down and unable to produce messages to topic #{topic}")
      {:reply, :error, {children, refs, worker_ref, partition_counts}}
    else
      partition_counts =
        case partition_counts |> Map.has_key?(topic) do
          true -> partition_counts
          false -> partition_counts |> Map.put(topic, get_partition_count(topic))
        end

      messages_by_parition =
        messages
        |> Enum.reduce(%{}, fn message, map ->
          partition =
            case Map.has_key?(message, :key) && !is_nil(message.key) do
              true -> rem(Murmur.hash_x86_32(message.key), partition_counts[topic])
              false -> Enum.random(0..(partition_counts[topic] - 1))
            end

          Map.put(map, partition, Map.get(map, partition, []) ++ [message])
        end)

      Logger.debug("KafkaEx producing to topic #{topic} partitions #{inspect(Map.keys(messages_by_parition))}")

      for partition <- messages_by_parition |> Map.keys() do
        messages =
          messages_by_parition[partition]
          |> Enum.map(fn message ->
            case Map.has_key?(message, :key) && !is_nil(message.key) do
              true -> %KafkaEx.Protocol.Produce.Message{key: message.key, value: message.value}
              false -> %KafkaEx.Protocol.Produce.Message{value: message.value}
            end
          end)

        Logger.debug(inspect(messages))

        try do
          KafkaEx.produce(%KafkaEx.Protocol.Produce.Request{topic: topic, partition: partition, messages: messages, required_acks: 0, timeout: 10000})
        rescue
          err in RuntimeError -> Logger.error("KafkaEx error producing to topic #{topic}: #{inspect(err)}")
        end
      end

      {:reply, :ok, {children, refs, worker_ref, partition_counts}}
    end
  end

  defp get_partition_count(topic) when is_binary(topic) do
    response = KafkaEx.metadata(topic: topic)
    response.topic_metadatas |> List.first() |> Map.get(:partition_metadatas) |> length()
  end
end
