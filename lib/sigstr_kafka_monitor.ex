defmodule SigstrKafkaMonitor do
  use GenServer
  require Logger

  @restart_wait_seconds 60

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(child_specs) do
    children = %{}
    refs = %{}

    Supervisor.start_link(
      [{DynamicSupervisor, name: SigstrElixirKafka.Supervisor, strategy: :one_for_one}],
      strategy: :one_for_one,
      name: SigstrElixirKafka.Monitor
    )

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
      Process.send(self(), {:start, child_spec}, [])
    end

    {:ok, {children, refs}}
  end

  def handle_info({:start, child_spec}, {children, refs}) do
    Application.start(:kafka_ex)

    {children, refs} =
      case DynamicSupervisor.start_child(SigstrElixirKafka.Supervisor, child_spec) do
        {:ok, pid} ->
          Logger.info("Monitor monitoring #{inspect(child_spec)} running at #{inspect(pid)}")
          ref = Process.monitor(pid)
          refs = Map.put(refs, ref, child_spec)
          children = Map.put(children, child_spec, pid)
          {children, refs}

        {:error, error} ->
          Logger.warn("#{inspect(child_spec)} failed to start. Restarting in #{@restart_wait_seconds} seconds...")
          Logger.warn(inspect(error))

          Process.send_after(self(), {:start, child_spec}, @restart_wait_seconds * 1000)
          {children, refs}
      end

    {:noreply, {children, refs}}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, {children, refs}) do
    {child_spec, refs} = Map.pop(refs, ref)
    children = Map.delete(children, child_spec)

    Logger.warn("#{inspect(child_spec)} went down. Restarting in #{@restart_wait_seconds} seconds...")

    Process.send_after(self(), {:start, child_spec}, @restart_wait_seconds * 1000)
    {:noreply, {children, refs}}
  end
end
