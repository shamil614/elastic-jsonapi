defmodule Search.Stages.IndexActionConsumerSupervisor do
  @moduledoc """
  Supervises IndexActionConsumers
  """

  use Supervisor

  alias Search.Stages.IndexActionConsumer

  def start_link do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(_arg) do
    worker_count = Search.worker_count()

    children =
      Enum.map(1..worker_count, fn i ->
        worker(IndexActionConsumer, [], id: {IndexActionConsumer, i}, restart: :permanent)
      end)

    supervise(children, strategy: :one_for_one)
  end
end
