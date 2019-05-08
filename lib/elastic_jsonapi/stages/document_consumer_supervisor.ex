defmodule Search.Stages.DocumentConsumerSupervisor do
  @moduledoc """
  Supervises DocumentConsumers
  """

  use Supervisor
  alias Search.Stages.DocumentConsumer

  def start_link do
    Supervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  def init(_arg) do
    worker_count = Search.worker_count()

    children =
      Enum.map(1..worker_count, fn i ->
        worker(DocumentConsumer, [], id: {DocumentConsumer, i}, restart: :permanent)
      end)

    supervise(children, strategy: :one_for_one)
  end
end
