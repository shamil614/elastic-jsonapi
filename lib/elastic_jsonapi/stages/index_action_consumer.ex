defmodule Search.Stages.IndexActionConsumer do
  @moduledoc """
  Consume the queue of IndexAction operations to perform on Indices.
  """

  require Logger

  use Retry
  use GenStage

  import Utils, only: [module_name: 1]

  alias Search.Stages.IndexActionProducer

  def start_link do
    GenStage.start_link(__MODULE__, :state_doesnt_matter)
  end

  def init(state) do
    {:consumer, state, subscribe_to: [{IndexActionProducer, max_demand: 20, min_demand: 0}]}
  end

  def handle_events(events, _from, state) do
    events
    |> Enum.uniq()
    |> log_events()
    |> Enum.each(fn {module, channel, payload} ->
      # kick off function to perform the index changes
      module.indices_changes(channel, payload)
    end)

    {:noreply, [], state}
  end

  @spec log_events(list) :: list
  defp log_events(events) do
    Logger.info(fn ->
      "#{module_name(__MODULE__)} : #{inspect(self())} Handling #{Enum.count(events)} events. \n"
    end)

    Logger.debug(fn ->
      "#{module_name(__MODULE__)} : #{inspect(self())} events => #{inspect(events)}"
    end)

    events
  end
end
