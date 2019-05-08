defmodule Search.Stages.IndexActionProducer do
  @moduledoc """
  Produces a pipeline of operations for Search IndexAction operations
  """

  use GenStage

  @max_queue_size Confex.get_env(:search, :max_queue_size)
  @name __MODULE__

  require Logger

  import Utils, only: [module_name: 1]

  def start_link(_init) do
    GenStage.start_link(__MODULE__, {Qex.new(), 0}, name: @name)
  end

  def init(state), do: {:producer, state}

  @doc """
  Performs async call to add an item to the queue.
  """
  @spec add(index_action_module :: module(), channel :: String.t(), payload :: String.t()) :: :ok
  def add(index_action_module, channel, payload) do
    GenStage.cast(@name, {:add, index_action_module, channel, payload})
  end

  def handle_cast({:add, index_action_module, channel, payload}, {queue, pending_demand}) do
    Logger.debug(fn ->
      "#{module_name(__MODULE__)} : handle_cast/2 Queue length => #{Enum.count(queue)}"
    end)

    updated_queue =
      if Enum.member?(queue, {index_action_module, channel, payload}) do
        Logger.debug(fn ->
          "#{module_name(__MODULE__)} : Not adding action => #{inspect({channel, payload})}"
        end)

        queue
      else
        Logger.debug(fn ->
          "#{module_name(__MODULE__)} : Adding action => #{inspect({channel, payload})}"
        end)

        Qex.push(queue, {index_action_module, channel, payload})
      end

    {events, state} = take_demand(updated_queue, pending_demand)
    {:noreply, events, state}
  end

  def handle_demand(demand, {queue, pending_demand}) do
    maybe_notify_queue(queue)

    {events, state} = take_demand(queue, pending_demand + demand)

    Logger.debug(fn ->
      "#{module_name(__MODULE__)} : Demand #{demand} + Pending Demand #{pending_demand} => \n Events #{
        inspect(events)
      } => \n Updated State #{inspect(state)}"
    end)

    {:noreply, events, state}
  end

  # notify when the queue size exceeds a threshold
  defp maybe_notify_queue(queue) do
    size = Enum.count(queue)

    Logger.debug(fn ->
      "#{module_name(__MODULE__)} queue size: #{size} => #{inspect(queue)}"
    end)

    if size > @max_queue_size do
      Honeybadger.notify(
        "#{module_name(__MODULE__)} queue exceeds #{@max_queue_size} count threshold => #{size}"
      )
    end

    queue
  end

  defp take_demand(queue, 0) do
    {[], {queue, 0}}
  end

  defp take_demand(queue, pending_demand) when pending_demand > 0 do
    queue_count = Enum.count(queue)

    # Make sure that demand doesn't exceed queue size.
    # Fill what can be filled.
    # Indicate when demand is met with 0
    {demand, pending_demand} =
      if pending_demand >= queue_count do
        {queue_count, pending_demand - queue_count}
      else
        {pending_demand, 0}
      end

    {events, updated_queue} = Qex.split(queue, demand)

    {Enum.to_list(events), {updated_queue, pending_demand}}
  end
end
