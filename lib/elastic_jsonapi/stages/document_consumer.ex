defmodule Search.Stages.DocumentConsumer do
  @moduledoc """
  Consumes a queue of operations for Documents that are operated on.
  """

  require Ecto.Query
  require Logger

  use Retry
  use GenStage

  alias Elastix.{Bulk, HTTP}
  alias HTTPoison.Response
  alias Search.Document
  alias Search.Stages.DocumentProducer

  import Destructure
  import Search, only: [bulk_size: 0, elasticsearch_url: 0]
  import Utils, only: [module_name: 1]

  @dialyzer {:nowarn_function, bulk_request: 1}

  def start_link do
    GenStage.start_link(__MODULE__, :state_doesnt_matter)
  end

  def init(state) do
    {:consumer, state, subscribe_to: [{DocumentProducer, max_demand: bulk_size(), min_demand: 0}]}
  end

  @doc """
  Handles events by building a multiple action payload. Sends multiple actions in one API call.
  [%{action: action, id: id, document_module: document_module}]
  """
  def handle_events(events, _from, state) do
    events
    |> Enum.uniq()
    |> log_events()
    |> build_bulk_payload()
    |> bulk_request()

    {:noreply, [], state}
  end

  @doc """
  Executes the bulk API call to Elasticsearch. It also has error checking and retry components.
  """
  @spec bulk_request(list) :: HTTP.resp()
  def bulk_request(bulk_payload) do
    # exponential backoff. expires after 20 seconds
    result =
      retry(with: exp_backoff() |> randomize() |> expiry(20_000)) do
        res = Bulk.post(elasticsearch_url(), bulk_payload)

        res
        |> log_msg("Bulk request result")
        |> maybe_report_error()
      after
        result -> result
      else
        error -> error
      end

    result |> maybe_report_error()
  end

  @spec maybe_report_error(HTTP.resp()) :: HTTP.resp()
  defp maybe_report_error(bulk_result = {:error, response}) do
    Honeybadger.notify("#{module_name(__MODULE__)}Failed", %{bulk_results: inspect(response)})
    bulk_result
  end

  defp maybe_report_error(bulk_result = {:ok, %Response{body: %{"errors" => false}}}), do: bulk_result

  defp maybe_report_error(bulk_result = {:ok, %Response{body: %{"errors" => true, "items" => items}}}) do
    Honeybadger.notify("#{module_name(__MODULE__)} Errors", %{bulk_items_errors: items})
    bulk_result
  end

  @spec log_events([map]) :: [map]
  defp log_events(events) do
    Logger.info(fn ->
      "#{module_name(__MODULE__)} : #{inspect(self())} Handling #{Enum.count(events)} events. \n" <>
        inspect(events)
    end)

    events
  end

  @spec log_msg(any, String.t()) :: any
  defp log_msg(data, msg) do
    Logger.debug(fn ->
      msg <> " DATA => " <> inspect(data)
    end)

    data
  end

  @doc """
  Sets up a data structure for storing actions per Document module.
  %{FooDocument => %{ids: [], actions: []}, BarDocument => %{ids: [], actions: []}}
  """
  def grouped_document_modules do
    Enum.into(Document.document_modules(), %{}, fn i ->
      {i, %{ids: [], actions: []}}
    end)
  end

  @doc """
  Builds the payload to send on the bulk api call. Combines all the events into one payload.
  """
  @spec build_bulk_payload([map]) :: [map]
  def build_bulk_payload(events) do
    events
    |> bulk_items_by_document()
    |> merge_action_items()
  end

  @doc """
  Filters the actions by Document, collects record ids for easy database lookup,
  and builds the base item for bulk operations. Certain actions like `:delete` are not fetched in the database.
  Index operations do need to be fetched from the database so a document payload can be created.
  """
  @spec bulk_items_by_document([map]) :: map
  def bulk_items_by_document(events = [%{action: _, document_module: _, id: _} | _]) do
    # Sort through the events and filter out by document module and ids.
    grouped_modules = grouped_document_modules()

    Enum.reduce(events, grouped_modules, fn d(%{action, document_module, id}), acc ->
      module_events = d(%{ids, actions}) = Map.fetch!(acc, document_module)

      item = bulk_item(document_module, id, action)

      updated_module_events =
        module_events
        |> Map.put(:actions, [item | actions])

      # delete actions don't need a document payload.
      # only track ids that need a record from the DB to build a document payload
      updated_module_events =
        if action == :index do
          Map.put(updated_module_events, :ids, [id | ids])
        else
          updated_module_events
        end

      Map.put(acc, document_module, updated_module_events)
    end)
  end

  @spec bulk_item(document :: module, id :: integer, action :: atom) :: map
  defp bulk_item(document_module, id, action) do
    # common document data
    type = document_module.type()
    index_name = document_module.versioned_index_name()

    # common bulk item data
    %{
      action => %{
        "_type" => type,
        "_id" => to_string(id),
        "_index" => index_name
      }
    }
  end

  @spec merge_action_items(map) :: [map]
  defp merge_action_items(action_groups) do
    # Query the DB for the records needed to build the ES payload
    Enum.flat_map(action_groups, fn {document_module, d(%{ids, actions})} ->
      repo = document_module.repo()

      # where applicable, query should be setup to exclude soft deleted records if the schema does have `deleted_at`
      query = true |> document_module.query() |> Ecto.Query.where([m], m.id in ^ids)

      # build a map with the record id as the key for easy access later
      mapped_records = query |> repo.all() |> Enum.into(%{}, fn r -> {to_string(r.id), r} end)

      Enum.flat_map(actions, fn action ->
        maybe_build_payload(document_module, action, mapped_records)
      end)
    end)
  end

  @spec maybe_build_payload(document_module :: module, action :: map, mapped_records :: map) :: [map]
  defp maybe_build_payload(document_module, action = %{delete: %{"_id" => id}}, _mapped_records) do
    log_action(:delete, id, document_module)
    [action]
  end

  defp maybe_build_payload(document_module, action = %{index: item = %{"_id" => id}}, mapped_records) do
    struct = Map.get(mapped_records, id)

    # detect record was not found. could be a soft delete. attempt to delete from ES.
    if is_nil(struct) do
      # don't log here. the function matching the delete action logs
      maybe_build_payload(document_module, %{delete: item}, mapped_records)
    else
      log_action(:index, id, document_module)
      payload = struct |> document_module.build_payload()
      [action, payload]
    end
  end

  @spec log_action(action_type :: atom, id :: String.t(), document_module :: module) :: :void
  defp log_action(action_type, id, document_module) do
    current_module_name = module_name(__MODULE__)
    document_module_name = module_name(document_module)

    Logger.info(fn ->
      "#{current_module_name} #{inspect(self())} performing #{action_type} on #{document_module_name} => #{id}"
    end)

    :void
  end
end
