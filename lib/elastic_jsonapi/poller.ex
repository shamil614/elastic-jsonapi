defmodule Search.Poller do
  @moduledoc """
  Utility module designed to query Elasticsearch for indexed documents.
  """

  use Retry

  require Logger

  alias Elastix.Search, as: ExSearch
  alias HTTPoison.Error, as: HTTPoisonError
  alias HTTPoison.Response, as: HTTPoisonResponse
  alias Search.Request

  # dialyzer can't figure out the return value from the retry block
  @dialyzer {:nowarn_function, search_request_with_retry: 4}

  @doc """
  Performs the actual requests to Elasticsearch by the index module.
  The list of document ids are the documents to be searched for.
  Document ids directly correspond to the primary keys of the records in the database.
  Optional retry to continuously poll until the documents are found or until a timeout is reached.
  """
  @spec perform(
          index_module :: module,
          document_ids :: [String.t()] | [non_neg_integer],
          filter :: Request.bool_filter(),
          retry :: boolean
        ) :: {:ok | :error, true | false}
  def perform(index_module, document_ids, filter, retry \\ false) do
    index_name = index_module.versioned_index_name()
    types = [index_module.mapping_module().type()]

    if retry do
      search_request_with_retry(index_name, types, document_ids, filter)
    else
      search_request(index_name, types, document_ids, filter)
    end
  end

  @spec build_payload(document_ids :: [String.t()] | [non_neg_integer], filter :: Request.bool_filter()) ::
          map
  defp build_payload(document_ids, filter) do
    must = get_in(filter, [:bool, :must])

    id_filter = [
      %{
        terms: %{
          "data.id.raw" => document_ids
        }
      }
    ]

    updated_filter = put_in(filter, [:bool, :must], id_filter ++ must)

    %{
      size: 0,
      query: %{
        bool: %{
          filter: updated_filter
        }
      }
    }
  end

  @spec search_request_with_retry(
          index :: String.t(),
          types :: list(String.t()),
          document_ids :: [String.t()] | [non_neg_integer],
          filter :: Request.bool_filter()
        ) :: {:ok | :error, true | false}
  defp search_request_with_retry(index, types, document_ids, filter) do
    # linear back off starting at 5 miliseconds, not to exceed 1 second in between attempts.
    # number of iterations is controlled by config
    # at 10 iterations total time is 4.2 seconds.
    # at 15 iterations total time is 9.2 seconds.
    iterations = Confex.get_env(:search, :poller_retry_iterations)
    backoff = 5 |> lin_backoff(2) |> cap(1_000) |> Stream.take(iterations)

    retry(with: backoff) do
      Logger.debug(fn ->
        "Search Polling for #{inspect(document_ids)}"
      end)

      search_request(index, types, document_ids, filter)
    after
      result -> result
    else
      error ->
        context = %{index: index, types: inspect(types), document_ids: inspect(document_ids)}
        Honeybadger.notify("Search.Poller failed", context)
        error
    end
  end

  @spec search_request(
          index :: String.t(),
          types :: list(String.t()),
          document_ids :: [String.t()] | [non_neg_integer],
          filter :: Request.bool_filter()
        ) :: {:ok, true} | {:error, false}
  defp search_request(index, types, document_ids, filter) do
    data = build_payload(document_ids, filter)
    expected_hits = Enum.count(document_ids)

    total_hits =
      Search.elasticsearch_url()
      |> ExSearch.search(index, types, data)
      |> maybe_extract_hits()

    if total_hits == nil || total_hits < expected_hits do
      {:error, false}
    else
      {:ok, true}
    end
  end

  @spec maybe_extract_hits({:ok | :error, %HTTPoisonResponse{} | %HTTPoisonError{}}) :: nil | non_neg_integer
  defp maybe_extract_hits({:ok, %HTTPoisonResponse{body: %{"error" => _error}}}), do: nil
  defp maybe_extract_hits({:error, %HTTPoisonError{}}), do: nil

  defp maybe_extract_hits({:ok, %HTTPoisonResponse{body: %{"hits" => %{"total" => total_hits}}}}) do
    total_hits
  end
end
