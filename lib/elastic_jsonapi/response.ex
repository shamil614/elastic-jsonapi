defmodule Search.Response do
  @moduledoc """
  Format the Search Response into a JSONAPI compliant document.
  """

  require Logger

  alias Elastix.HTTP
  alias HTTPoison.Error, as: HTTPoisonError
  alias HTTPoison.Response, as: HTTPoisonResponse
  alias Plug.Conn
  alias Search.{Document, Paginator, Request}

  @type resp :: {:ok | :error, map | String.t()}

  @template %{
    "data" => [],
    "included" => [],
    "jsonapi" => %{"version" => "1.0"},
    "links" => %{},
    "meta" => %{"record-count" => 0}
  }

  @doc """
  Performs the logic necessary to format a response from Elasticsearch.
  The extra Elasticsearch data, and rooted includes are dropped.
  The repeating `data` and `included` data are aggregagted into a single data structure.
  Record count is reflected in the metadata.
  """
  @spec perform(HTTP.resp(), scope_module :: module, conn :: Conn.t(), search_request :: Request.t()) ::
          __MODULE__.resp()
  def perform(
        {:ok, %HTTPoisonResponse{body: %{"hits" => %{"hits" => hits, "total" => total_hits}}}},
        scope_module,
        conn = %Conn{},
        search_request = %Request{}
      ) do
    pagination = Paginator.to_page_links(conn, total_hits)

    # rename filters to use dashes to match the client supplied names. avoids confusion.
    filters =
      scope_module.allowed_search_filters()
      |> Enum.map(fn filter ->
        String.replace(filter, "_", "-")
      end)

    accumulator =
      @template
      |> Map.put("links", pagination)
      |> put_in(["meta", "record-count"], total_hits)
      |> put_in(["meta", "search-fields"], Map.keys(scope_module.allowed_search_fields))
      |> put_in(["meta", "sort-fields"], Map.keys(scope_module.allowed_sort_fields))
      |> put_in(["meta", "filters"], filters)

    names_for_included =
      Enum.flat_map(search_request.include, fn i -> Map.get(scope_module.include_types_mapping, i, []) end)

    response = extract_jsonapi_keys(hits, accumulator, names_for_included)

    {:ok, response}
  end

  def perform({:ok, %HTTPoisonResponse{body: %{"error" => error}}}, _scope_module, _conn, _search_request) do
    msg = "Elasticsearch error detected"
    # force whatever error is to a string. ensures the return type.
    error = inspect(error)

    Logger.info(fn ->
      msg <> " => #{error}"
    end)

    Honeybadger.notify(msg, %{error: error})

    {:error, error}
  end

  def perform({:error, error = %HTTPoisonError{}}, _scope_module, _conn, _search_request) do
    msg = "HTTP error detected"
    # force Struct to a string. ensures the return type.
    error = inspect(error)

    Logger.info(fn ->
      msg <> " => #{error}"
    end)

    Honeybadger.notify(msg, %{error: error})

    {:error, error}
  end

  @spec extract_jsonapi_keys(list(map), map, list(String.t())) :: map
  defp extract_jsonapi_keys(hits, accumulator, include_types) do
    response =
      %{"data" => data} =
      Enum.reduce(hits, accumulator, fn hit, acc ->
        acc_data = Map.get(acc, "data")
        acc_included = Map.get(acc, "included")

        data = hit |> get_in(["_source", "data"]) |> format_data() |> List.wrap() |> Enum.concat(acc_data)

        included =
          hit
          |> get_in(["_source", "included"])
          |> List.wrap()
          |> Enum.concat(acc_included)
          |> Enum.filter(fn inc -> Enum.member?(include_types, inc["type"]) end)

        acc
        |> Map.put("data", data)
        |> Map.put("included", included)
      end)

    # need to reverse the list since hit is prepended to the accumulated list. otherwise sort is incorrect.
    reversed_data = Enum.reverse(data)
    Map.put(response, "data", reversed_data)
  end

  @spec format_data(map) :: map
  defp format_data(data) do
    data |> format_attributes()
  end

  @spec format_attributes(map) :: map
  defp format_attributes(data = %{"attributes" => attributes}) do
    formatted_attributes =
      Enum.into(attributes, %{}, fn {k, v} ->
        formatted_values = format_values(v)

        {k, formatted_values}
      end)

    Map.put(data, "attributes", formatted_attributes)
  end

  @spec format_values(any) :: any
  defp format_values(value = %{}) do
    Enum.into(value, %{}, fn {k, v} ->
      formatted_values = format_values(v)
      {k, formatted_values}
    end)
  end

  defp format_values(value) when is_list(value) do
    replace_empty_value(value)
  end

  defp format_values(value = nil), do: value

  defp format_values(value), do: replace_empty_value(value)

  @spec replace_empty_value(any) :: any
  defp replace_empty_value(value) do
    empty_value = Document.empty_value()

    case value do
      [^empty_value] -> nil
      ^empty_value -> nil
      _ -> value
    end
  end
end
