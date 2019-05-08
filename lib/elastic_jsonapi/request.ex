defmodule Search.Request do
  @moduledoc """
  Formats the HTTP search request prams into an Elasticsearch query AND executes the query on Elasticsearch.
  """

  require Logger

  import Destructure

  alias Elastix.Search, as: ExSearch
  alias Search.{Paginator, Response, Scope}
  alias Plug.Conn

  @default_sort %{"_score" => %{"order" => "desc"}}
  @filter_template %{bool: %{must: [], must_not: [], should: []}}
  @query_param "q"

  defstruct filter: nil,
            from: 0,
            include: nil,
            q: nil,
            search_fields: nil,
            size: 10,
            sort: nil

  @typedoc """
  * `filter` - specificies how to filter the search request
  * `from` - starting point for pagination
  * `q` - the query term
  * `search_fields` - comma separated list of fields to search with the `q` term
  * `size` - number of results for pagination
  * `sort` - the direction for ordering a single attribute
  """
  @type t :: %__MODULE__{
          filter: bool_filter(),
          from: non_neg_integer,
          q: String.t() | nil,
          include: list(String.t()) | nil,
          search_fields: list(String.t()) | nil,
          size: non_neg_integer,
          sort: list(sort_t()) | nil
        }

  @typedoc """
  Data structure defining how Elasticsearch sorts the results.
  """
  @type sort_t :: %{required(String.t()) => %{required(String.t()) => String.t()}}

  @typedoc """
  Full bool filter format. The format is verbose but allows for flexibility in building filters.
  """
  @type bool_filter :: %{
          required(:bool) => %{
            required(:must) => list(),
            required(:must_not) => list(),
            required(:should) => list()
          }
        }

  @doc """
  Handles the request parameters to build the query for the specific index AND executes the query on Elasticsearch.
  """
  @spec perform(index_name :: String.t(), params :: map, scope_module :: module, conn :: Conn.t()) ::
          Response.resp()
  def perform(index_name, params, scope_module, conn) do
    search_request = params_to_struct(params, scope_module, conn)
    payload = build_payload(search_request)

    Logger.debug(fn ->
      "Search Request payload => \n #{inspect(payload)}"
    end)

    Search.elasticsearch_url()
    |> ExSearch.search(index_name, scope_module.types(), payload)
    |> Response.perform(scope_module, conn, search_request)
  end

  @doc false
  @spec filter_template() :: bool_filter()
  def filter_template, do: @filter_template

  @spec build_payload(%__MODULE__{}) :: map
  defp build_payload(d(%__MODULE__{filter, from, size, sort, q: q})) when is_nil(q) or q == "" do
    %{
      from: from,
      query: %{
        bool: %{
          filter: filter,
          must: %{
            match_all: %{}
          }
        }
      },
      size: size,
      sort: sort
    }
  end

  defp build_payload(d(%__MODULE__{filter, from, q, search_fields, size, sort})) do
    %{
      from: from,
      query: %{
        bool: %{
          filter: filter,
          must: %{
            multi_match: %{fields: search_fields, query: q}
          }
        }
      },
      size: size,
      sort: sort
    }
  end

  @spec params_to_struct(params :: map, scope_module :: module, conn :: Conn.t()) :: __MODULE__.t()
  defp params_to_struct(params, scope_module, conn) do
    d(%{from, size}) = Paginator.page_to_offset(params)

    search_fields =
      params
      |> Map.get("search_fields", nil)
      |> verify_search_fields(scope_module)

    search_filters =
      params
      |> Map.get("filter", %{})
      |> verify_search_filters(scope_module, conn)
      |> merge_filters()

    include_types =
      params
      |> Map.get("include", "")
      |> verify_include_types(scope_module)

    sort =
      params
      |> Map.get("sort", nil)
      |> verify_sort_fields(scope_module)

    %__MODULE__{
      filter: search_filters,
      from: from,
      q: Map.get(params, @query_param, nil),
      size: size,
      search_fields: search_fields,
      include: include_types,
      sort: sort
    }
  end

  @doc false
  @spec merge_filters([Scope.filter_body()]) :: bool_filter()
  def merge_filters(filters) do
    Enum.reduce(filters, @filter_template, fn filter, acc ->
      updated_conds =
        acc
        |> Map.get(:bool)
        |> Map.merge(filter, fn _k, acc_value, filter_value ->
          filter_value ++ acc_value
        end)

      Map.put(acc, :bool, updated_conds)
    end)
  end

  # whitelist or default the fields to apply a query search to
  @spec verify_search_fields(search_fields :: String.t() | nil, scope_module :: module) :: list(String.t())
  defp verify_search_fields(nil, scope_module) do
    allowed_search_fields = scope_module.allowed_search_fields()

    allowed_search_fields |> Map.values() |> List.flatten()
  end

  # parse the string of fields. get the full path of the attribute from the whitelist of attrs.
  defp verify_search_fields(search_fields, scope_module) when is_binary(search_fields) do
    allowed_search_fields = scope_module.allowed_search_fields()

    search_fields
    |> String.split(",")
    |> Enum.flat_map(fn field ->
      allowed_search_fields |> Map.get(field, []) |> List.wrap()
    end)
  end

  # possible that the incorrect type is passed
  defp verify_search_fields(_search_fields, scope_module) do
    allowed_search_fields = scope_module.allowed_search_fields()

    verify_search_fields(nil, allowed_search_fields)
  end

  # possible that incorrect includes could be passed in
  @spec verify_include_types(include_types :: String.t(), scope_module :: module()) :: list(String.t())
  defp verify_include_types("", _scope_module), do: []

  defp verify_include_types(include_types, scope_module) do
    allowed_include_types =
      scope_module.include_types_mapping()
      |> Map.keys()

    include_types
    |> String.split(",")
    |> Enum.filter(fn type_name -> Enum.member?(allowed_include_types, type_name) end)
  end

  @doc false
  @spec verify_search_filters(
          filters :: %{required(String.t()) => any},
          scope_module :: module(),
          conn :: Conn.t()
        ) :: list(Scope.filter_body())
  def verify_search_filters(filters, scope_module, conn) do
    allowed_search_filters = scope_module.allowed_search_filters()

    filters
    |> Enum.flat_map(fn {filter_name, filter_value} ->
      if Enum.member?(allowed_search_filters, filter_name) do
        parsed_value = filter_value |> to_string() |> String.downcase() |> String.split(",")
        filter_name |> scope_module.filter(parsed_value, conn) |> List.wrap()
      else
        []
      end
    end)
  end

  @spec verify_sort_fields(sort :: String.t() | nil, scope_module :: module) :: list(sort_t())
  defp verify_sort_fields(nil, _scope_module), do: [@default_sort]

  defp verify_sort_fields(sort, scope_module) when is_binary(sort) do
    {parsed_sort, order} =
      case String.split(sort, ~r{\A-}) do
        [attr] -> {attr, "asc"}
        ["", attr] -> {attr, "desc"}
      end

    allowed_sort_fields = scope_module.allowed_sort_fields()
    sort_field = Map.get(allowed_sort_fields, parsed_sort)

    if sort_field do
      [%{sort_field => %{"order" => order}}]
    else
      [@default_sort]
    end
  end
end
