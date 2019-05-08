defmodule Search.Scope do
  @moduledoc """
  Defines a set of behaviours the `Search.Request` depends on to build a proper search request.
  """

  @typedoc """
  Defined set of key / values that define an Elasticsearch Filter.
  """
  @type filter_body :: %{
          optional(:must) => list(),
          optional(:must_not) => list(),
          optional(:should) => list()
        }

  @doc """
  List of filter names that are allowed to be applied from a request.
  Names should be underscored to match the formatted request.
  EX: `job-assignments.state` matches to `job_assignments.state` after formatted to params.
  """
  @callback allowed_search_filters() :: list(String.t()) | list()

  @doc """
  Map of the relationship between requestable 'include' paths and the data returned.
  The keys represent the relationship paths requested by the FE
  The values represent the corresponding types of records that could be returned
  """
  @callback include_types_mapping() :: %{required(String.t()) => list(String.t())}

  @doc """
  Mapping of the allowed fields that can be queried in a search request.
  The key represents the abreviated request field.
  The value represents the full path to the attribute in the document.
  EX: `created-at => data.attributes.created-at`
  """
  @callback allowed_search_fields() :: %{required(String.t()) => String.t() | list(String.t())}

  @doc """
  Mapping of the allowed fields that can be sorted in a search request.
  The key represents the abbreviated request field.
  The value represents the full path to the attribute in the document.
  EX: `external-reference => data.attributes.external-reference.raw`
  """
  @callback allowed_sort_fields() :: %{required(String.t()) => String.t() | list(String.t())}

  @doc """
  """
  @callback filter(name :: String.t(), value :: String.t(), conn :: Plug.Conn.t()) :: filter_body()

  @doc """
  The document types the search applies to. There should only be one type in a index.
  """
  @callback types() :: list(String.t())
end
