defmodule Search.Mapping do
  @moduledoc """
  Provides a behavior for managing an Elasticsearch Mapping.
  """

  @typedoc """
  Specific data format expected by Index module for creating a mapping with the index.
  """
  # Normally I would keep it consistent with the other payloads and use a String key,
  # but the typsec fails with a String.
  @type payload :: %{properties: map}

  @doc """
  Docuemnt type name. Should match what's on the JSONAPI resource.
  """
  @callback type() :: String.t()

  @doc """
  Payload used when creating / updating an Index with the mapping of document to Elasticsearch properties.
  """
  @callback build_payload() :: payload()

  @doc """
  Mapping of the overall fields that can be queried in a search request.
  The key represents the abreviated request field.
  The value represents the full path to the attribute in the document.
  EX: `created-at => data.attributes.created-at`
  """
  @callback search_fields_mapping() :: %{required(String.t()) => String.t() | list(String.t())}

  @doc """
  Mapping of the overall fields that can be sorted in a search request.
  The key represents the abreviated request field.
  The value represents the full path to the attribute in the document.
  EX: `external-reference => data.attributes.external-reference.raw`
  """
  @callback sort_fields_mapping() :: %{required(String.t()) => String.t() | list(String.t())}
end
