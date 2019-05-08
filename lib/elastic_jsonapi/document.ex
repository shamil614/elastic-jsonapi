defmodule Search.Document do
  @moduledoc """
  Provides a behaviour and common functions for managing Elasticsearch Documents.
  """

  # requires
  require Logger

  alias Ecto.{Query}
  alias Search.Indices.BulkIndex

  ## Module Attributes
  @empty_value "______"

  @doc """
  Hook to allow for manipulation of the data / record before serialization.
  Result of the function gets passed to the serializer.
  """
  @callback before_serialize(record :: struct(), related_data :: map | nil) :: struct()

  @doc """
  Construts the data needed to send to Elasticsearch.
  """
  @callback build_payload(id :: non_neg_integer | String.t()) :: map
  @callback build_payload(record :: struct(), related_data :: map | nil) :: map

  @doc """
  Convenience function to perform bulk indexing of the resource.
  """
  @callback bulk_index :: BulkIndex.res()
  @callback bulk_index(nil | BulkIndex.options()) :: BulkIndex.res()

  @doc """
  Called after building payload. Allows for modification / customization of payload before indexing.
  """
  @callback customize_payload(resource :: map, record :: struct()) :: map

  @doc """
  Hook to allow the implementing module to customize the individual include before copying to the document root.
  This can be helpful to denormalize parent data into child include to make authorization possible.
  """
  @callback customize_root_include(related :: map, resource :: map) :: map

  @doc """
  Base query used to find records for serialzation for indexing.
  Helpful to include associations necessary for serialization.
  """
  @callback query(preload_associations? :: boolean) :: Query.t()

  @doc """
  Convenience function to define associations to load before building the document payload.
  """
  @callback preload_associations() :: [] | [atom]

  ## Functions

  @doc """
  All the child modules using the Document behaviour.
  """
  @spec document_modules() :: [module]
  def document_modules do
    with {:ok, list} <- :application.get_key(:search, :modules) do
      list
      |> Enum.filter(fn module ->
        attrs = module.__info__(:attributes)

        if __MODULE__ in List.wrap(attrs[:behaviour]) do
          module
        end
      end)
    end
  end

  @doc """
  Convenience function for accessing the value used to repalce illegal values.
  """
  @spec empty_value :: String.t()
  def empty_value, do: @empty_value

  def find_illegal_values(data) when is_map(data) do
    data
    |> Enum.filter(fn {_k, v} ->
      v == nil || v == "" || v == [] || v == [""] || v == [nil]
    end)
  end

  @spec log_msg(String.t()) :: no_return
  def log_msg(msg) do
    Logger.debug(fn ->
      msg
    end)
  end

  @spec replace_illegal_values(nil | String.t() | list | map) :: String.t() | list | map
  def replace_illegal_values(nil), do: @empty_value
  def replace_illegal_values(""), do: @empty_value
  def replace_illegal_values([nil]), do: [@empty_value]
  def replace_illegal_values([""]), do: [@empty_value]
  def replace_illegal_values([]), do: [@empty_value]

  def replace_illegal_values(pairs = [_head | _tail]) do
    pairs
    |> Enum.into(%{}, fn {k, v} ->
      {k, replace_illegal_values(v)}
    end)
  end

  def replace_illegal_values(resource = %{"data" => %{"attributes" => attributes}}) do
    updated_attributes =
      attributes
      |> replace_illegal_values()

    put_in(resource, ["data", "attributes"], updated_attributes)
  end

  def replace_illegal_values(data) when is_map(data) do
    illegal = data |> find_illegal_values()

    case illegal do
      # nothing illegal was found. move along
      [] ->
        data

      [_head | _tail] ->
        legal_map =
          illegal
          |> replace_illegal_values()

        Map.merge(data, legal_map)
    end
  end

  # Macros

  defmacro configure(config) do
    define_configuration(config)
  end

  defmacro document(config) do
    define_document(config)
  end

  defp define_configuration(config) do
    index_name = config[:index_name]
    repo = config[:repo]
    schema_module = config[:schema_module]
    serializer_module = config[:serializer_module]
    type = config[:type]
    versioned_index_name = config[:versioned_index_name]

    quote location: :keep do
      @doc """
      Base index name.
      """
      @spec index_name :: String.t()
      def index_name, do: unquote(index_name)

      @doc """
      Convenience function to reference the Ecto Repo from which to find the records.
      """
      @spec repo() :: Repo.t()
      def repo, do: unquote(repo)

      @doc """
      The module that defines the Ecto.Schema
      """
      def schema_module, do: unquote(schema_module)

      @doc """
      Convenience function to reference the module that handles serialization to JSONAPI
      """
      @spec serializer_module() :: module
      def serializer_module, do: unquote(serializer_module)

      @doc """
      Type name for the Elasticsearch document.
      """
      @spec type :: String.t()
      def type, do: unquote(type)

      @doc """
      Index name with the version prefixed.
      """
      @spec versioned_index_name :: String.t()
      def versioned_index_name, do: unquote(versioned_index_name)
    end
  end

  def define_document(config) do
    include = config[:include] || ""
    root_included = config[:root_included] || []

    quote location: :keep do
      @doc """
      Convenience function for accessing the value used to repalce illegal values.
      """
      @spec empty_value :: String.t()
      def empty_value, do: unquote(empty_value())

      @doc """
      Define what relationships to include in the JSONAPI document sent to Elastisearch.
      Use a comma separated string of names: "some-resource,bar"
      """
      @spec include :: String.t()

      def include, do: unquote(include)

      @doc """
      Defines the JSONAPI types to extract out of the JSONAPI included property.
      """
      @spec root_included :: list
      def root_included, do: unquote(root_included)
    end
  end

  defmacro __using__(_) do
    quote location: :keep do
      ## Requires
      require Ecto.Query

      ## Imports
      import Destructure
      import unquote(__MODULE__)

      ## Behaviours
      @behaviour Search.Document

      ## Aliases
      alias Elastix.Document
      alias Search.Indices.BulkIndex

      @doc """
      Hook to allow for manipulation of the data / record before serialization.
      Result of the function gets passed to the serializer.
      """
      @impl true
      def before_serialize(record, _related_data), do: record

      @doc """
      Builds a payload to be indexed.
      """
      @impl true
      def build_payload(id) when is_binary(id) or is_integer(id) do
        record = repo().get(schema_module(), id)
        build_payload(record, nil)
      end

      @impl true
      def build_payload(record = %{__struct__: _}, related_data \\ nil) do
        record = repo().preload(record, preload_associations())
        record = before_serialize(record, related_data)

        serializer_module()
        |> JaSerializer.format(record, %{}, include: include())
        |> customize_payload(record)
        |> included_to_root()
        |> replace_illegal_values()
      end

      @doc """
      Convenience function to perform bulk indexing of the resource.
      """
      @impl true
      def bulk_index, do: bulk_index([])

      @impl true
      def bulk_index(opts) do
        BulkIndex.perform(__MODULE__, opts)
      end

      @impl true
      def customize_root_include(related, _resource), do: related

      @doc """
      Hook allows the implementing module to customize the payload before the data is sent to Elasticsearch
      for indexing.
      """
      @impl true
      def customize_payload(resource, record), do: resource

      @doc """
      Delete a document from search.
      """
      @spec delete(struct() | non_neg_integer | String.t()) :: {:ok | :error, %HTTPoison.Response{}}
      def delete(%{__struct__: _, id: id}), do: delete(id)

      def delete(id) when is_integer(id), do: delete(to_string(id))

      def delete(id) when is_binary(id) do
        Document.delete(Search.elasticsearch_url(), versioned_index_name(), type(), id)
      end

      @doc """
      Fetches the document from search by the document id.
      """
      @spec get(struct() | non_neg_integer | String.t()) :: {:ok | :error, %HTTPoison.Response{}}
      def get(%{__struct__: _, id: id}), do: get(id)

      def get(id) when is_binary(id) or is_integer(id) do
        Document.get(Search.elasticsearch_url(), versioned_index_name(), type(), id)
      end

      @doc """
      Indexes the document by sending the document to Elasticsearch.
      """
      @spec index(record :: struct | id :: non_neg_integer | id :: String.t()) ::
              {:ok | :error, %HTTPoison.Response{}}
      def index(record = %{__struct__: _, id: id}) do
        data = record |> build_payload()
        index = versioned_index_name()
        log_msg("Indexing #{index}, id: #{id}")
        Document.index(Search.elasticsearch_url(), index, type(), id, data)
      end

      def index(id) when is_binary(id) or is_integer(id) do
        record = repo().get(schema_module(), id)
        index(record)
      end

      @doc """
      Copies `included` data from the document, and nests the included data in the root of the document.
      Allows for mapping and filtering by related include data by denormalizing / splitting the `type` into
      it's own property so it can be mapped independently of all `includes`.
      """
      @spec included_to_root(resource :: map) :: map
      def included_to_root(resource) do
        related = root_included()

        nested_includes =
          related
          |> Enum.into(%{}, fn type ->
            related =
              resource
              |> Map.get("included", [])
              |> Enum.filter(fn include ->
                Map.get(include, "type") == type
              end)
              |> Enum.map(fn rel ->
                customize_root_include(rel, resource)
              end)

            {type, related}
          end)

        Map.merge(resource, nested_includes)
      end

      defoverridable before_serialize: 2, bulk_index: 1, customize_root_include: 2, customize_payload: 2
    end
  end
end
