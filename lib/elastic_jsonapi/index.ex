defmodule Search.Index do
  @moduledoc """
  Provides a behavior and common functions for managing an Elasticsearch Index.
  """
  require Logger

  import Search.IndexAlias
  import Utils, only: [module_name: 1]

  use Retry

  alias Elastix.HTTP

  # dialyzer can't figure out the return value from the retry block
  @dialyzer {:nowarn_function, update_aliases: 1}

  @doc """
  Base name of the index. Non-versioned, human readable name.
  Used in aliasing the index names.
  """
  @callback index_name() :: String.t()

  @doc """
  Module responsible for handling Mapping logic
  """
  @callback mapping_module :: module

  defmacro index(config) do
    define_index(config)
  end

  defp define_index(config) do
    aliases = config[:aliases]

    quote location: :keep do
      @doc """
      Define the types of aliases to create for an Index.
      """
      def aliases, do: unquote(aliases)
    end
  end

  @doc """
  Update existing indices that define an alias corresponding to the struct being passed.
  """
  def update_aliases(struct = %{__struct__: _}) do
    added_aliases =
      Enum.map(index_modules_using_alias(struct), fn module ->
        index_name = module.versioned_index_name()
        {alias_name, %{filter: filter}} = struct |> alias_filter(index_name)
        %{add: %{index: index_name, alias: alias_name, filter: filter}}
      end)

    # exponential backoff. expires after 10 seconds
    response =
      retry(with: exp_backoff() |> randomize() |> expiry(10_000)) do
        Elastix.Index.update_aliases(Search.elasticsearch_url(), %{actions: added_aliases})
      after
        result -> result
      else
        error -> error
      end

    Logger.info(fn ->
      "Update Alias Response => #{inspect(response)}"
    end)

    maybe_notify_update_aliases(response)
  end

  @doc """
  All the child modules using the Index behaviour.
  """
  @spec index_modules() :: [module]
  def index_modules do
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
  Filters all the index modules to a list of modules that implement the alias corresponding to the struct
  being passed.
  """
  @spec index_modules_using_alias(struct()) :: list(module())
  def index_modules_using_alias(struct) do
    modules = index_modules()

    Enum.filter(modules, fn module ->
      alias_atom = struct_to_alias_atom(struct)
      aliases = module.aliases()
      Keyword.get(aliases, alias_atom, false)
    end)
  end

  @doc """
  Helper function to transform a struct into an atom name.
  The atom name corresponds to the configured alias name.
  """
  @spec struct_to_alias_atom(struct()) :: atom
  def struct_to_alias_atom(struct) do
    struct
    |> Map.get(:__struct__)
    |> to_string()
    |> String.split(".")
    |> List.last()
    |> Macro.underscore()
    |> String.to_existing_atom()
  end

  @spec maybe_notify_update_aliases(HTTP.resp()) :: HTTP.resp()
  defp maybe_notify_update_aliases(resp = {:ok, _}), do: resp

  defp maybe_notify_update_aliases(resp = {:error, response}) do
    Honeybadger.notify("#{module_name(__MODULE__)}Failed update_aliases/1", %{
      update_aliases_response: inspect(response)
    })

    resp
  end

  defmacro __using__(_) do
    quote location: :keep do
      alias Elastix.Index

      import Search.Index
      import Search.IndexAlias

      ## Behaviour
      @behaviour Search.Index

      ## Public functions

      @doc """
      An alternative name for a real index. Alias index can be used like a true index.
      """
      def alias_name(struct) do
        alias_name(struct, versioned_index_name())
      end

      @doc """
      Create a search index.
      """
      @spec create_index() :: {:ok | :error, %HTTPoison.Response{}}
      @spec create_index(aliases :: map) :: {:ok | :error, %HTTPoison.Response{}}
      def create_index do
        index_name = versioned_index_name()

        index_name
        |> all_alias_filters(aliases())
        |> create_index()
      end

      def create_index(aliases = %{}) do
        Index.create(Search.elasticsearch_url(), versioned_index_name(), create_index_payload(aliases))
      end

      @doc """
      Get info about an existing index
      """
      @spec get() :: {:ok | :error, %HTTPoison.Response{}}
      def get do
        Index.get(Search.elasticsearch_url(), versioned_index_name())
      end

      @doc """
      A version name is prepended to the index name.
      Useful for migrating indices when there are incompatible changes.
      """
      @spec versioned_index_name() :: String.t()
      def versioned_index_name do
        Search.stack_name() <> "_" <> index_name()
      end

      ## Private functions

      defp create_index_payload(aliases) do
        %{
          "aliases" => aliases,
          "mappings" => %{mapping_module().type() => mapping_module().build_payload()},
          "settings" => %{
            "analysis" => %{
              "filter" => %{
                "autocomplete_filter" => %{
                  "type" => "edge_ngram",
                  "min_gram" => 2,
                  "max_gram" => 15
                }
              },
              "analyzer" => %{
                "autocomplete" => %{
                  "type" => "custom",
                  "tokenizer" => "standard",
                  "filter" => ~w(lowercase autocomplete_filter)s
                }
              }
            },
            "index" => %{
              # use caution when adjusting this value. causes ES to scan through the index and consume resources.
              # https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html
              "max_result_window" => 20000,
              "number_of_replicas" => 0,
              "number_of_shards" => 1
            }
          }
        }
      end
    end
  end
end
