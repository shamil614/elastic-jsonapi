defmodule Search.Paginator do
  @moduledoc """
  Handles the formatting of JSONAPI pagination params to Elasticsearch from and size.
  Also translates the the result counts to the proper page links for the response.
  """

  import Destructure

  alias Plug.Conn
  alias Plug.Conn.Query, as: ConnQuery

  @default_page_size 10
  @max_page_size 100

  @typedoc """
  * from - the starting position for the pagination results
  * size - the number of results for the pagination to display
  """
  @type offset :: %{from: non_neg_integer, size: non_neg_integer}

  @typedoc """
  Mapping of the types of pages for pagination and the links to access those pages.
  """
  @type page_links :: %{
          required(:first) => non_neg_integer,
          required(:last) => non_neg_integer,
          optional(:prev) => non_neg_integer | nil,
          optional(:next) => non_neg_integer | nil
        }

  @typedoc """
  Mapping of the types of pages for pagination and the numbers of the pages.
  """
  @type page_numbers :: %{
          required(:first) => non_neg_integer,
          required(:last) => non_neg_integer,
          required(:prev) => non_neg_integer | nil,
          required(:next) => non_neg_integer | nil
        }

  ## Public functions

  @doc """
  Converts page number and size params to values Elasticsearch uses for pagination
  """
  @spec page_to_offset(params :: map()) :: offset()
  def page_to_offset(params = %{}) do
    size = get_in(params, ["page", "size"])
    number = get_in(params, ["page", "number"])

    size = verify_page_size(size)
    from = page_number_into_from(number, size)

    %{from: from, size: size}
  end

  @doc """
  Takes the params from the Conn and the total number of hits (results) and generates pagination links.
  """
  @spec to_page_links(Conn.t(), non_neg_integer) :: page_links()
  def to_page_links(conn = %Conn{params: params}, total_hits) do
    %{from: from, size: size} = page_to_offset(params)
    pages = Enum.chunk_every(0..(total_hits - 1), size)
    first_page_number = 1
    last_page_number = Enum.count(pages)
    current_page_number = determine_current_page_number(pages, from)

    {prev, next} = prev_next_number(current_page_number, last_page_number)

    numbers = %{first: first_page_number, last: last_page_number, next: next, prev: prev}
    page_numbers_to_links(numbers, size, conn)
  end

  ## Public but not documented functions

  # Left function public to be able to unit test.
  @doc false
  @spec prev_next_number(current :: non_neg_integer, last :: non_neg_integer, first :: non_neg_integer) ::
          {prev :: non_neg_integer | nil, next :: non_neg_integer | nil}
  def prev_next_number(current, last), do: prev_next_number(current, last, 1)

  def prev_next_number(_, 1, 1), do: {nil, nil}
  def prev_next_number(1, _last, 1), do: {nil, 2}
  def prev_next_number(current, last, 1) when current == last, do: {current - 1, nil}
  def prev_next_number(current, _last, 1), do: {current - 1, current + 1}

  ## Private functions

  @spec determine_current_page_number(list(list(non_neg_integer)), non_neg_integer) :: non_neg_integer
  defp determine_current_page_number(pages, from) do
    Enum.reduce_while(pages, 0, fn page, acc ->
      if Enum.member?(page, from) do
        {:halt, acc + 1}
      else
        {:cont, acc + 1}
      end
    end)
  end

  @spec verify_page_size(size :: String.t() | integer) :: non_neg_integer
  defp verify_page_size(size) when is_binary(size) do
    size |> String.to_integer() |> verify_page_size()
  end

  defp verify_page_size(nil), do: @default_page_size
  defp verify_page_size(0), do: @default_page_size
  defp verify_page_size(size) when is_integer(size) and size < 0, do: @default_page_size
  defp verify_page_size(size) when is_integer(size) and size > @max_page_size, do: @max_page_size
  defp verify_page_size(size) when is_integer(size) and size <= @max_page_size, do: size

  @spec page_number_into_from(number :: String.t() | integer, size :: non_neg_integer) :: non_neg_integer
  defp page_number_into_from(number, size) when is_binary(number) and is_integer(size) do
    number |> String.to_integer() |> page_number_into_from(size)
  end

  defp page_number_into_from(nil, _size), do: 0
  defp page_number_into_from(0, _size), do: 0
  defp page_number_into_from(1, _size), do: 0
  defp page_number_into_from(number, _size) when is_integer(number) and number < 0, do: 0

  defp page_number_into_from(number, size) when is_integer(number) and is_integer(size) do
    size * (number - 1)
  end

  @spec page_numbers_to_links(numbers :: page_numbers(), size :: non_neg_integer, Conn.t()) :: page_links()
  defp page_numbers_to_links(numbers, size, conn = d(%Conn{params})) do
    url = build_url(conn)

    Enum.reduce(numbers, %{}, fn {type, number}, acc ->
      if number do
        encoded_params =
          params
          |> Map.put("page", %{"size" => size, "number" => number})
          |> ConnQuery.encode()
          |> String.replace("[", "%5B")
          |> String.replace("]", "%5D")

        link = url <> "?" <> encoded_params
        Map.put(acc, type, link)
      else
        acc
      end
    end)
  end

  @spec build_url(Conn.t()) :: String.t()
  defp build_url(d(%Conn{host, request_path, req_headers, scheme})) do
    fallback = "#{scheme}://#{host}#{request_path}"

    Enum.find_value(req_headers, fallback, fn {header, value} ->
      if header == "x-forwarded-url" do
        value
      end
    end)
  end
end
