defmodule Explorer.SortingHelper do
  require Explorer.SortingHelper

  @doc """

  """

  import Ecto.Query

  def apply_sorting(query, sorting, default_sorting) when is_list(sorting) and is_list(default_sorting) do
    sorting |> sorting_with_defaults(default_sorting) |> apply_as(query)
  end

  def sorting_with_defaults(sorting, default_sorting) when is_list(sorting) and is_list(default_sorting) do
    (sorting ++ default_sorting)
    |> Enum.uniq_by(fn
      {_, field} -> field
      {_, field, as} -> {field, as}
    end)
  end

  defp apply_as(sorting, query) do
    sorting
    |> Enum.reduce(query, fn
      {order, column, binding}, query -> query |> order_by([{^order, field(as(^binding), ^column)}])
      no_binding, query -> query |> order_by(^[no_binding])
    end)
  end

  def page_with_sorting(sorting, default_sorting) do
    sorting |> sorting_with_defaults(default_sorting) |> do_page_with_sorting()
  end

  defp do_page_with_sorting([{order, column} | rest]) do
    fn key -> page_by_column(key, column, order, do_page_with_sorting(rest)) end
  end

  defp do_page_with_sorting([{order, column, binding} | rest]) do
    fn key -> page_by_column(key, {column, binding}, order, do_page_with_sorting(rest)) end
  end

  defp do_page_with_sorting([]), do: nil


  # we could use here some function like
  # defp apply_column({column, binding}) do
  #   dynamic([t], field(as(^binding), ^column))
  # end

  # defp apply_column(column) do
  #   dynamic([t], field(t, ^column))
  # end
  # but at the moment using such dynamic in comparisons lead ecto to
  # failure in type inference from scheme and it expects some defaults types
  # like string instead of `Hash.Address`
  defp page_by_column(key, column, :desc_nulls_last, next_column) do
    key
    |> get_column_from_key(column)
    |> case do
      nil ->
        dynamic([t], is_nil(^apply_column(column)) and ^apply_next_column(next_column, key))

      value ->
        dynamic(
          [t],
          is_nil(^apply_column(column)) or ^apply_column(column) < ^value or
            (^apply_column(column) == ^value and ^apply_next_column(next_column, key))
        )
    end
  end

  defp page_by_column(key, column, :desc_nulls_last, next_column) do
    key
    |> get_column_from_key(column)
    |> case do
      nil ->
        dynamic([t], is_nil(^apply_column(column)) and ^apply_next_column(next_column, key))

      value ->
        dynamic(
          [t],
          is_nil(field(t, ^column)) or ^apply_column(column) < ^value or
            (field(t, ^column) == ^value and ^apply_next_column(next_column, key))
        )
    end
  end

  defp page_by_column(key, column, :asc_nulls_first, next_column) do
    key
    |> get_column_from_key(column)
    |> case do
      nil ->
        apply_next_column(next_column, key)

      value ->
        dynamic(
          [t],
          not is_nil(^apply_column(column)) and
            (^apply_column(column) > ^value or
               (^apply_column(column) == ^value and ^apply_next_column(next_column, key)))
        )
    end
  end

  defp page_by_column(key, column, :asc, next_column) do
    key
    |> get_column_from_key(column)
    |> case do
      nil ->
        dynamic([t], is_nil(^apply_column(column)) and ^apply_next_column(next_column, key))

      value ->
        dynamic(
          [t],
          is_nil(field(t, ^column)) or
            (^apply_column(column) > ^value or
               (^apply_column(column) == ^value and ^apply_next_column(next_column, key)))
        )
        |> dbg()
    end
  end

  defp page_by_column(key, column, :desc, next_column) do
    key
    |> get_column_from_key(column)
    |> case do
      nil ->
        apply_next_column(next_column, key)

      value ->
        dynamic(
          [t],
          not is_nil(^apply_column(column)) and
            (^apply_column(column) < ^value or
               (^apply_column(column) == ^value and ^apply_next_column(next_column, key)))
        )
    end
  end

  defp get_column_from_key(key, {column, _}), do: key[column]
  defp get_column_from_key(key, column), do: key[column]

  defp apply_next_column(nil, _key) do
    false
  end

  defp apply_next_column(next_column, key) do
    next_column.(key)
  end
end
