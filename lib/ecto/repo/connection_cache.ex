defmodule Ecto.Repo.ConnectionCache do

  defmacro __using__(options) do
    quote do
      use Ecto.Repo, unquote(options)
      def set_database(database) do
        Ecto.Repo.ConnectionCache.set_database(__MODULE__, database)
      end
    end
  end

  def set_database(module, database) do
    pool_name = "#{module}.Pool" |> String.to_atom
    database = case database do
      database when is_list(database) -> normalize_database(database)
      database when is_binary(database) -> normalize_database(database: database)
      :default -> :default
      nil -> :default
      _ -> raise ArgumentError, "expected keyword list, string, nil or :default"
    end
    GenServer.call pool_name, {:set_database, database}
  end

  defp normalize_database(keywords) do
    keywords |> Enum.uniq |> Enum.sort
  end

end
