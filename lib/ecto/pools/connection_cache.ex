defmodule Ecto.Pools.ConnectionCache do

  defstruct [
    name:             nil,
    conn_options:     nil,
    conn_module:      nil,
    busy:             [],
    available:        [],
    database_id_ets:  nil,
    size:             nil
  ]

  require Logger
  use GenServer

  @behaviour DBConnection.Pool

  def ensure_all_started(_options, _type) do
    {:ok, []}
  end

  def child_spec(module, options, child_options) do
    Supervisor.Spec.worker(__MODULE__, [module, options], child_options)
  end

  def start_link(module, options) do
    {name, options} = Keyword.pop(options, :name)
    GenServer.start_link(__MODULE__, {name, module, options}, name: name)
  end

  def checkout(pool, opts) do
    GenServer.call(pool, {:checkout, opts})
  end

  def checkin(pool_ref, state, opts) do
    { name, _, _ } = pool_ref
    GenServer.call name, {:checkin, pool_ref, state, opts}
  end

  def disconnect(pool_ref, err, state, opts) do
    { name, conn, worker_ref } = pool_ref
    DBConnection.Connection.disconnect(worker_ref, err, state, opts)
    checkin(pool_ref, state, opts)
  end

  def stop(pool_ref, err, state, opts) do
    { name, conn, worker_ref } = pool_ref
    DBConnection.Connection.sync_stop(worker_ref, err, state, opts)
    checkin(pool_ref, state, opts)
  end

  def init {name, conn_module, options} do
    conn_options = Keyword.take(options, ~w(hostname port database username password extensions)a)
      |> Keyword.put(:types, true)
      |> Enum.uniq

    size = Keyword.get(options, :pool_size, 10)

    cache = %__MODULE__{
      name: name,
      conn_module: conn_module,
      conn_options: conn_options,
      database_id_ets: :ets.new(name, []),
      size: size
    }

    {:ok, cache}
  end

  def handle_call {:set_database, database_name}, from, cache do
    {pid, _ref} = from
    :ets.insert(cache.database_id_ets, {pid, database_name})
    {:reply, {:ok, database_name}, cache}
  end

  def handle_call {:checkout, opts}, from, cache do
    {from_pid, _from_ref} = from
    {conn, cache} = get_db_connection(cache, from_pid)

    { :ok, worker_ref, mod, state } = DBConnection.Connection.checkout(conn, opts)
    pool_ref = { cache.name, conn, worker_ref }
    {:reply, { :ok, pool_ref, mod, state }, cache}
  end

  def handle_call {:checkin, pool_ref, state, opts}, _from, cache do
    # Check in the worker first.
    { _name, conn, worker_ref } = pool_ref
    DBConnection.Connection.checkin(worker_ref, state, opts)

    # Find the cache item in our busy list.
    cache_item = Enum.find cache.busy, fn {_, c} -> c == conn end

    cache = cache
      |> remove(:busy, cache_item)
      |> add(:available, cache_item)
      |> prune

    {:reply, :ok, cache}
  end

  defp get_db_connection(cache, from_pid) do
    %{ available: available } = cache
    database_id = database_id(cache, from_pid)
    { _database_id, db_conn } = List.keyfind(available, database_id, 0, {nil, nil})
    if db_conn do
      get_available_connection(cache, database_id, db_conn)
    else
      get_new_connection(cache, database_id)
    end
  end

  defp get_new_connection(cache, database_id) do
    Logger.debug "creating connection to #{inspect database_id}"

    %{ conn_module: conn_module, conn_options: conn_options } = cache

    conn_options = database_connection_options(conn_options, database_id)

    db_conn = case DBConnection.Connection.start_link(conn_module, conn_options) do
      { :ok, conn } -> conn
      _ -> raise "Umm, what do I do??"
    end

    cache = cache |> add(:busy, {database_id, db_conn})

    {db_conn, cache}
  end

  defp get_available_connection(cache, database_id, db_conn) do
    Logger.debug "getting connection for #{inspect database_id}"

    cache_item = {database_id, db_conn}

    cache = cache
      |> remove(:available, cache_item)
      |> add(:busy, cache_item)

    {db_conn, cache}
  end

  defp database_id(cache, from_pid) do
    id = case :ets.lookup(cache.database_id_ets, from_pid) do
      [] -> nil
      [{_pid, id} | _] -> id
    end

    if !id do
      :default
    else
      id
    end
  end

  defp database_connection_options(default_options, database_id) do
    case database_id do
      :default -> default_options
      _ -> Keyword.merge(default_options, database_id)
    end
  end

  defp add(cache, which, item) do
    %{ cache | which => [item | Map.get(cache, which)] }
  end

  defp remove(cache, which, item) do
    %{ cache | which => List.delete(Map.get(cache, which), item) }
  end

  defp prune(%{ available: available, size: size } = cache) when length(available) <= size, do: cache
  defp prune(%{ available: available, size: size } = cache) do

    pruning_fn = fn {cache_item, i}, memo ->
      if i < size do
        [cache_item | memo]
      else
        {database, db_conn} = cache_item
        Logger.debug("expiring connection to #{inspect database}")
        close_connection(db_conn)
        memo
      end
    end

    available = available
      |> Enum.with_index
      |> Enum.reduce([], pruning_fn)
      |> Enum.reverse

    %{ cache | available: available }
  end

  # Not sure how else to do this.
  defp close_connection(db_conn) do
    Process.unlink(db_conn)
    Process.exit(db_conn, :shutdown)
  end

end
