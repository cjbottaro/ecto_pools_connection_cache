defmodule Ecto.Pools.ConnectionCache do

  defstruct [
    name:             nil,
    conn_options:     nil,
    conn_module:      nil,
    busy:             [], # [ {owner_pid, database_id, db_conn} ]
    available:        [], # [ {database_id, db_conn} ]
    database_id_ets:  nil,
    size:             nil,
    monitors:         %{}
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
    gen_server_options = Keyword.take(options, [:name])
    GenServer.start_link(__MODULE__, {module, options}, gen_server_options)
  end

  def checkout(pool, opts) do
    IO.puts "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
    conn = GenServer.call(pool, {:checkout, opts})
    IO.puts "conn #{inspect conn}, opts #{inspect opts}"

    { :ok, worker_ref, mod, state } = DBConnection.Connection.checkout(conn, opts)
    pool_ref = { pool, conn, worker_ref }
    IO.puts "done checkout #{inspect pool_ref}"
    { :ok, pool_ref, mod, state }
  end

  def checkin(pool_ref, state, opts) do
    IO.puts "!!!!!!!!!!! checkin #{inspect pool_ref}"
    { name, conn, worker_ref } = pool_ref
    DBConnection.Connection.checkin(worker_ref, state, opts)
    GenServer.call name, {:checkin, conn}
  end

  # I do not know what this is or how it's supposed to work.
  # I basically copied the poolboy pool implementation.
  def disconnect(pool_ref, err, state, opts) do
    IO.puts "!!!!!!!!!!!!!! disconnect"
    { name, conn, worker_ref } = pool_ref
    DBConnection.Connection.disconnect(worker_ref, err, state, opts)
    GenServer.call(name, {:checkin, conn})
    # checkin(pool_ref, state, opts)
  end

  # I do not know what this is or how it's supposed to work.
  # I basically copied the poolboy pool implementation.
  def stop(pool_ref, err, state, opts) do
    IO.puts "!!!!!!!!!!!!!! stop"
    { name, _conn, worker_ref } = pool_ref
    try do
      DBConnection.Connection.sync_stop(worker_ref, err, state, opts)
    after
      {pool_pid, _, _} = pool_ref
      checkin(pool_ref, state, opts)
    end
  end

  def init {conn_module, conn_options} do
    Process.flag(:trap_exit, true)

    name = Keyword.get(conn_options, :name, self)
    size = Keyword.get(conn_options, :pool_size, 10)

    cache = %__MODULE__{
      name: name,
      conn_module: conn_module,
      conn_options: conn_options,
      database_id_ets: :ets.new(nil, []),
      size: size
    }

    # Make an initial connection to default database
    # and put it in our available queue. This is
    # to satisy integration tests
    db_conn = new_db_conn(cache, :default)
    cache = push_available(cache, {:default, db_conn})

    {:ok, cache}
  end

  def handle_call {:set_database, database_id}, {from, _}, cache do
    :ets.insert(cache.database_id_ets, {from, database_id})
    {:reply, {:ok, database_id}, cache}
  end

  def handle_call {:checkout, opts}, {from, _}, cache do
    {cache, db_conn} = cache
      |> monitor_client(from)
      |> checkout_db_conn(from)
    {:reply, db_conn, cache}
  end

  def handle_call {:checkin, db_conn}, {from, _}, cache do
    cache = cache
      |> demonitor_client(from)
      |> checkin_db_conn(db_conn)
      |> prune
    {:reply, :ok, cache}
  end

  # If our DBConnection.Connection died, then we need to detect that
  # and remove it from available and busy queues.
  def handle_info({:EXIT, db_conn, _reason}, cache) do
    # %{ available: available, busy: busy } = cache
    # {_, cache} = pop_available(cache, db_conn)
    # {_, cache} = pop_busy(cache, db_conn)
    #
    # %{ available: available, busy: busy } = cache
    # cache =
    #   if available == [] && busy == [] do
    #     db_conn = new_db_conn(cache, :default)
    #     push_available(cache, {:default, db_conn})
    #   else
    #     cache
    #   end
    #
    {:noreply, cache}
  end

  # If our client (the process that called checkout) dies, then
  # we need to handle that.
  def handle_info({:DOWN, _ref, :process, pid, _reason}, cache) do
    IO.puts "!!!!!!!!!!!!! :DOWN #{inspect pid}"
    # {conn, cache} = pop_busy(cache, pid)
    # {_pid, database_id, db_conn} = conn
    # cache = push_available(cache, {database_id, db_conn})
    {:noreply, cache}
  end

  defp checkout_db_conn(cache, from_pid) do
    database_id = get_database_id(cache, from_pid)
    IO.puts "!!!!!!!!!! get_database_id: #{inspect database_id}"

    {cache, db_conn} =
      case pop_available(cache, database_id) do
        {nil, cache} ->
          Logger.debug("connection cache MISS #{inspect database_id}")
          db_conn = new_db_conn(cache, database_id)
          {cache, db_conn}
        {conn, cache} ->
          Logger.debug("connection cache HIT #{inspect database_id}")
          {_, db_conn} = conn
          {cache, db_conn}
      end

    cache = push_busy(cache, from_pid, {database_id, db_conn})

    {cache, db_conn}
  end

  defp checkin_db_conn(cache, db_conn) do
    IO.puts "checkin_db_conn: #{inspect cache} #{inspect db_conn}"
    case pop_busy(cache, db_conn) do
      {nil, cache} ->
        cache
      {{_pid, database_id, db_conn}, cache}  ->
        push_available(cache, {database_id, db_conn})
    end
  end

  defp get_database_id(cache, from_pid) do
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

  defp new_db_conn(cache, database_id) do
    conn_options = database_connection_options(cache.conn_options, database_id)
    {:ok, db_conn} = DBConnection.Connection.start_link(cache.conn_module, conn_options)
    db_conn
  end

  defp database_connection_options(default_options, database_id) do
    case database_id do
      :default -> default_options
      _ -> Keyword.merge(default_options, database_id)
    end
  end

  def push_available(%{ available: available } = cache, conn) do
    %{ cache | available: [conn | available] }
  end

  defp pop_available(%{ available: available } = cache, database_id) when is_atom(database_id) do
    conn = List.keyfind(available, database_id, 0)
    pop_available(cache, conn)
  end

  defp pop_available(%{ available: available } = cache, db_conn) when is_pid(db_conn) do
    conn = List.keyfind(available, db_conn, 1)
    pop_available(cache, conn)
  end

  defp pop_available(cache, nil), do: {nil, cache}
  defp pop_available(%{ available: available } = cache, conn) when is_tuple(conn) do
    if Enum.member?(available, conn) do
      {conn, %{ cache | available: List.delete(available, conn) }}
    else
      {nil, cache}
    end
  end

  def push_busy(%{ busy: busy } = cache, pid, {database_id, db_conn}) do
    %{ cache | busy: [ {pid, database_id, db_conn} | busy ] }
  end

  def pop_busy(%{ busy: busy } = cache, db_conn) do
    conn = List.keyfind(busy, db_conn, 2)
    busy = List.keydelete(busy, db_conn, 2)
    {conn, %{ cache | busy: busy }}
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

  defp monitor_client(cache, pid) do
    ref = Process.monitor(pid)
    %{ cache | monitors: Map.put(cache.monitors, pid, ref) }
  end

  defp demonitor_client(cache, pid) do
    {ref, monitors} = Map.pop(cache.monitors, pid)
    Process.demonitor(ref)
    %{ cache | monitors: monitors }
  end

end
