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

  defmodule Connection do
    defstruct [:database_id, :db_conn, :client_pid, :monitor_ref, :timer_ref]
  end

  require Logger

  use GenServer

  alias Connection, as: Conn

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
    Logger.debug "checkout #{inspect opts}"
    conn = GenServer.call(pool, {:checkout, opts})

    { :ok, worker_ref, mod, state } = DBConnection.Connection.checkout(conn.db_conn, opts)
    pool_ref = { pool, conn, worker_ref }
    { :ok, pool_ref, mod, state }
  end

  def checkin(pool_ref, state, opts) do
    { name, conn, worker_ref } = pool_ref
    DBConnection.Connection.checkin(worker_ref, state, opts)
    GenServer.call name, {:checkin, conn}
  end

  # I do not know what this is or how it's supposed to work.
  # I basically copied the poolboy pool implementation.
  def disconnect(pool_ref, err, state, opts) do
    { name, conn, worker_ref } = pool_ref
    DBConnection.Connection.disconnect(worker_ref, err, state, opts)
    GenServer.call(name, {:checkin, conn})
    # checkin(pool_ref, state, opts)
  end

  # I do not know what this is or how it's supposed to work.
  # I basically copied the poolboy pool implementation.
  def stop(pool_ref, err, state, opts) do
    { _name, _conn, worker_ref } = pool_ref
    try do
      DBConnection.Connection.sync_stop(worker_ref, err, state, opts)
    after
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
    conn = new_connection(cache, :default)
    cache = %{ cache | available: [conn] }

    {:ok, cache}
  end

  def handle_call {:set_database, database_id}, {from, _}, cache do
    :ets.insert(cache.database_id_ets, {from, database_id})
    {:reply, {:ok, database_id}, cache}
  end

  def handle_call {:checkout, options}, {client_pid, _}, cache do
    Logger.debug "checkout by #{inspect client_pid}"

    database_id = get_database_id(cache, client_pid)
    timeout = Keyword.get(options, :timeout)

    conn = get_connection(cache, database_id)
    {cache, conn} = available_to_busy(cache, conn, client_pid, timeout)

    {:reply, conn, cache}
  end

  def handle_call {:checkin, conn}, {client_pid, _}, cache do
    Logger.debug "checkin by #{inspect client_pid}"

    cache = cache
      |> busy_to_available(conn)
      |> elem(0)
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
  def handle_info({:DOWN, _ref, :process, client_pid, _reason}, cache) do
    cache = Enum.reduce cache.busy, cache, fn conn, cache ->
      if conn.client_pid == client_pid do
        busy_to_available(cache, conn) |> elem(0)
      else
        cache
      end
    end
    {:noreply, cache}
  end

  def handle_info({:timeout, conn}, cache) do
    Logger.debug "Connection timeout: #{inspect conn}"
    cache = Enum.reduce cache.busy, cache, fn c, cache ->
      if c.client_pid == conn.client_pid && c.db_conn == c.db_conn do
        busy_to_available(cache, conn) |> elem(0)
      else
        cache
      end
    end
    {:noreply, cache}
  end

  defp get_connection(cache, database_id) do
    case find_connection(cache, database_id) do
      nil ->
        Logger.debug("connection cache MISS #{inspect database_id}")
        new_connection(cache, database_id)
      conn ->
        Logger.debug("connection cache HIT #{inspect database_id}")
        conn
    end
  end

  defp find_connection(cache, database_id) do
    Enum.find(cache.available, &(&1.database_id == database_id))
  end

  defp new_connection(cache, database_id) do
    conn_options = database_connection_options(cache.conn_options, database_id)
    {:ok, db_conn} = DBConnection.Connection.start_link(cache.conn_module, conn_options)
    %Conn{ database_id: database_id, db_conn: db_conn }
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

  defp database_connection_options(default_options, database_id) do
    case database_id do
      :default -> default_options
      _ -> Keyword.merge(default_options, database_id)
    end
  end

  defp prune(%{ available: available, size: size } = cache) when length(available) <= size, do: cache
  defp prune(%{ available: available, size: size } = cache) do

    pruning_fn = fn {conn, i}, memo ->
      if i < size do
        [conn | memo]
      else
        Logger.debug("expiring connection to #{inspect conn.database_id}")
        close_connection(conn.db_conn)
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

  def available_to_busy(cache, conn, client_pid, timeout) do
    # Track the client.
    conn = %{ conn | client_pid: client_pid }

    # Monitor the client.
    conn = %{ conn | monitor_ref: Process.monitor(client_pid) }

    # Set the timeout if needed.
    conn = set_timer(conn, timeout)

    # Remove from available list.
    available = List.delete(cache.available, make_available(conn))

    # Add to busy list.
    busy = [conn | cache.busy]

    # Update the cache
    cache = %{ cache | available: available, busy: busy }

    {cache, hd(busy)}
  end

  def busy_to_available(cache, conn) do
    case pop_busy(cache, conn) do
      {nil, cache} -> {cache, nil}

      {conn, cache} ->
        # Demonitor the client.
        Process.demonitor(conn.monitor_ref)

        # Cancel the timer.
        cancel_timer(conn.timer_ref)

        # Add to available.
        available = [make_available(conn) | cache.available]

        # Update the cache.
        cache = %{ cache | available: available }

        {cache, hd(available)}
    end
  end

  defp pop_busy(cache, conn) do
    case Enum.find(cache.busy, &(&1.db_conn == conn.db_conn)) do
      nil -> {nil, cache}
      conn -> {conn, %{ cache | busy: List.delete(cache.busy, conn) }}
    end
  end

  defp make_available(conn) do
    %{ conn | client_pid: nil, monitor_ref: nil, timer_ref: nil }
  end

  defp set_timer(conn, nil), do: conn
  defp set_timer(conn, timeout) do
    # The conn being sent won't have the timer_ref, but that's ok,
    # we don't need it in the timeout handler.
    timer_ref = Process.send_after(self, {:timeout, conn}, timeout)
    %{ conn | timer_ref: timer_ref }
  end

  defp cancel_timer(nil), do: nil
  defp cancel_timer(timer_ref), do: Process.cancel_timer(timer_ref)

end
