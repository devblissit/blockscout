defmodule Explorer.Counters.BlockValidationCounter do
  use GenServer

  @moduledoc """
  Module responsible for fetching and consolidating the number of
  validations from an address.
  """

  alias Explorer.Chain
  alias Explorer.Chain.Hash

  @table :block_validation_counter

  def table_name do
    @table
  end

  # It is undesirable to automatically start the consolidation in all environments.
  # Consider the test environment: if the consolidation initiates but does not
  # finish before a test ends, that test will fail. This way, hundreds of
  # tests were failing before disabling the consolidation and the scheduler in
  # the test env.
  config = Application.get_env(:explorer, Explorer.Counters.BlockValidationCounter)
  @enable_consolidation Keyword.get(config, :enable_consolidation)

  @doc """
  Creates a process to continually monitor the validation counts.
  """
  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  ## Server
  @impl true
  def init(args) do
    create_table()

    if enable_consolidation?() do
      Task.start_link(&consolidate_blocks/0)
    end

    Chain.subscribe_to_events(:blocks)

    {:ok, args}
  end

  def create_table do
    opts = [
      :set,
      :named_table,
      :public,
      read_concurrency: true
    ]

    :ets.new(table_name(), opts)
  end

  @doc """
  Consolidates the number of block validations grouped by `address_hash`.
  """
  def consolidate_blocks do
    Chain.each_address_block_validation_count(fn {address_hash, total} ->
      insert_or_update_counter(address_hash, total)
    end)
  end

  @doc """
  Fetches the number of validations related to an `address_hash`.
  """
  @spec fetch(Hash.Address.t()) :: non_neg_integer
  def fetch(addr_hash) do
    do_fetch(:ets.lookup(table_name(), to_string(addr_hash)))
  end

  defp do_fetch([{_, result} | _]), do: result
  defp do_fetch([]), do: 0

  @impl true
  def handle_info({:chain_event, :blocks, _type, blocks}, state) do
    blocks
    |> Enum.map(& &1.miner_hash)
    |> Enum.each(&insert_or_update_counter(&1, 1))

    {:noreply, state}
  end

  @doc """
  Inserts a new item into the `:ets` table.

  When the record exist, the counter will be incremented by one. When the
  record does not exist, the counter will be inserted with a default value.
  """
  @spec insert_or_update_counter(Hash.Address.t(), non_neg_integer) :: term()
  def insert_or_update_counter(addr_hash, number) do
    string_addr = to_string(addr_hash)
    default = {string_addr, 0}

    :ets.update_counter(table_name(), string_addr, number, default)
  end

  @doc """
  Returns a boolean that indicates whether consolidation is enabled

  In order to choose whether or not to enable the scheduler and the initial
  consolidation, change the following Explorer config:

  `config :explorer, Explorer.Counters.BlockValidationCounter, enable_consolidation: true`

  to:

  `config :explorer, Explorer.Counters.BlockValidationCounter, enable_consolidation: false`
  """
  def enable_consolidation?, do: @enable_consolidation
end
