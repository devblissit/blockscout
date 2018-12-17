defmodule Explorer.Validator.MetadataProcessor do
  @moduledoc """
  module to periodically retrieve and update metadata belonging to validators
  """
  use GenServer
  alias Explorer.Validator.{MetadataImporter, MetadataRetriever}

  def start_link(_) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(args) do
    import_and_reschedule()
    {:ok, args}
  end

  @impl true
  def handle_info(:import_and_reschedule, state) do
    import_and_reschedule()

    {:noreply, state}
  end

  defp import_and_reschedule do
    MetadataRetriever.fetch_data()
    |> MetadataImporter.import_metadata()

    reschedule()
  end

  defp reschedule do
    Process.send_after(self(), :import_and_reschedule, :timer.hours(24))
  end
end
