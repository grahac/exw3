defmodule ExW3.Logs do
  use GenServer
  require Logger

  alias ExW3.Rpc

  @type log_filter :: %{
          optional(:fromBlock) => String.t(),
          optional(:address) => String.t(),
          optional(:topics) => [String.t()]
        }

  ### Client

  @spec start(filters :: log_filter()) :: :ignore | {:error, any} | {:ok, pid}
  def start(filters) do
    GenServer.start(__MODULE__, [filters])
  end

  @spec start_link(filters :: log_filter()) :: :ignore | {:error, any} | {:ok, pid}
  def start_link(filters) do
    GenServer.start_link(__MODULE__, [filters])
  end

  def stop(pid) do
    GenServer.stop(pid)
  end

  def add_recipient(pid, sub_pid) do
    GenServer.cast(pid, {:add_recipient, sub_pid})
  end

  ### Server

  defmodule State do
    defstruct filters: nil,
              last_block: 0,
              recipients: MapSet.new(),
              sub_id: nil

    def new(last_block), do: %__MODULE__{last_block: last_block}

    def set_last_block(state = %__MODULE__{}, block) do
      %{state | last_block: block}
    end

    def set_filters(state = %__MODULE__{}, filters) do
      %{state | filters: filters}
    end

    def add_recipient(state = %__MODULE__{recipients: recipients}, pid) do
      %{state | recipients: MapSet.put(recipients, pid)}
    end

    def set_sub_id(state = %__MODULE__{}, sub_id) do
      %{state | sub_id: sub_id}
    end
  end

  def init([filters]) do
    {:ok, block} = Rpc.block_number()
    {:ok, sub_id} = Rpc.eth_subscribe(["logs", filters], self())

    state =
      State.new(block)
      |> State.set_filters(filters)
      |> State.set_sub_id(sub_id)

    {:ok, state}
  end

  def terminate(_reason, %State{sub_id: sub_id}) do
    Rpc.eth_unsubscribe(sub_id)
  end

  def handle_cast({:add_recipient, pid}, state) do
    state = State.add_recipient(state, pid)
    {:noreply, state}
  end

  def handle_info({:eth_subscription, message}, state = %State{}) do
    Logger.debug("received eth_subscription: #{inspect(message)}")
    %{"params" => %{"result" => result = %{"blockNumber" => block}}} = message
    state = State.set_last_block(state, block)

    Enum.each(state.recipients, fn recipient ->
      send(recipient, {:eth_log, result})
    end)

    {:noreply, state}
  end

  def handle_info({:resubscribe, %{"result" => sub_id}}, state = %State{filters: filters}) do
    Logger.debug("received resubscribe")
    # the socket has disconnected and now re-connected, so fetch the logs since we've been offline
    # and send to recipients
    {:ok, block} = Rpc.block_number()

    state =
      state
      |> State.set_last_block(block)
      |> State.set_sub_id(sub_id)

    {:ok, logs} =
      filters
      |> Map.put(:fromBlock, state.last_block + 1)
      |> Rpc.get_logs()

    for log <- logs,
        recipient <- state.recipients do
      send(recipient, {:eth_log, log})
    end

    {:noreply, state}
  end
end
