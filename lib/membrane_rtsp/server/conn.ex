defmodule Membrane.RTSP.Server.Conn do
  @moduledoc false
  use GenServer

  alias Membrane.RTSP.Request
  alias Membrane.RTSP.Server.Logic

  # Upper bound on the size of a single request (headers and body). Without it a client
  # could make the connection process accumulate data indefinitely by never terminating
  # the header section or by announcing a huge Content-Length.
  @max_request_size 64 * 1024

  @spec start(map()) :: GenServer.on_start()
  def start(state) do
    GenServer.start(__MODULE__, state)
  end

  @impl true
  def init(config) do
    state = %Logic.State{
      socket: config.socket,
      request_handler: config.handler,
      request_handler_state:
        config.handler.handle_open_connection(config.socket, config.handler_state),
      rtp_socket: config.udp_rtp_socket,
      rtcp_socket: config.udp_rtcp_socket,
      session_timeout: config.session_timeout
    }

    {:ok, state, {:continue, :process_client_requests}}
  end

  @impl true
  def handle_continue(:process_client_requests, state) do
    case do_process_client_requests(state, state.session_timeout) do
      %Logic.State{recording_with_tcp?: true} = state ->
        {:noreply, state}

      {:error, _reason, state} ->
        state.request_handler.handle_closed_connection(state.request_handler_state)
        {:stop, :normal, state}
    end
  end

  @impl true
  def handle_info({:rtsp, raw_rtsp_request}, state) do
    with {:ok, request} <- Request.parse(raw_rtsp_request) do
      case Logic.process_request(request, state) do
        %Logic.State{recording_with_tcp?: true} = state ->
          {:noreply, state}

        state ->
          handle_continue(:process_client_requests, state)
      end
    else
      {:error, error} ->
        raise "Failed to parse RTSP request: #{inspect(error)}.\nRequest: #{inspect(raw_rtsp_request)}"
    end
  end

  defp do_process_client_requests(state, timeout) do
    case get_request(state.socket, timeout) do
      {:ok, request} ->
        case Logic.process_request(request, state) do
          %Logic.State{recording_with_tcp?: true} = state ->
            state

          %Logic.State{session_state: :recording} = state ->
            do_process_client_requests(state, :infinity)

          state ->
            do_process_client_requests(state, state.session_timeout)
        end

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp get_request(socket, timeout, acc \\ "") do
    with {:ok, acc} <- do_recv(socket, timeout, acc),
         :ok <- check_request_size(acc) do
      headers_and_body = String.split(acc, ~r/\r?\n\r?\n/, parts: 2)

      case do_parse_request(headers_and_body) do
        :more -> get_request(socket, timeout, acc)
        other -> other
      end
    end
  end

  defp do_recv(socket, timeout, acc) do
    case :gen_tcp.recv(socket, 0, timeout) do
      {:ok, data} -> {:ok, acc <> data}
      {:error, reason} -> {:error, reason}
    end
  end

  defp check_request_size(data) when byte_size(data) <= @max_request_size, do: :ok
  defp check_request_size(_data), do: {:error, :request_too_large}

  defp do_parse_request([raw_request, body]) do
    with {:ok, %Request{} = request} <- Request.parse(raw_request <> "\r\n\r\n"),
         {:ok, content_length} <- get_content_length(request) do
      cond do
        content_length > @max_request_size ->
          {:error, :request_too_large}

        byte_size(body) >= content_length ->
          {:ok, %Request{request | body: :binary.part(body, 0, content_length)}}

        true ->
          :more
      end
    else
      _error -> {:error, :invalid_request}
    end
  end

  defp do_parse_request(_raw_request), do: :more

  defp get_content_length(request) do
    case Request.get_header(request, "Content-Length") do
      {:ok, value} ->
        case Integer.parse(value) do
          {content_length, _rest} when content_length >= 0 -> {:ok, content_length}
          _error -> {:error, :invalid_content_length}
        end

      _error ->
        {:ok, 0}
    end
  end
end
