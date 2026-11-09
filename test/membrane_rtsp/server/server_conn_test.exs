defmodule Membrane.RTSP.ServerConnTest do
  @moduledoc false

  use ExUnit.Case

  alias Membrane.RTSP.Server
  alias Membrane.RTSP.Server.FakeHandler

  setup do
    server = start_supervised!({Server, [port: 0, handler: FakeHandler, session_timeout: 1]})
    {:ok, port} = Server.port_number(server)
    {:ok, socket} = :gen_tcp.connect(~c"localhost", port, [:binary, active: false])

    [socket: socket]
  end

  test "responds to a valid request", %{socket: socket} do
    :ok = :gen_tcp.send(socket, "OPTIONS rtsp://localhost/stream RTSP/1.0\r\nCSeq: 1\r\n\r\n")
    assert {:ok, "RTSP/1.0 200 OK\r\n" <> _rest} = :gen_tcp.recv(socket, 0, 1000)
  end

  test "closes the connection when the request grows too large", %{socket: socket} do
    :ok = :gen_tcp.send(socket, "OPTIONS rtsp://localhost/stream RTSP/1.0\r\n")

    # keep sending headers without ever terminating the header section
    assert :ok = send_until_closed(socket, "X-Header: " <> String.duplicate("a", 1014) <> "\r\n")
  end

  test "closes the connection when Content-Length is not a number", %{socket: socket} do
    :ok =
      :gen_tcp.send(
        socket,
        "ANNOUNCE rtsp://localhost/stream RTSP/1.0\r\nCSeq: 1\r\nContent-Length: abc\r\n\r\n"
      )

    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1000)
  end

  test "closes the connection when Content-Length is too large", %{socket: socket} do
    :ok =
      :gen_tcp.send(
        socket,
        "ANNOUNCE rtsp://localhost/stream RTSP/1.0\r\nCSeq: 1\r\nContent-Length: 100000000\r\n\r\n"
      )

    assert {:error, :closed} = :gen_tcp.recv(socket, 0, 1000)
  end

  defp send_until_closed(socket, data, attempts \\ 1000)
  defp send_until_closed(_socket, _data, 0), do: {:error, :still_open}

  defp send_until_closed(socket, data, attempts) do
    case :gen_tcp.send(socket, data) do
      :ok ->
        case :gen_tcp.recv(socket, 0, 10) do
          {:error, :timeout} -> send_until_closed(socket, data, attempts - 1)
          {:error, :closed} -> :ok
          {:ok, _data} -> send_until_closed(socket, data, attempts - 1)
        end

      {:error, _reason} ->
        :ok
    end
  end
end
