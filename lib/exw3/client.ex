defmodule ExW3.Client do
  @type argument :: term
  @type request_error :: Ethereumex.Client.Behaviour.error()
  @type error :: {:error, :invalid_client_type} | request_error

  @spec call_client(atom) :: {:ok, term} | error
  @spec call_client(atom, [argument]) :: {:ok, term} | error
  def call_client(method_name, arguments \\ []) do
    url_opt = extract_url_opt(arguments)

    case client_type(url_opt) do
      :http -> apply(Ethereumex.HttpClient, method_name, arguments)
      :ipc -> apply(Ethereumex.IpcClient, method_name, arguments)
      :ws -> apply(Ethereumex.WsClient, method_name, arguments)
      _ -> {:error, :invalid_client_type}
    end
  end

  defp extract_url_opt(arguments) do
    arguments
    |> List.last()
    |> case do
      last when is_list(last) -> Keyword.get(last, :url)
      _ -> nil
    end
  end

  defp client_type(nil), do: Application.get_env(:ethereumex, :client_type, :http)
  defp client_type("http://" <> _), do: :http
  defp client_type("https://" <> _), do: :http
  defp client_type("ws://" <> _), do: :ws
  defp client_type("wss://" <> _), do: :ws
  defp client_type(_), do: :invalid
end
