defmodule Erlazure.MixProject do
  use Mix.Project

  def project() do
    [
      app: :erlazure,
      version: read_version(),
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :xmerl, :inets, :crypto, :ssl],
      mod: {:erlazure_app, []}
    ]
  end

  defp deps do
    [
      {:jsx, "~> 3.1"},
      {:redbug, "~> 2.0", only: [:test, :dev]},
    ]
  end

  defp read_version() do
    try do
      {out, 0} = System.cmd("git", ["describe", "--tags"])
      out
      |> String.trim()
      |> Version.parse!()
      |> Map.put(:pre, [])
      |> to_string()
    rescue
      _ -> "0.1.0"
    end
  end
end
