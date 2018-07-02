defmodule SigstrElixirKafka.MixProject do
  use Mix.Project

  def project do
    [
      app: :sigstr_elixir_kafka,
      version: "0.1.0",
      elixir: "~> 1.6",
      start_permanent: false,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"},
      {:kafka_ex, "~> 0.8.3"},
      {:murmur, "~>1.0"}
    ]
  end
end
