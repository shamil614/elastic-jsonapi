defmodule ElasticJSONAPI.MixProject do
  use Mix.Project

  def project do
    [
      app: :elastic_jsonapi,
      version: "0.1.0",
      elixir: "~> 1.8",
      start_permanent: Mix.env() == :prod,
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
      # Javascript-style destructuring
      {:destructure, "~> 0.2.3"},
      {:elastix, "~> 0.5"},
      # Wraps erlang :queue and adds some API improvments
      {:qex, "~> 0.5"},
      # Simple Elixir macros for linear retry, exponential backoff and wait with composable delays.
      # Used in elastix. Best to be explicit and declare dependency. Used in testing.
      {:retry, "~> 0.8"},
      # time manipulations for Serializers
      {:timex, "~> 3.3"}
    ]
  end
end
