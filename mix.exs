defmodule RaftedValue.Mixfile do
  use Mix.Project

  @github_url "https://github.com/skirino/rafted_value"

  def project do
    [
      app:             :rafted_value,
      version:         "0.0.1",
      elixir:          "~> 1.2",
      build_embedded:  Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      compilers:       compilers,
      deps:            deps,
      description:     "Replicated and synchronized values wrapped by processes",
      package:         package,
      source_url:      @github_url,
      homepage_url:    @github_url,
      test_coverage:   [tool: Coverex.Task]
    ]
  end

  def application do
    [applications: [:croma]]
  end

  defp compilers do
    additional = if Mix.env == :prod, do: [], else: [:exref]
    Mix.compilers ++ additional
  end

  defp deps do
    [
      {:croma  , "~> 0.4"},
      {:exref  , "~> 0.1" , only: [:dev, :test]},
      {:coverex, "~> 1.4" , only: :test},
      {:dialyze, "~> 0.2" , only: :dev },
      {:earmark, "~> 0.2" , only: :dev },
      {:ex_doc , "~> 0.11", only: :dev },
    ]
  end

  defp package do
    [
      files:       ["lib", "mix.exs", "README.md", "LICENSE"],
      maintainers: ["Shunsuke Kirino"],
      licenses:    ["MIT"],
      links:       %{"GitHub repository" => @github_url, "Doc" => "http://hexdocs.pm/rafted_value/"},
    ]
  end
end
