defmodule RaftedValue.Mixfile do
  use Mix.Project

  @github_url "https://github.com/skirino/rafted_value"

  def project() do
    [
      app:             :rafted_value,
      version:         "0.10.3",
      elixir:          "~> 1.6",
      build_embedded:  Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps:            deps(),
      description:     "Replicated and synchronized values wrapped by processes",
      package:         package(),
      source_url:      @github_url,
      homepage_url:    @github_url,
      test_coverage:   [tool: Coverex.Task, coveralls: true],
    ]
  end

  def application() do
    [applications: [:croma]]
  end

  defp deps() do
    [
      {:croma   , "~> 0.9"},
      {:coverex , "~> 1.4"   , only: :test},
      {:dialyxir, "~> 0.5"   , only: :dev },
      {:ex_doc  , "~> 0.18.0", only: :dev },
    ]
  end

  defp package() do
    [
      files:       ["lib", "mix.exs", "README.md", "LICENSE"],
      maintainers: ["Shunsuke Kirino"],
      licenses:    ["MIT"],
      links:       %{"GitHub repository" => @github_url},
    ]
  end
end
