defmodule Dux.SQL do
  @moduledoc false

  # AST expression nodes -> SQL fragments.
  # The inner compiler: traverses the expression AST from Dux.Query
  # and generates DuckDB SQL with parameter bindings for ^pin interpolations.
end
