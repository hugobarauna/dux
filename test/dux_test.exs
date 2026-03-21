defmodule DuxTest do
  use ExUnit.Case

  test "struct creation" do
    dux = %Dux{source: {:csv, "test.csv", []}, names: ["a", "b"]}
    assert dux.names == ["a", "b"]
    assert dux.ops == []
  end
end
