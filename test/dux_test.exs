defmodule DuxTest do
  use ExUnit.Case

  test "struct creation with defaults" do
    dux = %Dux{}
    assert dux.source == nil
    assert dux.ops == []
    assert dux.names == []
    assert dux.dtypes == %{}
    assert dux.groups == []
  end

  test "struct creation with source" do
    dux = %Dux{source: {:csv, "test.csv", []}, names: ["a", "b"]}
    assert dux.source == {:csv, "test.csv", []}
    assert dux.names == ["a", "b"]
    assert dux.ops == []
  end
end
