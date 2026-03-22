if Code.ensure_loaded?(FLAME.Trackable) do
  defimpl FLAME.Trackable, for: Dux do
    def track(%Dux{source: {:table, ref}} = dux, acc, node) when is_reference(ref) do
      if node(ref) == node do
        {dux, [ref | acc]}
      else
        {dux, acc}
      end
    end

    def track(dux, acc, _node), do: {dux, acc}
  end
end
