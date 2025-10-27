defmodule LuneTest do
  use ExUnit.Case, async: true
  doctest Lune

  test "17893729974" do
    Lune.verify?("17893729974")
  end

  test "1789372997" do
    !Lune.verify?("1789372997")
  end
end
