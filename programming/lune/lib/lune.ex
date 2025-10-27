defmodule Lune do
  @moduledoc """
  Lune module for verifying account numbers
  """

  @doc """
  Verfiy account number

  ## Examples
    iex> Lune.verify?("17893729974")
    true
    iex> Lune.verify?("1789372997")
    false

  """
  @spec verify?(binary()) :: boolean()
  def verify?(account_num) do
    case String.last(account_num) do
      nil -> false
      s -> String.to_integer(s) == compute_check_digit(account_num)
    end
  end

  @spec compute_check_digit(binary()) :: integer()
  defp compute_check_digit(account_num) do
    account_num
    |> String.slice(0..-2//1)
    |> String.split("", trim: true)
    |> Enum.map(&String.to_integer/1)
    |> Enum.reverse()
    |> Stream.chunk_every(2)
    |> Enum.flat_map(fn
      [d1, d2] -> [luhn_double(d1), d2]
      [d1] -> [luhn_double(d1)]
    end)
    |> Enum.sum()
    |> then(fn sum -> Integer.mod(10 - Integer.mod(sum, 10), 10) end)
  end

  @spec compute_check_digit(integer()) :: integer()
  defp luhn_double(digit) do
    doubled = digit * 2
    if doubled > 9, do: doubled - 9, else: doubled
  end
end
