const f = (n: number): number => {
  let [prevprev, prev, cur] = [1, 1, 2]; // f(0), f(1), f(2)

  for (let i = 0; i < n; i++) {
    [prevprev, prev, cur] = [prev, cur, cur + prev + prevprev];
  }

  return prevprev;
};

function main() {
  const expectations = [1, 1, 2, 4, 7, 13];
  expectations.forEach((expectation, i) => {
    const result = f(i);
    console.assert(
      result === expectation,
      `f(${i}) != ${expectation} got ${result}`,
    );
  });
}

main();
