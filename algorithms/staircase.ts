// solve staircase problem
//
// given n steps how many possible combinations are there
//
//  for (let i = 1; i < n; i++) {
//    if (n % i === 0 && i > 1 ) total++
//    total += branch(i, n)
//  }
//
//  if (n <= 1 ) {
//    return n;
//  }

  //let total = 1;

 // return total;
//
//
//

function solve(n: number): number {
  if (n <= 1) return 1
  if (n == 2) return 2

  return solve(n-3) + solve(n - 2) + solve(n -1)
}

function branch(n: number, target: number): number {
  //  console.log("target", target, "cur", n)
  let result = target - n;
  
  if (result === 1) return 1;

  return branch(result - 1, result) + branch(result - 1, result);
}

function main() {
  console.assert(solve(1) === 1, 1);
  console.assert(solve(2) === 2, 2);
  console.assert(solve(3) === 4, 3);
  console.assert(solve(4) === 7, 4);
  console.assert(solve(5) === 13, 5);
}

main();
