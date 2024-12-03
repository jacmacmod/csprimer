
export function Q(nodes: Array<Nodeq>): Nodeq {
  const ns = nodes[Symbol.iterator]();
  const root = ns.next().value;
  let parent = root;

  for (const n of ns) {
    parent.child = n;
    parent = n;
  }
  return root;
}

export async function* run(q: Nodeq) {
  while (true) {
    const x = await q.next();
    if (Array.isArray(x) && x.length === 0) continue;
    if (!x) break;

    yield await Promise.resolve(x);
  }
}