#!/usr/bin/env python3
import re
import sys
from typing import Dict, Optional, Set, Tuple

# strip ANSI color codes
ansi = re.compile(r"\x1b\[[0-9;]*m")


def strip_ansi(s: str) -> str:
    return ansi.sub("", s)


# match “[UpdateCache] index=20998 ... LEARNED new key ID 10733”
learn_re = re.compile(r"\[UpdateCache\]\s+index=(\d+).*?LEARNED new key ID\s+(\d+)")


def parse_log(path: str) -> Tuple[Set[str], Dict[int, Dict[str, Optional[str]]]]:
    all_nodes: Set[str] = set()
    by_index: Dict[int, Dict[str, Optional[str]]] = {}

    with open(path) as f:
        for raw in f:
            line = strip_ansi(raw)
            if "LEARNED new key" not in line or "|" not in line:
                continue

            # split off the node name
            left, right = line.split("|", 1)
            node = left.strip().split()[-1]

            m = learn_re.search(right)
            if not m:
                continue

            idx = int(m.group(1))
            key_id = m.group(2)

            all_nodes.add(node)

            # initialize this index if first time seen
            if idx not in by_index:
                # pre-fill any existing nodes as None
                by_index[idx] = {n: None for n in all_nodes}
            # make sure every prior index knows about new node
            for prev in by_index:
                if node not in by_index[prev]:
                    by_index[prev][node] = None

            by_index[idx][node] = key_id

    return all_nodes, by_index


def find_divergence(
    all_nodes: Set[str], by_index: Dict[int, Dict[str, Optional[str]]]
) -> None:
    for idx in sorted(by_index):
        node_map = by_index[idx]
        vals = [node_map.get(n) for n in sorted(all_nodes)]
        # if not all equal (None counts as a distinct value)
        if len(set(vals)) > 1:
            print(f"\n▶ Divergence at index {idx}:")
            for n in sorted(all_nodes):
                v = node_map.get(n)
                print(f"   {n}: key ID {v if v is not None else '<missing>'}")
            return
    print("✅ No divergence found.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} combined.log")
        sys.exit(1)

    nodes, by_idx = parse_log(sys.argv[1])
    find_divergence(nodes, by_idx)
