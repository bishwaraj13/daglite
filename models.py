from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 1  # total attempts including first
    strategy: str = "fixed"  # fixed|exponential
    base_delay_seconds: float = 1.0
    jitter_seconds: float = 0.0

    def delay_for_attempt(self, attempt_number: int) -> float:
        """
        attempt_number: 1 for first attempt, 2 for first retry, etc.
        delay applies when scheduling the *next* attempt after a failure.
        """
        if self.max_attempts <= 1:
            return 0.0
        if self.strategy == "exponential":
            # after attempt 1 fails, schedule attempt 2 with base*2^(2-2)=base
            # attempt 3 => base*2^(3-2)=2*base
            delay = self.base_delay_seconds * (2 ** max(0, attempt_number - 2))
        else:
            delay = self.base_delay_seconds
        return max(0.0, delay)


TaskCallable = Callable[..., Any]


@dataclass
class Task:
    name: str
    fn: TaskCallable
    deps: Sequence[str] = ()
    retry: RetryPolicy = RetryPolicy()
    timeout_seconds: Optional[float] = None

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Task.name must be non-empty")


@dataclass
class Flow:
    name: str
    tasks: Dict[str, Task]
    edges: List[Tuple[str, str]]  # (upstream, downstream)
    version: str = "v1"
    description: str = ""

    def topological_layers(self) -> List[List[str]]:
        """
        Simple layering by longest distance (good enough for MVP UI graph layout).
        """
        from collections import defaultdict, deque

        indeg = defaultdict(int)
        children = defaultdict(list)
        for u, v in self.edges:
            children[u].append(v)
            indeg[v] += 1
            indeg.setdefault(u, 0)

        q = deque([n for n in self.tasks.keys() if indeg[n] == 0])
        order: List[str] = []
        while q:
            n = q.popleft()
            order.append(n)
            for c in children.get(n, []):
                indeg[c] -= 1
                if indeg[c] == 0:
                    q.append(c)

        if len(order) != len(self.tasks):
            raise ValueError("Flow contains a cycle or missing task definitions.")

        # compute level
        level: Dict[str, int] = {n: 0 for n in order}
        for u in order:
            for v in children.get(u, []):
                level[v] = max(level[v], level[u] + 1)

        layers: Dict[int, List[str]] = {}
        for n, lv in level.items():
            layers.setdefault(lv, []).append(n)
        return [layers[k] for k in sorted(layers.keys())]


def task(
    *,
    name: Optional[str] = None,
    deps: Sequence[str] = (),
    retry: Optional[RetryPolicy] = None,
    timeout_seconds: Optional[float] = None,
):
    """
    Decorator to define tasks ergonomically.
    """
    def deco(fn: TaskCallable) -> Task:
        tname = name or fn.__name__
        return Task(
            name=tname,
            fn=fn,
            deps=tuple(deps),
            retry=retry or RetryPolicy(),
            timeout_seconds=timeout_seconds,
        )
    return deco