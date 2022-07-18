import time
from typing import Set, Tuple, Dict, List

#
import pytest

#
import opentelemetry as ot

#
from pynb_dag_runner.opentelemetry_helpers import (
    get_span_id,
    has_keys,
    read_key,
    is_parent_child,
    get_duration_s,
    iso8601_to_epoch_s,
    get_duration_range_us,
    get_span_exceptions,
    Spans,
    SpanRecorder,
    UDT,
)
from pynb_dag_runner.helpers import one


def test_nested_dict_helpers():
    a_dict = {
        "a": {
            "b": 123,
            "foo": "bar",
            "bar": "baz",
            "c": {"e": 1, "f": 2, "g": "hello", "h": None},
        }
    }
    assert has_keys(a_dict, ["a"])
    assert has_keys(a_dict, ["a", "b"])
    assert not has_keys(a_dict, ["key-does-not-exist"])
    assert not has_keys(a_dict, ["key-does-not-exist", "key-does-not-exist"])
    assert not has_keys(a_dict, ["a", "key-does-not-exist"])

    assert read_key(a_dict, ["a", "b"]) == 123


def test_iso8601_to_epoch_s():
    assert iso8601_to_epoch_s("2021-10-10T10:25:35.173367Z") == 1633861535173367 / 1e6


def test_tracing_get_span_id_and_duration():
    # Example span generated by Ray when calling a remote method on an Actor
    test_span = {
        "name": "ActorA.foo ray.remote_worker",
        "context": {
            "trace_id": "<hex-trace-id>",
            "span_id": "<hex-span-id>",
            "trace_state": "[]",
        },
        "kind": "SpanKind.CONSUMER",
        "parent_id": "<hex-parent-id>",
        "start_time": "2021-10-10T10:25:35.173367Z",
        "end_time": "2021-10-11T10:25:46.173381Z",
        "status": {"status_code": "UNSET"},
        "attributes": {
            "ray.remote": "actor",
            "ray.actor_class": "ActorA",
            "ray.actor_method": "foo",
            "ray.function": "ActorA.foo",
            "ray.pid": "1234",
            "ray.job_id": "01000000",
            "ray.node_id": "<hex-ray-node-id>",
            "ray.actor_id": "<hex-ray-actor-id>",
            "ray.worker_id": "<hex-ray-worker-id>",
        },
        "events": [],
        "links": [],
        "resource": {
            "telemetry.sdk.language": "python",
            "telemetry.sdk.name": "opentelemetry",
            "telemetry.sdk.version": "1.5.0",
            "service.name": "unknown_service",
        },
    }

    assert get_span_id(test_span) == "<hex-span-id>"
    assert get_duration_s(test_span) == 86411.0000140667
    assert get_duration_range_us(test_span) == range(1633861535173367, 1633947946173381)
    assert get_span_exceptions(test_span) == []


# --- test Directed Graph functions ---


def test__graph__can_create_empty_graph():
    assert len(UDT[int].from_edges(set([]))) == 0


@pytest.fixture
def test_udt_fixture() -> UDT[int]:
    """
    Test UDT (Union of Directed Tree:s):

              0
              |
              1
            /   \
           2     3
         -----
        / | | \
       4  5 6  7
          |    |
          8    9
        / | \
      10 11  12
    """

    edges: Set[Tuple[int, int]] = {
        # (parent_id, child_id)
        (0, 1),
        #
        (1, 2),
        (1, 3),
        #
        (2, 4),
        (2, 5),
        (2, 6),
        (2, 7),
        #
        (5, 8),
        (7, 9),
        #
        (8, 10),
        (8, 11),
        (8, 12),
    }

    udt: UDT[int] = UDT.from_edges(edges)
    assert udt.all_node_ids == set(range(13))
    assert udt.root_nodes() == {0}
    assert set(udt.edges()) == edges
    return udt


def test__graph__from_edges(test_udt_fixture: UDT[int]):
    def get_child_ids(node_id: int) -> Set[int]:
        return set(
            child_id
            for (parent_id, child_id) in test_udt_fixture.edges()
            if parent_id == node_id
        )

    expected_child_ids: Dict[int, List[int]] = {
        0: [1],
        1: [2, 3],
        2: [4, 5, 6, 7],
        3: [],
        4: [],
        5: [8],
        6: [],
        7: [9],
        8: [10, 11, 12],
        9: [],
        10: [],
        11: [],
        12: [],
    }

    for parent_id, child_ids in expected_child_ids.items():
        assert get_child_ids(node_id=parent_id) == set(child_ids)


def test__graph__built_in_methods(test_udt_fixture: UDT[int]):
    assert len(test_udt_fixture) == 13

    # iteration over nodes
    ts_list = list(iter(test_udt_fixture))
    assert set(ts_list) == set(range(13))

    # test element membership in graph
    for node_id in range(13):
        assert node_id in test_udt_fixture

    assert -1 not in test_udt_fixture
    assert "foo" not in test_udt_fixture  # type: ignore


def test__graph__bound_by(test_udt_fixture: UDT[int]):
    def validate_root_bound_by_0(udt: UDT[int]):
        # Bounding test-tree by node_id=0 (inclusive) should return the same tree
        assert udt.root_nodes() == {0}
        assert udt == test_udt_fixture
        assert len(udt) == len(test_udt_fixture)

    validate_root_bound_by_0(test_udt_fixture.bound_by(0, inclusive=True))

    # Bounding by other node_id:s should not return same tree
    assert test_udt_fixture != test_udt_fixture.bound_by(0, inclusive=False)

    def validate_root_bound_by_2(udt: UDT[int]):
        # Bounding test-tree by node_id=2 (non-inclusive) should give union of four
        # disconnected directed trees:
        #
        #     4     5     6     7
        #           |           |
        #           8           9
        #         / | \
        #       10 11  12
        #
        assert udt.root_nodes() == {4, 5, 6, 7}
        assert udt.all_node_ids == {4, 5, 6, 7, 8, 9, 10, 11, 12}
        assert udt.edges() == set([(5, 8), (8, 10), (8, 11), (8, 12), (7, 9)])
        assert len(udt) == 9

    validate_root_bound_by_2(test_udt_fixture.bound_by(2, inclusive=False))

    def validate_root_bound_by_5(udt: UDT[int]):
        #
        # Bounding test-tree by node_id=5 (inclusive) should give tree
        #
        #        5
        #        |
        #        8
        #      / | \
        #    10 11  12
        #
        assert udt.root_nodes() == {5}
        assert udt.all_node_ids == {
            5,
            8,
            10,
            11,
            12,
        }
        assert udt == UDT[int].from_edges(
            {
                (5, 8),
                #
                (8, 10),
                (8, 11),
                (8, 12),
            }
        )

    validate_root_bound_by_5(test_udt_fixture.bound_by(5, inclusive=True))

    def validate_root_bound_by_11(udt: UDT[int]):
        # Bounding test-tree by node_id=11 (inclusive) should give tree with only one
        # element.
        #
        # Note: this graph can not be generated from list of edges since there is only
        # one node.
        assert udt.all_node_ids == {11}
        assert udt.root_nodes() == {11}
        assert udt.edges() == set([])
        assert len(udt) == 1

    validate_root_bound_by_11(test_udt_fixture.bound_by(11, inclusive=True))


def test__graph__contains_path(test_udt_fixture: UDT[int]):
    assert test_udt_fixture.root_nodes() == set([0])

    assert test_udt_fixture.contains_path(0, 1, 2, 4)
    assert test_udt_fixture.contains_path(0, 4)
    assert test_udt_fixture.contains_path(5, 8, 12)
    assert test_udt_fixture.contains_path(5, 12)

    assert not test_udt_fixture.contains_path(3, 7)
    assert not test_udt_fixture.contains_path(2, 3)
    assert not test_udt_fixture.contains_path(8, 9)
    assert not test_udt_fixture.contains_path(3, 0)
