import time

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


@pytest.mark.parametrize("dummy_loop_parameter", range(3))
def test_tracing_native_python(dummy_loop_parameter):
    with SpanRecorder() as r:
        tracer = ot.trace.get_tracer(__name__)

        with tracer.start_as_current_span("TopLevel") as t:
            t.record_exception(ValueError("foo!"))

    span = one(r.spans)
    assert read_key(span, ["name"]) == "TopLevel"
    assert read_key(span, ["parent_id"]) is None

    exception_event = one(get_span_exceptions(span))
    assert exception_event["name"] == "exception"
    assert exception_event["attributes"]["exception.type"] == "ValueError"
    assert exception_event["attributes"]["exception.message"] == "foo!"


@pytest.mark.parametrize("dummy_loop_parameter", range(3))
def test_tracing_nested_native_python(dummy_loop_parameter):
    # test that we can record and validate properties of spans emitted by native
    # Python code

    def get_test_spans():
        with SpanRecorder() as sr:
            tracer = ot.trace.get_tracer(__name__)

            with tracer.start_as_current_span("top"):
                time.sleep(0.2)
                with tracer.start_as_current_span("sub1") as span_sub1:
                    time.sleep(0.3)
                    span_sub1.set_attribute("sub1attribute", 12345)
                    with tracer.start_as_current_span("sub11"):
                        time.sleep(0.4)

                with tracer.start_as_current_span("sub2"):
                    time.sleep(0.1)

        return sr.spans

    spans: Spans = get_test_spans()

    assert len(spans) == 4

    top = one(spans.filter(["name"], "top"))
    sub1 = one(spans.filter(["name"], "sub1"))
    sub11 = one(spans.filter(["name"], "sub11"))
    sub2 = one(spans.filter(["name"], "sub2"))

    def check_parent_child(parent, child):
        assert spans.contains_path(parent, child, recursive=True)
        assert spans.contains_path(parent, child, recursive=False)
        assert is_parent_child(parent, child)

    check_parent_child(top, sub1)
    check_parent_child(top, sub2)
    check_parent_child(sub1, sub11)

    assert not is_parent_child(top, top)
    assert not is_parent_child(sub1, top)

    # Check that we can find "top -> sub11" relationship in "top -> sub1 -> sub11"
    assert spans.contains_path(top, sub11, recursive=True)
    assert not spans.contains_path(top, sub11, recursive=False)

    def check_duration(span, expected_duration_s: float) -> bool:
        return abs(get_duration_s(span) - expected_duration_s) < 0.05

    assert check_duration(top, 0.2 + 0.3 + 0.4 + 0.1)
    assert check_duration(sub1, 0.3 + 0.4)
    assert check_duration(sub11, 0.4)
    assert check_duration(sub2, 0.1)

    assert read_key(sub1, ["attributes", "sub1attribute"]) == 12345
