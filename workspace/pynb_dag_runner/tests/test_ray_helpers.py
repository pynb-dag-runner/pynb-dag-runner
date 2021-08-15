import time, random
from pathlib import Path
from uuid import uuid4
from typing import Callable

#
import pytest, ray

#
from pynb_dag_runner.helpers import flatten, ranges_intersect
from pynb_dag_runner.ray_helpers import try_eval_f_async_wrapper, retry_wrapper, Future
from conftest import repeat_in_stress_tests


@ray.remote(num_cpus=0)
class StateActor:
    def __init__(self):
        self._state = []

    def add(self, value):
        self._state += [value]

    def get(self):
        return self._state


@repeat_in_stress_tests
def test_future_map(repeat_count):
    @ray.remote(num_cpus=0)
    def f() -> int:
        return 123

    # example of a future having Future[int] type, but type checker does not notice
    # any problem with the below code.
    future: Future[bool] = f.remote()

    assert ray.get(Future.map(future, lambda x: x + 1)) == 124


### tests for try_eval_f_async_wrapper wrapper


@repeat_in_stress_tests
def test_timeout_w_success(repeat_count):
    def f(x: int) -> int:
        return x + 1

    f_timeout: Callable[[Future[int]], Future[int]] = try_eval_f_async_wrapper(
        f,
        timeout_s=10,
        success_handler=lambda x: 2 * x,
        error_handler=lambda _: None,
    )

    for x in range(3):
        assert ray.get(f_timeout(ray.put(x))) == 2 * (x + 1)


@repeat_in_stress_tests
def test_timeout_w_exception(repeat_count):
    def f(dummy):
        raise Exception(f"BOOM{dummy}")

    f_timeout = try_eval_f_async_wrapper(
        f,
        timeout_s=10,
        success_handler=lambda _: None,
        error_handler=lambda x: x,
    )

    for x in range(3):
        try:
            _ = ray.get(f_timeout(ray.put(x)))
        except Exception as e:
            assert f"BOOM{x}" in str(e)


@repeat_in_stress_tests
@pytest.mark.parametrize("task_timeout_s", [0.001, 10.0])
@pytest.mark.parametrize("state_type", ["Actor", "File"])
def test_timeout_w_timeout(tmp_path: Path, repeat_count, state_type, task_timeout_s):
    class State:
        pass

    class FileState(State):
        def __init__(self):
            self.temp_file = tmp_path / f"{uuid4()}.txt"

        def flip(self):
            return self.temp_file.touch()

        def did_flip(self) -> bool:
            return self.temp_file.is_file()

    class ActorState(State):
        def __init__(self):
            self.state_actor = StateActor.remote()

        def flip(self):
            return self.state_actor.add.remote(1)

        def did_flip(self) -> bool:
            return 1 in ray.get(self.state_actor.get.remote())

    assert state_type in ["Actor", "File"]
    state: State = ActorState() if state_type == "Actor" else FileState()

    task_duration_s = 0.2

    def f(dummy):
        time.sleep(task_duration_s)

        # We should not get here if the task is canceled by timeout
        state.flip()

    f_timeout = try_eval_f_async_wrapper(
        f,
        timeout_s=task_timeout_s,
        success_handler=lambda _: "RUN OK",
        error_handler=lambda e: "FAIL:" + str(e),
    )

    result = ray.get(f_timeout(ray.put("dummy")))

    # Wait for task to finish
    time.sleep(4.0)

    if task_timeout_s < task_duration_s:
        # f should have been canceled, and state should not have flipped
        assert not state.did_flip()  # type: ignore
        assert result.startswith("FAIL:") and "timeout" in result.lower()
        assert "timeout" in result.lower()
    else:
        assert state.did_flip()  # type: ignore
        assert result == "RUN OK"


### tests for retry_wrapper


@repeat_in_stress_tests
def test_retry_all_fail(repeat_count):
    results = ray.get(
        retry_wrapper(
            f_task_remote=ray.remote(num_cpus=0)(lambda _: "foo").remote,
            max_retries=10,
            is_success=lambda _: False,
        )
    )
    assert results == ["foo"] * 10


@repeat_in_stress_tests
def test_retry_all_success(repeat_count):
    results = ray.get(
        retry_wrapper(
            f_task_remote=ray.remote(num_cpus=0)(lambda _: "foo").remote,
            max_retries=10,
            is_success=lambda _: True,
        )
    )
    assert results == ["foo"]


@repeat_in_stress_tests
def test_retry_deterministic_success(repeat_count):
    results = ray.get(
        retry_wrapper(
            f_task_remote=ray.remote(num_cpus=0)(lambda retry_nr: retry_nr).remote,
            max_retries=10,
            is_success=lambda x: x >= 4,
        )
    )
    assert results == [0, 1, 2, 3, 4]


@repeat_in_stress_tests
def test_retry_random(repeat_count):
    for _ in range(10):
        results = ray.get(
            retry_wrapper(
                f_task_remote=ray.remote(num_cpus=0)(
                    lambda _: random.randint(1, 10)
                ).remote,
                max_retries=5,
                is_success=lambda x: x >= 5,
            )
        )

        assert all(isinstance(r, int) for r in results)
        assert 0 < len(results) <= 5

        if len(results) < 5:
            assert results[-1] >= 5  # last is success
            assert all(r < 5 for r in results[:-1])  # other is failures


@repeat_in_stress_tests
def test_multiple_retrys_should_run_in_parallel(repeat_count):
    def make_f(task_label: str):
        def f(retry_count):
            start_ts = time.time_ns()
            time.sleep(0.1)
            return {
                "task_label": task_label,
                "retry_count": retry_count,
                "start_ts": start_ts,
                "stop_ts": time.time_ns(),
            }

        return f

    f_a = retry_wrapper(
        ray.remote(num_cpus=0)(make_f("task-a")).remote,
        10,
        is_success=lambda result: result["retry_count"] >= 2,
    )
    f_b = retry_wrapper(
        ray.remote(num_cpus=0)(make_f("task-b")).remote,
        10,
        is_success=lambda result: result["retry_count"] >= 2,
    )

    results = flatten(ray.get([f_a, f_b]))
    assert len(results) == 2 * 3

    # On fast multi-core computers we can check that ray.get takes less than 2x the
    # sleep delay in f. However, on slower VMs with only two cores (and possibly other
    # processes?, like github's defaults runners) there may be so much overhead this
    # is not true. Instead we check that that there is some overlap between run times
    # for the two tasks. This seems like a more stable condition.

    def get_range(task_label: str):
        task_results = [r for r in results if r["task_label"] == task_label]

        return range(
            min(r["start_ts"] for r in task_results),
            max(r["stop_ts"] for r in task_results),
        )

    assert ranges_intersect(get_range("task-a"), get_range("task-b"))


### Test composition of both retry and timeout wrappers


@repeat_in_stress_tests
def test_retry_and_timeout_composition(repeat_count):
    def f(retry_count):
        if retry_count < 5:
            time.sleep(1e6)  # hang computation

    f_timeout = try_eval_f_async_wrapper(
        f,
        timeout_s=1,
        success_handler=lambda _: "SUCCESS",
        error_handler=lambda e: f"FAIL:{e}",
    )

    f_retry_timeout = retry_wrapper(
        lambda retry_count: f_timeout(ray.put(retry_count)),
        10,
        is_success=lambda result: result == "SUCCESS",
    )

    results = flatten(ray.get([f_retry_timeout]))

    assert len(results) == 6
    for result in results[:-1]:
        assert result.startswith("FAIL:Timeout error:")
    assert results[-1] == "SUCCESS"
