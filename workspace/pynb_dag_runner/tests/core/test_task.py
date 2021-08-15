import time

import ray, pytest

from pynb_dag_runner.core.dag_runner import (
    Task,
    TaskDependence,
    TaskDependencies,
)
from conftest import repeat_in_stress_tests


@repeat_in_stress_tests
def test_task_execute_states(repeat_count):
    @ray.remote(num_cpus=0)
    def f():
        time.sleep(0.1)
        return 1234

    task = Task(f.remote)

    # test properties of task before task execution has started
    assert not task.has_started()
    assert not task.has_completed()
    with pytest.raises(Exception):
        task.result()  # fails before task has started

    # test properties of task while task execution is in progress
    task.start()
    assert task.has_started()
    assert not task.has_completed()
    with pytest.raises(Exception):
        task.result()  # fails since task has not completed

    # test properties of task when task has completed
    time.sleep(0.2)
    assert task.has_started()
    assert task.has_completed()
    assert task.result() == ray.get(task.get_ref()) == 1234


@repeat_in_stress_tests
def test_task_exceptions_should_propagate(repeat_count):
    @ray.remote(num_cpus=0)
    def f():
        raise Exception("BOOM123!")

    task = Task(f.remote)

    try:
        task.start()
        time.sleep(0.2)
        ray.get(task.get_ref())

    except Exception as e:
        assert "BOOM123!" in str(e)
