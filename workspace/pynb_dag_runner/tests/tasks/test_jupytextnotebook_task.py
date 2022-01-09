import datetime
from pathlib import Path

#
import pytest

#
from pynb_dag_runner.tasks.tasks import make_jupytext_task, make_jupytext_task_ot
from pynb_dag_runner.core.dag_runner import (
    run_tasks,
    TaskDependencies,
    start_and_await_tasks,
)
from pynb_dag_runner.helpers import one
from pynb_dag_runner.notebooks_helpers import JupytextNotebook
from pynb_dag_runner.opentelemetry_helpers import Spans, Span, SpanRecorder, read_key
from pynb_dag_runner.tasks.extract import get_tasks, LoggedJupytextTask, LoggedTaskRun

# TODO: all the below tests should run multiple times in stress tests
# See, https://github.com/pynb-dag-runner/pynb-dag-runner/pull/5


def isotimestamp_normalized():
    """
    Return ISO timestamp modified (by replacing : with _) so that it can be used
    as part of a directory or file name.

    Eg "YYYY-MM-DDTHH-MM-SS.ffffff+00-00"

    This is useful to generate output directories that are guaranteed to not exist.
    """
    return datetime.datetime.now(datetime.timezone.utc).isoformat().replace(":", "-")


def make_test_nb_task(nb_name: str, max_nr_retries: int, task_parameters={}):
    nb_path: Path = (Path(__file__).parent) / "jupytext_test_notebooks"
    return make_jupytext_task_ot(
        notebook=JupytextNotebook(nb_path / nb_name),
        tmp_dir=nb_path,
        timeout_s=5,
        max_nr_retries=max_nr_retries,
        task_parameters=task_parameters,
    )


def test_jupytext_run_ok_notebook():
    def get_test_spans():
        with SpanRecorder() as rec:
            jupytext_task = make_test_nb_task(
                nb_name="notebook_ok.py",
                max_nr_retries=1,
                task_parameters={"variable_a": "task-value"},
            )
            _ = start_and_await_tasks([jupytext_task], [jupytext_task], arg={})

        return rec.spans

    def validate_spans(spans: Spans):
        jupytext_span = one(
            spans.filter(["name"], "execute-task")
            #
            .filter(["attributes", "tags.task_type"], "jupytext")
        )
        assert jupytext_span["status"] == {"status_code": "OK"}

        assert (
            read_key(jupytext_span, ["attributes", "tags.notebook"])
            == str((Path(__file__).parent)) + "/jupytext_test_notebooks/notebook_ok.py"
        )

        artefact_span = one(spans.filter(["name"], "artefact"))
        for content in [str(1 + 12 + 123), "variable_a=task-value"]:
            assert content in artefact_span["attributes"]["content"]

        spans.contains_path(jupytext_span, artefact_span)

    def validate_recover_tasks_from_spans(spans: Spans):
        extracted_task = one(get_tasks(spans))

        assert isinstance(extracted_task, LoggedJupytextTask)
        assert extracted_task.is_success == True
        assert extracted_task.error is None

        run = one(extracted_task.runs)
        assert isinstance(run, LoggedTaskRun)
        assert run.is_success == True
        assert run.error is None
        assert run.run_parameters == {
            "retry.max_retries": 5,
            "retry.nr": 0,
            "task_id": "notebook_ok.py-task-render-notebook",
            "task_parameter.variable_a": "task-value",
        }

        # all runs should have logged the evaluated ipynb notebook
        assert one(run.artefacts).name == "notebook.ipynb"
        for content in [str(1 + 12 + 123), "variable_a=task-value"]:
            assert content in one(run.artefacts).content

    spans = get_test_spans()
    validate_spans(spans)
    # validate_recover_tasks_from_spans(spans)


@pytest.mark.parametrize("N_retries", [2, 10])
def disable_test_jupytext_exception_throwing_notebook(N_retries):
    def get_test_spans():
        with SpanRecorder() as rec:
            jupytext_task = make_test_nb_task(
                nb_name="notebook_exception.py",
                n_max_retries=N_retries,
                task_parameters={},
            )
            run_tasks([jupytext_task], TaskDependencies())

        return rec.spans

    # notebook will fail on first three runs. Depending on number of retries
    # determine which run:s are success/failed.
    def ok_indices():
        if N_retries == 2:
            return []
        else:
            return [3]

    def failed_indices():
        if N_retries == 2:
            return [0, 1]
        else:
            return [0, 1, 2]

    def validate_spans(spans: Spans):
        jupytext_span = one(
            spans.filter(["name"], "invoke-task").filter(
                ["attributes", "task_type"], "jupytext"
            )
        )
        if len(ok_indices()) > 0:
            assert jupytext_span["status"] == {"status_code": "OK"}
        else:
            assert jupytext_span["status"] == {
                "status_code": "ERROR",
                "description": "Jupytext notebook task failed",
            }

        run_spans = spans.filter(["name"], "task-run").sort_by_start_time()
        assert len(run_spans) == len(ok_indices()) + len(failed_indices())

        for idx in ok_indices():
            assert run_spans[idx]["status"] == {"status_code": "OK"}

        for idx in failed_indices():
            failed_run_span = run_spans[idx]

            exception = one(spans.exceptions_in(failed_run_span))["attributes"]
            assert exception["exception.type"] == "PapermillExecutionError"
            assert "Thrown from notebook!" in exception["exception.message"]

            assert run_spans[idx]["status"] == {
                "status_code": "ERROR",
                "description": "Run failed",
            }

        for idx in failed_indices() + ok_indices():
            # for both successful and failed runs, a partially evaluated notebook
            # should have been logged as an artefact.
            artefact_span = one(
                spans.restrict_by_top(run_spans[idx]).filter(["name"], "artefact")
            )
            assert artefact_span["attributes"]["name"] == "notebook.ipynb"
            assert str(1 + 12 + 123) in artefact_span["attributes"]["content"]

    def validate_recover_tasks_from_spans(spans: Spans):
        extracted_task = one(get_tasks(spans))

        assert isinstance(extracted_task, LoggedJupytextTask)

        if len(ok_indices()) > 0:
            assert extracted_task.is_success == True
            assert extracted_task.error is None
        else:
            assert extracted_task.is_success == False
            assert extracted_task.error == "Jupytext notebook task failed"

        runs = extracted_task.runs
        assert len(runs) == len(failed_indices() + ok_indices())

        for run in runs:
            assert isinstance(run, LoggedTaskRun)
            assert run.run_parameters.keys() == {
                "retry.max_retries",
                "retry.nr",
                "task_id",
            }
            # all runs should have logged partially evaluated ipynb notebook
            artefact = one(run.artefacts)
            assert artefact.name == "notebook.ipynb"
            assert str(1 + 12 + 123) in artefact.content

        for idx in ok_indices():
            assert runs[idx].is_success == True
            assert runs[idx].error is None

        for idx in failed_indices():
            assert runs[idx].is_success == False
            assert runs[idx].error == "Run failed"

    spans = get_test_spans()
    validate_spans(spans)
    validate_recover_tasks_from_spans(spans)


def disable_test_jupytext_stuck_notebook():
    """
    Currently, timeout canceling is done on Ray level, but error handling and
    recovery is done only within the Python process (using try .. catch).
    Therefore, timeout canceled tasks can not currently do proper error handling.
    """

    def get_test_spans():
        with SpanRecorder() as rec:
            jupytext_task = make_test_nb_task(
                nb_name="notebook_stuck.py",
                n_max_retries=1,
                task_parameters={},
            )
            run_tasks([jupytext_task], TaskDependencies())

        return rec.spans

    def validate_spans(spans: Spans):
        py_span = one(
            spans.filter(["name"], "invoke-task").filter(
                ["attributes", "task_type"], "python"
            )
        )
        assert py_span["status"] == {
            "description": "Task failed",
            "status_code": "ERROR",
        }

        jupytext_span = one(
            spans.filter(["name"], "invoke-task").filter(
                ["attributes", "task_type"], "jupytext"
            )
        )
        assert jupytext_span["status"] == {
            "description": "Jupytext notebook task failed",
            "status_code": "ERROR",
        }

        timeout_guard_span = one(spans.filter(["name"], "timeout-guard"))
        assert timeout_guard_span["status"] == {
            "status_code": "ERROR",
            "description": "Timeout",
        }

        spans.contains_path(jupytext_span, timeout_guard_span, py_span)

        assert len(spans.exceptions_in(jupytext_span)) == 0

        # notebook evaluation never finishes, and is cancled by Ray. Therefore no
        # artefact ipynb content is logged
        assert len(spans.filter(["name"], "artefact")) == 0

    def validate_recover_tasks_from_spans(spans: Spans):
        extracted_task = one(get_tasks(spans))

        assert isinstance(extracted_task, LoggedJupytextTask)

        assert extracted_task.is_success == False
        assert extracted_task.error == "Jupytext notebook task failed"

        run = one(extracted_task.runs)
        assert isinstance(run, LoggedTaskRun)
        assert run.run_parameters.keys() == {
            "retry.max_retries",
            "retry.nr",
            "task_id",
        }
        assert len(run.artefacts) == 0

        assert extracted_task.duration_s > run.duration_s > 5.0

    spans = get_test_spans()
    validate_spans(spans)
    validate_recover_tasks_from_spans(spans)
