from __future__ import annotations

from typing import Any, Callable

from blackbox.engines import is_dataframe_like, to_pandas


def blackbox_task(recorder, name: str, func: Callable[..., Any], *, run=None):
    """
    Wrap a task callable so it records a run and step automatically.
    """
    def _wrapped(*args, **kwargs):
        active_run = run or recorder.start_run(tags={"source": "airflow"})
        with active_run.step(name) as st:
            result = func(*args, **kwargs)
            if is_dataframe_like(result):
                st.capture_output(to_pandas(result))
            else:
                st.add_metadata(result_type=str(type(result)))
        if run is None:
            active_run.finish()
        return result
    return _wrapped


def blackbox_task_in_run(run, name: str, func: Callable[..., Any]):
    return blackbox_task(run.recorder, name, func, run=run)
