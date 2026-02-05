from __future__ import annotations

from typing import Any, Callable

from blackbox.engines import is_dataframe_like, to_pandas


def blackbox_op(recorder, name: str, func: Callable[..., Any]):
    """
    Wrap a Dagster op/asset callable so it records a run and step automatically.
    """
    def _wrapped(*args, **kwargs):
        run = recorder.start_run(tags={"source": "dagster"})
        with run.step(name) as st:
            result = func(*args, **kwargs)
            if is_dataframe_like(result):
                st.capture_output(to_pandas(result))
            else:
                st.add_metadata(result_type=str(type(result)))
        run.finish()
        return result
    return _wrapped
