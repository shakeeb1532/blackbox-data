from __future__ import annotations
from contextvars import ContextVar
from functools import wraps
from typing import Callable, TypeVar, Any, cast

T = TypeVar("T")

_active_run: ContextVar[Any] = ContextVar("blackbox_active_run", default=None)

def set_active_run(run: Any) -> None:
    _active_run.set(run)

def get_active_run() -> Any:
    return _active_run.get()

def record_step(name: str):
    def deco(fn: Callable[..., T]) -> Callable[..., T]:
        @wraps(fn)
        def wrapped(*args, **kwargs):
            run = get_active_run()
            if run is None:
                return fn(*args, **kwargs)
            # naive: first arg that is DataFrame is input_df
            import pandas as pd
            input_df = None
            for a in args:
                if isinstance(a, pd.DataFrame):
                    input_df = a
                    break
            if input_df is None:
                for v in kwargs.values():
                    if isinstance(v, pd.DataFrame):
                        input_df = v
                        break
            with run.step(name, input_df=input_df) as st:
                out = fn(*args, **kwargs)
                if hasattr(st, "capture_output"):
                    try:
                        import pandas as pd
                        if isinstance(out, pd.DataFrame):
                            st.capture_output(out)
                    except Exception:
                        pass
                return out
        return cast(Callable[..., T], wrapped)
    return deco
