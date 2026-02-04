import os
import schemathesis


@schemathesis.deserializer("application/jsonl")
def _jsonl_deserializer(response, *args, **kwargs):
    # Return raw text to avoid validation errors on JSONL streams.
    return response.text


@schemathesis.deserializer("application/zip")
def _zip_deserializer(response, *args, **kwargs):
    # Return raw bytes for zip responses.
    return response.content


@schemathesis.hook("before_call")
def _inject_known_params(context, case):
    project = os.environ.get("SCHEMA_PROJECT", "acme-data")
    dataset = os.environ.get("SCHEMA_DATASET", "demo")
    run_id = os.environ.get("SCHEMA_RUN_ID", "")

    if case.path in ("/runs",):
        case.query = {"project": project, "dataset": dataset}
        return

    if case.path in ("/verify", "/report", "/report_verbose", "/evidence"):
        if run_id:
            case.query = {"project": project, "dataset": dataset, "run_id": run_id}
