from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import os
import json


class StoreError(RuntimeError):
    pass


class Store:
    def put_bytes(self, key: str, data: bytes, *, content_type: str | None = None) -> None:
        raise NotImplementedError

    def get_bytes(self, key: str) -> bytes:
        raise NotImplementedError

    def put_json(self, key: str, obj: dict[str, Any]) -> None:
        self.put_bytes(
            key,
            json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8"),
            content_type="application/json",
        )

    def get_json(self, key: str) -> dict[str, Any]:
        return json.loads(self.get_bytes(key).decode("utf-8"))

    def put_parquet_df(self, key: str, df: "Any", *, compression: str | None) -> float:
        """
        Store a DataFrame as Parquet. Returns size in MB.

        Default implementation buffers to memory. LocalStore overrides for
        direct-to-disk writes to reduce memory overhead.
        """
        import io
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, compression=compression)
        data = buf.getvalue()
        self.put_bytes(key, data, content_type="application/octet-stream")
        return float(len(data) / (1024 * 1024))

    def list(self, prefix: str) -> list[str]:
        raise NotImplementedError

    # --- MVP polish: common primitives used by CLI and verification tooling ---

    def exists(self, key: str) -> bool:
        try:
            self.get_bytes(key)
            return True
        except FileNotFoundError:
            return False
        except Exception as e:
            # S3/boto3 missing key surfaces as ClientError with response code.
            code = getattr(e, "response", {}).get("Error", {}).get("Code")
            if code in {"NoSuchKey", "NotFound", "404", "NoSuchBucket"}:
                return False
            raise

    def list_dirs(self, prefix: str) -> list[str]:
        """
        List immediate child "directory names" under prefix.

        LocalStore: subdirectories under the filesystem folder.
        S3Store: inferred from keys (immediate next path segment).
        """
        raise NotImplementedError

    @staticmethod
    def local(root: str) -> "LocalStore":
        return LocalStore(root=root)

    @staticmethod
    def s3(bucket: str, prefix: str = "", **kwargs: Any) -> "S3Store":
        return S3Store(bucket=bucket, prefix=prefix, **kwargs)


@dataclass
class LocalStore(Store):
    root: str

    def _path(self, key: str) -> str:
        key = key.lstrip("/")
        return os.path.join(self.root, key)

    def put_bytes(self, key: str, data: bytes, *, content_type: str | None = None) -> None:
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(data)

    def put_parquet_df(self, key: str, df: "Any", *, compression: str | None) -> float:
        path = self._path(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.to_parquet(path, index=False, compression=compression)
        size_mb = os.path.getsize(path) / (1024 * 1024)
        return float(size_mb)
    def get_bytes(self, key: str) -> bytes:
        path = self._path(key)
        with open(path, "rb") as f:
            return f.read()

    def list(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if not os.path.exists(base):
            return []
        out: list[str] = []
        if os.path.isfile(base):
            return [prefix]
        for root, _, files in os.walk(base):
            for fn in files:
                full = os.path.join(root, fn)
                rel = os.path.relpath(full, self.root)
                out.append(rel.replace("\\", "/"))
        out.sort()
        return out

    def list_dirs(self, prefix: str) -> list[str]:
        base = self._path(prefix)
        if not os.path.exists(base):
            return []
        if os.path.isfile(base):
            return []
        out: list[str] = []
        for name in os.listdir(base):
            full = os.path.join(base, name)
            if os.path.isdir(full):
                out.append(name)
        out.sort()
        return out


@dataclass
class S3Store(Store):
    bucket: str
    prefix: str = ""
    region: str | None = None
    endpoint_url: str | None = None
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None

    def _client(self):
        try:
            import boto3
        except Exception as e:
            raise StoreError(
                "boto3 is required for S3Store. Install with: pip install 'blackbox-data[s3]'"
            ) from e
        session = boto3.session.Session(
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            aws_session_token=self.session_token,
            region_name=self.region,
        )
        return session.client("s3", endpoint_url=self.endpoint_url)

    def _key(self, key: str) -> str:
        key = key.lstrip("/")
        if self.prefix:
            return f"{self.prefix.rstrip('/')}/{key}"
        return key

    def put_bytes(self, key: str, data: bytes, *, content_type: str | None = None) -> None:
        c = self._client()
        extra = {}
        if content_type:
            extra["ContentType"] = content_type
        c.put_object(Bucket=self.bucket, Key=self._key(key), Body=data, **extra)

    def get_bytes(self, key: str) -> bytes:
        c = self._client()
        obj = c.get_object(Bucket=self.bucket, Key=self._key(key))
        return obj["Body"].read()

    def list(self, prefix: str) -> list[str]:
        c = self._client()
        pfx = self._key(prefix)
        keys: list[str] = []
        token = None
        while True:
            kwargs = dict(Bucket=self.bucket, Prefix=pfx)
            if token:
                kwargs["ContinuationToken"] = token
            resp = c.list_objects_v2(**kwargs)
            for it in resp.get("Contents", []):
                k = it["Key"]
                # strip store prefix
                if self.prefix and k.startswith(self.prefix.rstrip("/") + "/"):
                    k = k[len(self.prefix.rstrip("/")) + 1 :]
                keys.append(k)
            if not resp.get("IsTruncated"):
                break
            token = resp.get("NextContinuationToken")
        keys.sort()
        return keys

    def list_dirs(self, prefix: str) -> list[str]:
        # Infer "directories" from keys under prefix.
        keys = self.list(prefix)
        out = set()
        p = prefix.rstrip("/") + "/"
        for k in keys:
            k = str(k).lstrip("/")
            if not k.startswith(p):
                continue
            rest = k[len(p) :]
            seg = rest.split("/", 1)[0]
            if seg:
                out.add(seg)
        return sorted(out)
