# blackbox (core engine)

Core library for recording, diffing, snapshotting, and sealing pipeline steps.

Key areas:
- `recorder.py`: run/step lifecycle and artifact writing
- `hashing.py`: row hashing + fingerprints + diffing
- `seal.py`: hash chain integrity verification
- `store.py`: storage backends (local + S3)
- `engines.py`: dataframe conversions (pandas + adapters)

Use this package when embedding Blackbox into Python pipelines.
