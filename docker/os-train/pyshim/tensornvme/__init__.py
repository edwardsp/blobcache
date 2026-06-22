# Stub of the `tensornvme` package (async NVMe checkpoint IO) for the blobcache
# Open-Sora benchmark. opensora/utils/ckpt.py eagerly runs
# `from tensornvme.async_file_io import AsyncFileWriter` at import time, but
# tensornvme is not installed in the training image. With epochs=1 and
# ckpt_every=100000 the writer is never instantiated, so this stub only needs to
# satisfy the import; it raises if a checkpoint save is actually attempted.
