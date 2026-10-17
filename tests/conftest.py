"""Test bootstrap: Config reads env at import time — provide dummies."""
import os

os.environ.setdefault("MATRIX_USER_ID", "@bot:test")
os.environ.setdefault("MATRIX_ACCESS_TOKEN", "test-token")
os.environ.setdefault("BERGET_API_KEY", "test-key")
os.environ.setdefault("MATRIX_DEVICE_ID", "TESTDEV")
os.environ.setdefault("NIO_STORE_PATH", "/tmp/mt-test-store")
