"""Sahara AI Agent Runtime — Python gRPC Worker."""

import os
import sys

# Ensure generated protobuf code is importable
_gen_path = os.path.join(os.path.dirname(__file__), "..", "gen")
if os.path.isdir(_gen_path) and _gen_path not in sys.path:
    sys.path.insert(0, os.path.abspath(_gen_path))

__version__ = "0.1.0"
