#!/usr/bin/env python3
"""Backward-compatible entry point. Delegates to extractor.cli.main()."""
import sys
from extractor.cli import main

if __name__ == "__main__":
    sys.exit(main())
