#!/usr/bin/env python3

import gitinfo
import json
from pathlib import Path

LOCALDIR = Path(__file__).parent

if __name__ == "__main__":
    git = gitinfo.get_git_info()
    if not isinstance(git, dict):
        git = {}
    fn = LOCALDIR / 'data' / 'buildinfo.json'
    with open(fn, 'w') as fh:
        json.dump(git, fh)

