"""Plain business callable; no test-runner or optional package dependencies."""
import os
import sys


def read_worker_environment(row):
    return {'unrelated': os.environ.get('DEMIFLOW_RAY_BOUNDARY_UNDECLARED'),
            'python': sys.executable, 'prefix': sys.prefix}
