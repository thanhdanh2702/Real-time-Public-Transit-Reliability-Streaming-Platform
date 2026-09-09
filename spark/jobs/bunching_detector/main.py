"""Compatibility entry point for the single TripUpdate/bunching job.

Run this OR route_reliability/main.py, never both for the same flow/checkpoint.
"""

from spark.jobs.route_reliability.main import main

if __name__ == "__main__":
    main()
