import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main():
    os.chdir(ROOT_DIR)
    if ROOT_DIR not in sys.path:
        sys.path.insert(0, ROOT_DIR)
    from backend.cloudv2_fixture_simulator import run_fixture

    raise SystemExit(run_fixture())


if __name__ == "__main__":
    main()
