import os
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main():
    os.chdir(ROOT_DIR)
    if ROOT_DIR not in sys.path:
        sys.path.insert(0, ROOT_DIR)
    from backend.cloudv2_ping_monitoring import main as monitor_main

    monitor_main()


if __name__ == "__main__":
    main()
