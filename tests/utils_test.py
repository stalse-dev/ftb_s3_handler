import unittest
from ftb_s3_handler.utils import ShutdownManager


def test_shutdown_manager_singleton_instances() -> None:
    instance_1 = ShutdownManager()
    instance_2 = ShutdownManager()
    instance_3 = ShutdownManager()

    assert instance_1 is instance_2 is instance_3

if __name__ == '__main__':
    unittest.main()
