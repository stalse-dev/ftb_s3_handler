import unittest
from ftb_s3_handler.s3_client import S3Client


# testando se as instâncias do singleton serão sempre a mesma
def test_s3_client_singleton_instances() -> None:
    instance_1 = S3Client()
    instance_2 = S3Client()
    instance_3 = S3Client()

    assert instance_1 is instance_2 is instance_3

def test_s3_client_singleton_instances_thread_safe() -> None:
    import threading
    from concurrent.futures import ThreadPoolExecutor

    instances = []
    lock = threading.Lock()

    def get_instance():
        instance = S3Client()
        with lock:
            instances.append(instance)

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_instance) for _ in range(10)]
        for future in futures:
            future.result()

    first_instance = instances[0]
    for instance in instances[1:]:
        assert instance is first_instance


if __name__ == '__main__':
    unittest.main()
