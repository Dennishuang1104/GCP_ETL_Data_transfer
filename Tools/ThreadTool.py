from concurrent.futures import ThreadPoolExecutor, as_completed


class ThreadTool:
    def __init__(self, worker_num: int):
        self.worker_num = worker_num
        self.threads = []
        self.executor = ThreadPoolExecutor(max_workers=self.worker_num)

    def check_thread(self):
        try:
            for task in as_completed(self.threads):
                thread_result = task.result()
                if thread_result is not None:
                    print(f'thread_result: {thread_result}')
        except Exception as e:
            raise

    def return_thread(self):
        try:
            for task in as_completed(self.threads):
                thread_result = task.result()
                print(thread_result)
                if thread_result is not None:
                    return thread_result
        except Exception as e:
            raise