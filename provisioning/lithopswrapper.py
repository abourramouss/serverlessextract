from lithops import Storage, FunctionExecutor
from datasource import InputS3


class SimpleOptimizer:
    def __init__(self, base_memory=128, scale_factor=1.5, base_workers=5):
        self.base_memory = base_memory
        self.scale_factor = scale_factor
        self.base_workers = base_workers

    def optimize_memory(self, total_data_size):
        memory = int(
            self.base_memory
            * (1 + (total_data_size / 1024 / 1024) ** self.scale_factor)
        )

        return memory

    def estimate_data_size(self, storage, map_iterdata):
        total_size = 0
        for key in map_iterdata:
            obj_metadata = storage.head_object(key.bucket, key.key)
            total_size += obj_metadata["content-length"]
        return total_size  # Return size in bytes


class OptimizedLithopsWrapper:
    def __init__(self, user_config: dict):
        self.user_config = user_config or {}
        self.storage = Storage(config=self.user_config)
        self.__optimizer = SimpleOptimizer()

    def map(
        self,
        map_function,
        map_iterdata,
        extra_args=None,
        extra_env=None,
        timeout=None,
        include_modules=[],
        exclude_modules=[],
    ):
        # data_size = self.__optimizer.estimate_data_size(self.storage, map_iterdata)

        # runtime_memory = self.__optimizer.optimize_memory(data_size)

        config = self.user_config.copy()
        self.__fexec = FunctionExecutor(**config)

        futures = self.__fexec.map(
            map_function,
            map_iterdata,
            extra_args=extra_args,
            extra_env=extra_env,
            timeout=timeout,
            include_modules=include_modules,
            exclude_modules=exclude_modules,
        )
        result = self.__fexec.get_result(fs=futures)
        return result
