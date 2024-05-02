from provisioning import OptimizedLithopsWrapper
from datasource import InputS3, OutputS3
from lithops import Storage

if __name__ == "__main__":
    user_config = {
        "log_level": "INFO",
    }

    wrapper = OptimizedLithopsWrapper(user_config=user_config)
    storage = Storage()

    paths = [
        InputS3(
            bucket="ayman-extract",
            key="partitions/ms_zip/sb205.ms.zip",
        )
    ]

    storage.head_object(paths[0].bucket, paths[0].key)

    wrapper.map(
        map_function=lambda x: print(x),
        map_iterdata=paths,
    )
