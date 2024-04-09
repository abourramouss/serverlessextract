from lithops import Storage
from typing import Dict
from s3path import S3Path
from skopt import gp_minimize
from skopt.space import Integer, Real
from remote_static_partition import partition_measurement_sets


class ProvisioningLayer:
    def __init__(self):
        self.storage = Storage()
        self.optimization_results = {}
        self.previous_executions = {}

    def get_optimal_parameters(
        self, input_data_path: S3Path, previous_execution_data: Dict
    ) -> Dict:
        # Check if the optimal parameters are already cached
        if str(input_data_path) in self.optimization_results:
            return self.optimization_results[str(input_data_path)]

        # Store the previous execution data
        self.previous_executions[str(input_data_path)] = previous_execution_data

        # Approximate the computational complexity based on previous execution data
        computational_complexity = self._approximate_computational_complexity(
            previous_execution_data
        )

        # Define the objective function for Bayesian Optimization
        def objective(params):
            chunk_size, runtime_memory = params
            execution_time = self._evaluate_performance(
                chunk_size, runtime_memory, computational_complexity
            )
            return execution_time

        chunk_size_space = Integer(1, 8000)
        runtime_memory_space = Integer(1024, 10240)

        # Perform Bayesian Optimization
        result = gp_minimize(
            objective,
            [
                chunk_size_space,
                runtime_memory_space,
            ],  # Pass dimensions as separate elements
            n_calls=10,  # Number of iterations for optimization
            random_state=42,
        )

        # Retrieve the optimal parameters
        optimal_chunk_size = int(result.x[0])
        runtime_memory = int(result.x[1])
        cpus_per_worker = self._determine_cpus_per_worker(runtime_memory)

        # Cache the optimization results
        self.optimization_results[str(input_data_path)] = {
            "optimal_chunk_size": optimal_chunk_size,
            "runtime_memory": runtime_memory,
            "cpus_per_worker": cpus_per_worker,
        }

        return {
            "optimal_chunk_size": optimal_chunk_size,
            "runtime_memory": runtime_memory,
            "cpus_per_worker": cpus_per_worker,
        }

    def _approximate_computational_complexity(
        self, previous_execution_data: Dict
    ) -> str:
        # Approximate the computational complexity based on previous execution data
        # use metrics like execution time, CPU utilization, or other relevant factors
        execution_time = previous_execution_data.get("execution_time")
        if execution_time < 100:
            return "low"
        elif execution_time < 500:
            return "medium"
        else:
            return "high"

    def _evaluate_performance(
        self, chunk_size, runtime_memory, computational_complexity
    ):
        # Placeholder function to evaluate the performance metric
        # Use historical data or a performance model to estimate the metric
        execution_time = chunk_size * runtime_memory  # Dummy calculation
        return execution_time

    def _determine_cpus_per_worker(self, runtime_memory: int) -> int:
        # Determine the optimal number of CPUs per worker based on the runtime memory
        # Calculate the CPU per GB ratio
        cpu_per_gb = runtime_memory / 1024
        # Set the default number of CPUs per worker
        cpus_per_worker = 1
        # Adjust the number of CPUs per worker based on the CPU per GB ratio
        if cpu_per_gb >= 0.5:
            cpus_per_worker = int(cpu_per_gb)
        return cpus_per_worker


if __name__ == "__main__":
    provisioning_layer = ProvisioningLayer()
    previous_execution_data = {
        "execution_time": 200,
    }
    optimal_parameters = provisioning_layer.get_optimal_parameters(
        S3Path("/ayman-extract/partitions/partitions_7900_12zip"),
        previous_execution_data,
    )
    print(optimal_parameters)
