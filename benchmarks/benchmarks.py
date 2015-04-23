from __future__ import absolute_import

from terminaltables import UnixTable
import contextlib
import glob
import locale
import os
import inspect
import imp
import time


locale.setlocale(locale.LC_ALL, 'en_US')


class Benchmarks(object):
    @property
    def benchmark_files(self):
        return glob.glob("**/*_benchmark.py")

    def __init__(self):
        self.iterations = 100000
        self.verbose = True

    def run(self):
        for klass in self._yield_benchmark_classes():
            self.execute_benchmark_class(klass)

    def execute_benchmark_class(self, klass):
        instance = klass()

        self._print_progress("\n\nRunning %s" % klass.__name__)

        results = []
        for benchmark_method in self._yield_benchmark_methods(instance):
            executor = BenchmarkExecutor(
                benchmark_method,
                self._get_argument_generator_functions(instance, benchmark_method),
                self.iterations
            )
            self._print_progress("\nStarting %s" % executor)
            result = executor.execute_and_get_result()
            results.append(result)
            self._print_progress(result)

        if hasattr(instance, 'class_teardown'):
            instance.class_teardown()

        self._print_output()
        self._print_output(BenchmarkResult.create_table(klass, results))

    def _yield_benchmark_methods(self, instance):
        for attr_name in dir(instance):
            method = getattr(instance, attr_name)
            if attr_name.startswith('benchmark_') and inspect.ismethod(method):
                yield method

    def _yield_benchmark_classes(self):
        for module in self._yield_benchmark_modules():
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if attr_name.endswith('Benchmark') and inspect.isclass(attr):
                    yield attr

    def _yield_benchmark_modules(self):
        for path in self.benchmark_files:
            yield self._load_module(path)

    def _load_module(self, path):
        basename = os.path.basename(path)
        basename_without_extension = basename[:-3]

        return imp.load_source(basename_without_extension, path)

    def _get_argument_generator_functions(self, instance, benchmark_method):
        arg_spec = inspect.getargspec(benchmark_method)
        return [getattr(instance, "yield_%s" % arg)() for arg in arg_spec.args[1:]]

    def _print_output(self, output=""):
        print output

    def _print_progress(self, output=""):
        if self.verbose:
            print output


class BenchmarkExecutor(object):
    def __init__(self, method, argument_context_managers, iterations):
        self.method = method
        self.argument_context_managers = argument_context_managers
        self.iterations = iterations

    def execute_and_get_result(self):
        with contextlib.nested(*self.argument_context_managers) as args:
            self._warm(*args)
            self._execute(*args)

        return BenchmarkResult(
            self.method,
            self.start_time,
            self.end_time,
            self.iterations
        )

    def __str__(self):
        return "%s Executor" % self.method.__name__

    def _warm(self, *args):
        for _ in xrange(5):
            self.method(*args)

    def _execute(self, *args):
        method = self.method
        self.start_time = time.time()
        for _ in xrange(self.iterations):
            method(*args)
        self.end_time = time.time()


class BenchmarkResult(object):
    results_table_header = [
        'Benchmark',
        'Iterations',
        'Runtime (seconds)',
        'Rate (per second)'
    ]

    @property
    def runtime_seconds(self):
        return self.end_time - self.start_time

    @property
    def rate_per_second(self):
        return self.iterations / self.runtime_seconds

    @property
    def benchmark_name(self):
        return self.method.__name__

    @property
    def formatted_iterations(self):
        return locale.format("%d", self.iterations, grouping=True)

    @property
    def formatted_rate_per_second(self):
        return locale.format("%.2f", self.rate_per_second, grouping=True)

    @property
    def formatted_runtime_seconds(self):
        return locale.format("%.4f", self.runtime_seconds, grouping=True)

    @classmethod
    def create_table(cls, klass, results):
        table_data = (
            [cls.results_table_header] +
            [result._get_table_row() for result in results]
        )
        return UnixTable(table_data, klass.__name__).table

    def __init__(self, method, start_time, end_time, iterations):
        self.method = method
        self.start_time = start_time
        self.end_time = end_time
        self.iterations = iterations

    def __str__(self):
        return "%s\nTotal Time (%s iters): %s seconds\nRate: %s/second" % (
            self.benchmark_name,
            self.formatted_iterations,
            self.formatted_runtime_seconds,
            self.formatted_rate_per_second
        )

    def _get_table_row(self):
        return [
            self.benchmark_name,
            self.formatted_iterations,
            self.formatted_runtime_seconds,
            self.formatted_rate_per_second
        ]


if __name__ == "__main__":
    Benchmarks().run()
