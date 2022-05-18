
import sys
import os
import time
from multiprocessing import cpu_count

module_path = os.path.realpath(os.path.dirname(os.path.realpath(__file__)) + '/../')
sys.path = [module_path] + sys.path
from PDC_client.submodules.api import case_metadata, metadata

study_id = '3c0a00b6-154c-11ea-9bfa-0a42f3c845fe'

def time_f(f, *args, **kwargs):
    start = time.time()
    r = f(*args, **kwargs)
    end = time.time()
    return end - start, r

mult = int(cpu_count() / 2)
count = 10
thread_counts = [max(1, x * mult) for x in range(count)]
print(f'Testing thread counts: {thread_counts}')

def run_test(f):
    print(f'Testing {f}')
    for c in thread_counts:
        sys.stdout.write(f'{c}\t')
        elapsed, _ = time_f(f, study_id, max_threads=c)
        sys.stdout.write(f'{elapsed}\n')

run_test(case_metadata)
run_test(metadata)
