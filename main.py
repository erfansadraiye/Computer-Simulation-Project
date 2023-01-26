import queue
from collections import namedtuple

import numpy as np
import simpy


class CPU:
    def __init__(self, env, quan_times):
        self.env = env
        self.pq = simpy.PriorityResource(env)
        self.r1 = simpy.Resource(env)
        self.r2 = simpy.Resource(env)
        self.fcfs = simpy.Resource(env)
        self.r1.q_time, self.r2.q_time = quan_times

    def get_cnt(self):
        return len(self.r1.queue) + len(self.r2.queue) + len(self.fcfs.queue)


def dispatcher(env, interval_rate, service_rate, no_jobs, quan_times, k, dispatch_rate, simulation_time):
    # DispatchLode = namedtuple()
    counters = [0 for _ in range(simulation_time // dispatch_rate)]
    cpu = CPU(env, quan_times)
    for i in range(no_jobs):
        # create random numbers accordingly
        priority = np.random.choice([1, 2, 3], 1, p=[0.1, 0.2, 0.7])[0]
        service_time = np.random.exponential(service_rate)
        until_next = np.random.exponential(interval_rate)

        # fetch next not-full dispatch
        next_dispatch = get_dispatch_time(counters, dispatch_rate, env, k, priority)

        # process job
        j = job_creator(i, env, service_time, cpu, k, dispatch_rate, next_dispatch - env.now, - env.now)
        env.process(j)

        yield env.timeout(until_next)


def get_dispatch_time(counters, dispatch_rate, env, k, priority):
    cnt = int((env.now // dispatch_rate) + 1)
    while (counters[cnt] > k) and (cnt < len(counters)):
        cnt += 1
    # next dispatch will be release a few milliseconds with delay relative to their priority
    next_dispatch = cnt * dispatch_rate + 0.00001 * (priority - 1) / dispatch_rate
    return next_dispatch


"""
    priority policy:
        - if there is any leftovers from previous dispatch, they should go first
        - if all jobs from previous dispatches are done, then dispatch based on priority
        - if two jobs have arrival difference less than 1; then dispatch based on priority
"""


def job_creator(job_id, env, s_time, cpu, k, dispatch_rate, until_next_dispatch, priority):
    start = env.now
    print('----------------\n', 'TIMESTAMP:', env.now, '\n', 'JOB ID: ', job_id)
    print('JOB CREATED', 'with priority', priority, 'and time to dispatch:', until_next_dispatch)

    # wait until dispatch
    yield env.timeout(until_next_dispatch)

    if cpu.get_cnt() < k:

        # Priority Queue
        with cpu.pq.request(priority=priority) as pqreq:
            yield pqreq
            print('exited PQ', job_id, len(cpu.pq.queue))
        # Round-Robin-T1 queue enter on dispatch
        with cpu.r1.request() as r1req:
            print('----------------\n', 'TIMESTAMP:', env.now, '\n', 'JOB ID: ', job_id)
            print('R1 QUEUE ENTRANCE')
            yield r1req
            to_process = min([s_time, cpu.r1.q_time])
            yield env.timeout(to_process)
            s_time -= to_process
            print('----------------\n', 'TIMESTAMP:', env.now, '\n', 'JOB ID: ', job_id)
            print('R1 QUEUE SUCCESSFULL')

        if s_time > 0:
            # Round-Robin-T2 queue
            with cpu.r2.request() as r2req:
                print('----------------\n', 'TIMESTAMP:', env.now, '\n', 'JOB ID: ', job_id)
                print('R2 QUEUE ENTRANCE')
                yield r2req
                to_process = min([s_time, cpu.r2.q_time])
                yield env.timeout(to_process)
                s_time -= to_process
                print('----------------\n', 'TIMESTAMP:', env.now, '\n', 'JOB ID: ', job_id, '\n----------------')
                print('R2 QUEUE SUCCESSFULL')

        if s_time > 0:
            # FCFS queue
            with cpu.fcfs.request() as r2req:
                yield r2req
                yield env.timeout(s_time)
    else:
        env.process(job_creator(env, s_time, cpu, k, dispatch_rate, dispatch_rate))


if __name__ == '__main__':
    simulation_time = 100
    no_of_jobs = 10
    x = 2
    y = 3
    z = 10
    job_deploy_rate = 2
    k = 10
    t1 = 10
    t2 = 20
    env = simpy.Environment()
    env.process(dispatcher(env, x, y, no_of_jobs, [t1, t2], k, job_deploy_rate, simulation_time))
    env.run(until=simulation_time)
