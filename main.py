import numpy as np
import simpy


class CPU:
    def __init__(self, env, quan_times):
        self.env = env
        self.pq = simpy.PriorityResource(env)
        self.r1 = simpy.Resource(env)
        self.r2 = simpy.Resource(env)
        self.fcfs = simpy.Resource(env)
        self.cpu_core = simpy.PriorityResource(env)
        self.r1.q_time, self.r2.q_time = quan_times

    def get_cnt(self):
        return len(self.r1.queue) + len(self.r2.queue) + len(self.fcfs.queue)

    def dispatcher(self, to_process, priority):
        with self.cpu_core.request(priority=priority) as core_req:
            yield core_req
            yield self.env.timeout(to_process)


def job_source(env, interval_rate, service_rate, no_jobs, quan_times, k, dispatch_rate, simulation_time):
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
        j = job_creator(i, env, service_time, cpu, k, dispatch_rate, next_dispatch - env.now, priority)
        env.process(j)

        yield env.timeout(until_next)


def get_dispatch_time(counters, dispatch_rate, env, k, priority):
    cnt = int((env.now // dispatch_rate) + 1)
    while (counters[cnt] > k) and (cnt < len(counters)):
        cnt += 1
    # next dispatch will be release a few milliseconds with delay relative to their priority
    next_dispatch = cnt * dispatch_rate
    next_dispatch += 0.001 * (priority - 1) / dispatch_rate
    next_dispatch += 0.00001 * round((env.now - next_dispatch + dispatch_rate) / dispatch_rate, 2)
    return next_dispatch


"""
    priority policy:
        - if there is any leftovers from previous dispatch, they should go first
        - if all jobs from previous dispatches are done, then dispatch based on priority
        - if two jobs have arrival difference less than 1; then dispatch based on priority
"""


def print_stuff(time_stamp: object, job_id: object, message: object) -> object:
    print('----------------\n', 'TIMESTAMP:', time_stamp, '\n', 'JOB ID: ', job_id)
    print(message)


def job_creator(job_id, env, s_time, cpu, k, dispatch_rate, until_next_dispatch, priority):
    start = env.now
    print_stuff(env.now, job_id,
                'JOB CREATED with priority ' + str(priority) + ' dispatch time: ' + str(until_next_dispatch + start))

    # wait until dispatch
    yield env.timeout(until_next_dispatch)

    if cpu.get_cnt() < k:

        # Priority Queue
        with cpu.pq.request(priority=priority) as pqreq:
            yield pqreq
            print_stuff(env.now, job_id, 'exited PQ')

        # Round-Robin-T1 queue enter on dispatch
        with cpu.r1.request() as r1req:
            print_stuff(env.now, job_id, 'R1 QUEUE ENTRANCE')
            yield r1req
            to_process = min([s_time, cpu.r1.q_time])
            yield env.process(cpu.dispatcher(to_process, 1))
            s_time -= to_process
            print_stuff(env.now, job_id, 'R1 QUEUE SUCCESSFUL')

        if s_time > 0:
            # Round-Robin-T2 queue
            with cpu.r2.request() as r2req:
                print_stuff(env.now, job_id, 'R2 QUEUE ENTRANCE')
                yield r2req
                to_process = min([s_time, cpu.r2.q_time])
                yield env.process(cpu.dispatcher(to_process, 2))
                s_time -= to_process
                print_stuff(env.now, job_id, 'R1 QUEUE SUCCESSFUL')

        if s_time > 0:
            # FCFS queue
            with cpu.fcfs.request() as r2req:
                print_stuff(env.now, job_id, 'FCFS QUEUE ENTRANCE')
                yield r2req
                yield env.process(cpu.dispatcher(to_process, 3))
                print_stuff(env.now, job_id, 'FCFS QUEUE SUCCESSFUL')

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
    env.process(job_source(env, x, y, no_of_jobs, [t1, t2], k, job_deploy_rate, simulation_time))
    env.run(until=simulation_time)
