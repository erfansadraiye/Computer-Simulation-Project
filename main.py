import heapq

from numpy import NaN
from pandas import DataFrame
import numpy as np
import simpy


class Job:
    def __init__(self, id, created_time, service_time, priority, life_time):
        self.id = id
        self.created_time = created_time
        self.service_time = service_time
        self.priority = priority
        self.life_time = life_time
        self.is_done = False

    def __lt__(self, other):
        return (self.created_time * 0.01 + self.priority) < (other.created_time * 0.01 + other.priority)


class CPU:
    def __init__(self, env, quan_times):
        self.env = env
        self.pq = []
        self.r1 = []
        self.r1_requested = []
        self.r2 = []
        self.r2_requested = []
        self.fcfs = []
        self.fcfs_requested = []
        heapq.heapify(self.pq)
        heapq.heapify(self.r1)
        heapq.heapify(self.r2)
        heapq.heapify(self.fcfs)
        self.cpu_core = simpy.Resource(env, capacity=1)
        self.r1_q_time, self.r2_q_time = quan_times

        self.pq_lens = []
        self.r1_lens = []
        self.r2_lens = []
        self.fcfs_lens = []

        self.job_data = []
        self.time = []

        self.pq_waiting_times = {}

        self.r1_entrance_times = {}
        self.r1_exit_times = {}
        self.r1_process_finish = {}

        self.r2_entrance_times = {}
        self.r2_exit_times = {}
        self.r2_process_finish = {}

        self.fcfs_entrance_times = {}
        self.fcfs_exit_times = {}
        self.fcfs_process_finish = {}

    def dispatcher(self):
        env = self.env
        while True:
            random_order = np.random.choice([0, 1, 2], 1, p=[0.8, 0.1, 0.1])[0]
            if len(self.r1) != 0 and random_order == 0:
                job = heapq.heappop(self.r1)
                with self.cpu_core.request() as request:
                    self.r1_requested.append(job)
                    yield request
                    self.r1_requested.remove(job)
                    self.r1_exit_times[job] = env.now
                    # print_stuff(env.now, job.id, 'Start Process from R1')
                    if job.service_time <= self.r1_q_time:
                        yield env.timeout(job.service_time)
                        job.is_done = True
                    else:
                        yield env.timeout(self.r1_q_time)
                        job.service_time -= self.r1_q_time
                        if job.service_time != 0:
                            heapq.heappush(self.r2, job)
                            self.r2_entrance_times[job] = env.now
                            # print_stuff(env.now, job.id, 'R2 QUEUE ENTRANCE')
                        else:
                            job.is_done = True
                    self.r1_process_finish[job] = env.now
                    # print_stuff(env.now, job.id, 'Success Process from R1')

            elif len(self.r2) != 0 and random_order == 1:
                job = heapq.heappop(self.r2)
                with self.cpu_core.request() as request:
                    self.r2_requested.append(job)
                    yield request
                    self.r2_requested.remove(job)
                    self.r2_exit_times[job] = env.now
                    # print_stuff(env.now, job.id, 'Start Process from R2')
                    if job.service_time <= self.r2_q_time:
                        yield env.timeout(job.service_time)
                        job.is_done = True
                    else:
                        yield env.timeout(self.r2_q_time)
                        job.service_time -= self.r2_q_time
                        if job.service_time != 0:
                            heapq.heappush(self.fcfs, job)
                            self.fcfs_entrance_times[job] = env.now
                            # print_stuff(env.now, job.id, 'FCFS QUEUE ENTRANCE')
                        else:
                            job.is_done = True
                    self.r2_process_finish[job] = env.now
                    # print_stuff(env.now, job.id, 'Success Process from R2')

            elif len(self.fcfs) != 0 and random_order == 2:
                job = heapq.heappop(self.fcfs)
                with self.cpu_core.request() as request:
                    self.fcfs_requested.append(job)
                    yield request
                    self.fcfs_requested.remove(job)
                    self.fcfs_exit_times[job] = env.now
                    # print_stuff(env.now, job.id, 'Start Process from FCFS')
                    yield env.timeout(job.service_time)
                    self.fcfs_process_finish[job] = env.now
                    job.is_done = True
                    # print_stuff(env.now, job.id, 'Success Process from FCFS')
            else:
                yield env.timeout(0.1)
                if len(self.r1) == 0 and len(self.r2) == 0 and len(self.fcfs) == 0 and len(
                        self.pq) == 0 and self.cpu_core.count == 0 and len(self.cpu_core.queue) == 0:
                    break


def job_source(env, interval_rate, service_rate, dead_rate, no_jobs, cpu):
    for i in range(no_jobs):
        # create random numbers accordingly
        priority = np.random.choice([1, 2, 3], 1, p=[0.1, 0.2, 0.7])[0]
        until_next = int(np.random.exponential(interval_rate))
        service_time = int(np.random.exponential(service_rate))
        dead_time = int(np.random.exponential(dead_rate))
        # process job
        job = Job(i + 1, env.now, service_time, priority, dead_time)
        cpu.job_data.append(job)
        heapq.heappush(cpu.pq, job)
        yield env.timeout(until_next)


def check_dead_process(cpu, env):
    while True:
        cpu.pq_lens.append(len(cpu.pq))
        cpu.r1_lens.append(len(cpu.r1) + len(cpu.r1_requested))
        cpu.r2_lens.append(len(cpu.r2) + len(cpu.r2_requested))
        cpu.fcfs_lens.append(len(cpu.fcfs) + len(cpu.fcfs_requested))
        cpu.time.append(env.now)
        yield env.timeout(0.9)
        removed = 0
        new_q = []
        for job in cpu.pq:
            if cpu.env.now < (job.created_time + job.life_time):
                new_q.append(job)
            else:
                removed += 1
        cpu.pq = new_q
        heapq.heapify(cpu.pq)

        new_q = []
        for job in cpu.r1:
            if cpu.env.now < (job.created_time + job.life_time):
                new_q.append(job)
            else:
                removed += 1
        cpu.r1 = new_q
        heapq.heapify(cpu.r1)

        new_q = []
        for job in cpu.r2:
            if cpu.env.now < (job.created_time + job.life_time):
                new_q.append(job)
            else:
                removed += 1
        cpu.r2 = new_q
        heapq.heapify(cpu.r2)

        new_q = []
        for job in cpu.fcfs:
            if cpu.env.now == (job.created_time + job.life_time):
                new_q.append(job)
            else:
                removed += 1
        cpu.fcfs = new_q
        heapq.heapify(cpu.fcfs)

        yield env.timeout(0.1)


def print_stuff(time_stamp, job_id, message):
    print('----------------\n', 'TIMESTAMP:', time_stamp, '\n', 'JOB ID: ', job_id)
    print(message)


def check_pq(cpu, k, dispatcher_rate):
    env = cpu.env
    env.process(cpu.dispatcher())
    while True:
        yield env.timeout(dispatcher_rate)
        if len(cpu.pq) < k:
            jobs = heapq.nlargest(len(cpu.pq), cpu.pq)
            for job in jobs:
                cpu.pq_waiting_times[job] = env.now - job.created_time
                heapq.heappush(cpu.r1, job)
        else:
            jobs = heapq.nlargest(k, cpu.pq)
            for job in jobs:
                cpu.pq_waiting_times[job] = env.now - job.created_time
                cpu.r1_entrance_times[job] = env.now
                heapq.heappush(cpu.r1, job)


def simulate(x, y, z):
    simulation_time = 4000
    no_of_jobs = 30
    k = 10
    t1 = 5
    t2 = 10
    transfer_time = 30
    env = simpy.Environment()
    cpu = CPU(env, [t1, t2])
    env.process(job_source(env, x, y, z, no_of_jobs, cpu))
    env.process(check_pq(cpu, k, transfer_time))
    env.process(check_dead_process(cpu, env))
    env.run(until=simulation_time)

    all_data = [
        [job.id, job.priority, job.service_time, job.created_time, job.is_done,
         cpu.pq_waiting_times.get(job),

         cpu.r1_exit_times.get(job, -1) - cpu.r1_entrance_times.get(job, 0)
         if cpu.r1_exit_times.get(job, -1) >= cpu.r1_entrance_times.get(job, 0) else NaN,
         cpu.r2_exit_times.get(job, -1) - cpu.r2_entrance_times.get(job, 0)
         if cpu.r2_exit_times.get(job, -1) >= cpu.r2_entrance_times.get(job, 0) else NaN,
         cpu.fcfs_exit_times.get(job, -1) - cpu.fcfs_entrance_times.get(job, 0)
         if cpu.fcfs_exit_times.get(job, -1) >= cpu.fcfs_entrance_times.get(job, 0) else NaN,
         ] for job in cpu.job_data]
    df = DataFrame(all_data,
                   columns=['id', 'priority', 'service time', 'created time', 'is done',
                            'PQ wt',
                            'R1 wt',
                            'R2 wt',
                            'FCFS wt'
                            ])
    df_queue = DataFrame({
        'TIME': cpu.time,
        'PQ length': cpu.pq_lens,
        'R1 length': cpu.r1_lens,
        'R2 length': cpu.r2_lens,
        'FCFS length': cpu.fcfs_lens
    })

    return df, df_queue