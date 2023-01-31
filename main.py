import numpy as np
import simpy
import heapq


class Job:
    def __init__(self, id, created_time, service_time, priority, life_time):
        self.id = id
        self.created_time = created_time
        self.service_time = service_time
        self.priority = priority
        self.life_time = life_time

    def __lt__(self, other):
        return (self.created_time * 0.01 + self.priority) < (other.created_time * 0.01 + other.priority)


class CPU:
    def __init__(self, env, quan_times):
        self.env = env
        self.pq = []
        self.r1 = []
        self.r2 = []
        self.fcfs = []
        heapq.heapify(self.pq)
        heapq.heapify(self.r1)
        heapq.heapify(self.r2)
        heapq.heapify(self.fcfs)
        self.cpu_core = simpy.Resource(env, capacity=1)
        self.r1_q_time, self.r2_q_time = quan_times

    def dispatcher(self):
        while True:
            random_order = np.random.choice([0, 1, 2], 1, p=[0.8, 0.1, 0.1])[0]
            if len(self.r1) != 0 and random_order == 0:
                job = heapq.heappop(self.r1)
                with self.cpu_core.request() as request:
                    yield request
                    print_stuff(env.now, job.id, 'Start Process from R1')

                    if job.service_time < self.r1_q_time:
                        yield env.timeout(job.service_time)
                    else:
                        yield env.timeout(self.r1_q_time)
                        job.service_time -= self.r1_q_time
                        if job.service_time != 0:
                            heapq.heappush(self.r2, job)
                            print_stuff(env.now, job.id, 'R2 QUEUE ENTRANCE')
                    print_stuff(env.now, job.id, 'Success Process from R1')

            elif len(self.r2) != 0 and random_order == 1:
                job = heapq.heappop(self.r2)
                with self.cpu_core.request() as request:
                    yield request
                    print_stuff(env.now, job.id, 'Start Process from R2')
                    if job.service_time < self.r2_q_time:
                        yield env.timeout(job.service_time)
                    else:
                        yield env.timeout(self.r2_q_time)
                        job.service_time -= self.r2_q_time
                        if job.service_time != 0:
                            heapq.heappush(self.fcfs, job)
                            print_stuff(env.now, job.id, 'FCFS QUEUE ENTRANCE')
                    print_stuff(env.now, job.id, 'Success Process from R2')

            elif len(self.fcfs) != 0 and random_order == 2:
                job = heapq.heappop(self.fcfs)
                with self.cpu_core.request() as request:
                    yield request
                    print_stuff(env.now, job.id, 'Start Process from FCFS')
                    yield env.timeout(job.service_time)
                    print_stuff(env.now, job.id, 'Success Process from FCFS')


def job_source(env, interval_rate, service_rate, dead_rate, no_jobs, cpu):
    for i in range(no_jobs):
        # create random numbers accordingly
        priority = np.random.choice([1, 2, 3], 1, p=[0.1, 0.2, 0.7])[0]
        until_next = int(np.random.exponential(interval_rate))
        service_time = int(np.random.exponential(service_rate))
        dead_time = int(np.random.exponential(dead_rate))
        # process job
        job = Job(i + 1, env.now, service_time, priority, dead_time)
        heapq.heappush(cpu.pq, job)
        yield env.timeout(until_next)


def check_dead_process(cpu):
    while True:
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
    while True:
        yield env.timeout(dispatcher_rate)
        if len(cpu.pq) < k:
            jobs = heapq.nlargest(len(cpu.pq), cpu.pq)
            for job in jobs:
                heapq.heappush(cpu.r1, job)
        else:
            jobs = heapq.nlargest(k, cpu.pq)
            for job in jobs:
                heapq.heappush(cpu.r1, job)
        env.process(cpu.dispatcher())


if __name__ == '__main__':
    simulation_time = 60
    no_of_jobs = 50
    x = 2
    y = 30
    z = 5
    job_deploy_rate = 2
    k = 10
    t1 = 5
    t2 = 10
    transfer_time = 30
    env = simpy.Environment()
    cpu = CPU(env, [t1, t2])
    env.process(job_source(env, x, y, z, no_of_jobs, cpu))
    env.process(check_pq(cpu, k, transfer_time))
    env.process(check_dead_process(cpu))
    env.run(until=simulation_time)
