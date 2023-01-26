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


def job_source(env, interval_rate, service_rate, no_jobs, quan_times, k, transfer_rate, simulation_time):
    cpu = CPU(env, quan_times)
    for i in range(no_jobs):
        # create random numbers accordingly
        priority = np.random.choice([1, 2, 3], 1, p=[0.1, 0.2, 0.7])[0]
        service_time = np.random.exponential(service_rate)
        until_next = np.random.exponential(interval_rate)

        # fetch next not-full transfer time
        next_transfer = get_transfer_time(transfer_rate, env, priority, env.now)
        # process job
        j = job_creator(i, env, service_time, cpu, k, transfer_rate, next_transfer - env.now, priority, env.now)
        env.process(j)

        yield env.timeout(until_next)


def get_transfer_time(transfer_rate, env, priority, created_at):
    cnt = int((env.now // transfer_rate) + 1)
    next_transfer = cnt * transfer_rate
    priority_schedule = round((0.5 ** priority) / transfer_rate, 3)
    arrival_schedule = max(round(0.5 ** round(1 + ((next_transfer - created_at) / transfer_rate), 4) / transfer_rate, 10), 0)
    next_transfer += priority_schedule + arrival_schedule
    return next_transfer


"""
    priority policy:
        - if a 
        - the further we are from a task creation time, the higher it will be prioritized after each transfer
        - 
"""


def print_stuff(time_stamp, job_id, message):
    print('----------------\n', 'TIMESTAMP:', time_stamp, '\n', 'JOB ID: ', job_id)
    print(message)


def job_creator(job_id, env, s_time, cpu, k, transfer_rate, until_next_transfer, priority, created_at):
    start = env.now
    print_stuff(env.now, job_id,
                'JOB SCHEDULED with priority ' + str(priority) + ' for transfer time: ' + str(
                    until_next_transfer + start) + ' creation time: ' + str(created_at) + ' service time: ' + str(s_time))

    # wait until dispatch
    yield env.timeout(until_next_transfer)

    if cpu.get_cnt() < k:
        # Priority Queue
        with cpu.pq.request(priority=priority) as pqreq:
            yield pqreq

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
                print_stuff(env.now, job_id, 'R2 QUEUE SUCCESSFUL')

        if s_time > 0:
            # FCFS queue
            with cpu.fcfs.request() as r2req:
                print_stuff(env.now, job_id, 'FCFS QUEUE ENTRANCE')
                yield r2req
                yield env.process(cpu.dispatcher(to_process, 3))
                print_stuff(env.now, job_id, 'FCFS QUEUE SUCCESSFUL')

    else:
        new_transfer = get_transfer_time(transfer_rate, env, priority, created_at)
        print_stuff(env.now, job_id, 'JOB RESCHEDULING')
        yield env.process(
            job_creator(job_id, env, s_time, cpu, k, transfer_rate, new_transfer - env.now, priority, created_at)
        )


if __name__ == '__main__':
    simulation_time = 1000
    no_of_jobs = 50
    x = 2
    y = 30
    z = 10
    job_deploy_rate = 2
    k = 10
    t1 = 5
    t2 = 10
    env = simpy.Environment()
    env.process(job_source(env, x, y, no_of_jobs, [t1, t2], k, job_deploy_rate, simulation_time))
    env.run(until=simulation_time)
