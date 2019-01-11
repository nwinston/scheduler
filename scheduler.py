#!/usr/bin/env python3
import csv
import sys
from collections import namedtuple, defaultdict
from scipy.optimize import linear_sum_assignment
import numpy as np
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('periods', type=int, nargs='?', default=6, help='number of time periods being scheduled')
parser.add_argument('-workers', type=str, nargs='?',
    default='workers.txt', help='list of workers in order of precedence')
parser.add_argument('-jobs', type=str, nargs='?',
    default='joblist.txt', help='list of jobs being assigned')
parser.add_argument('-requests', type=str, nargs='?',
    default='requests.csv', help='job requests from each worker (see README for more details)')
parser.add_argument('-output', type=str, nargs='?',
    default='schedule.csv', help='schedule output file')
args = parser.parse_args()

# Costs
PRECEDENCE_WEIGHT = -5
FIRST_CHOICE      = -3
SECOND_CHOICE     = -2
THIRD_CHOICE      = -1
NO_PREF           = 0
LAST_CHOICE       = 20


def load_to_list(file):
    with open(file) as f:
        return [line.strip() for line in f.readlines()]


def sort_requests(requests, workers):
    '''
    Sorts self.requests in order of precedence
    '''
    return sorted(requests,
                  reverse=True,
                  key=lambda req: workers.index(req.Name))


def job_counts(periods, requests, jobs):
    '''
    Assigns number of jobs each worker has to work

    Args:
        - number of periods
        - list of ordered worker requests
        - list of job names
    '''
    total_jobs = periods * len(jobs)

    pool = defaultdict(int)
    for i in range(total_jobs):
        ndx = i % len(requests)
        pool[requests[ndx].Name] += 1
    return pool


def calc_costs(request, initial_cost, jobs):
    '''
    Args:
        - Worker's job requests
        - base cost
        - list of job names
    '''
    costs = []
    for job in jobs:
        cost = initial_cost
        if job == request.Choice1:
            cost += FIRST_CHOICE
        elif job == request.Choice2:
            cost += SECOND_CHOICE
        elif job == request.Choice3:
            cost += THIRD_CHOICE
        elif job == request.ChoiceLast:
            cost += LAST_CHOICE
        else:
            cost += NO_PREF

        costs.append(cost)
    return np.array(costs)


def assign_schedule(periods, requests, jobs):
    '''
    Args:
        - number of periods
        - list of requests
        - list of job names
    '''
    counts = job_counts(periods, requests, jobs)
    min_cost = PRECEDENCE_WEIGHT * len(requests)

    schedule = defaultdict(list)

    for period in range(periods):
        cost_matrix = []
        periods_left = periods - period

        for ndx, request in enumerate(requests):
            initial_cost = PRECEDENCE_WEIGHT * ndx
            shifts_left = counts[request.Name]

            # If a worker has as many shifts left as periods,
            # they get highest precedence 
            if shifts_left == periods_left:
                initial_cost += min_cost
            if shifts_left == 0:
                initial_cost = sys.maxsize

            costs = calc_costs(request, initial_cost, jobs)
            cost_matrix.append(costs)

        # Use the hungarian algorithm to calculate the optimal worker
        # for each job at this period
        row_ind, col_ind = linear_sum_assignment(cost_matrix)

        workers = [requests[i].Name for i in row_ind]
        jobs = [jobs[i] for i in col_ind]

        for w,j in zip(workers, jobs):
            counts[w] -= 1
            schedule[j].append(w)

    return schedule


def to_csv(schedule, dest):
    with open(dest, 'w') as csvfile:
        writer = csv.writer(csvfile)
        for job in schedule:
            writer.writerow([job] + schedule[job])


if __name__ == '__main__':
    workers = load_to_list(args.workers)
    jobs = load_to_list(args.jobs)

    with open(args.requests) as f:
        reader = csv.reader(f)
        data = namedtuple('Request', next(reader))
        requests = [row for row in map(data._make, reader)]

    requests = sort_requests(requests, workers)

    schedule = assign_schedule(args.periods, requests, jobs)
    to_csv(schedule, args.output)
