#!/usr/bin/env python3
import csv
import sys
from collections import namedtuple, defaultdict
from scipy.optimize import linear_sum_assignment
import numpy as np
import operator
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('intervals', type=int, nargs='?', default=4, help='number of time periods being scheduled')
parser.add_argument('-workers', type=str, nargs='?',
	default='example_workers.txt', help='list of workers in order of precedence')
parser.add_argument('-jobs', type=str, nargs='?',
	default='example_joblist.txt', help='list of jobs being assigned')
parser.add_argument('-requests', type=str, nargs='?',
	default='example_requests.csv', help='job requests from each worker (see README for more details)')
parser.add_argument('-output', type=str, nargs='?',
	default='schedule.csv', help='schedule output file')
args = parser.parse_args()

# Text file containing the list of workers 
### THIS LIST MUST BE ORDERED BY PRECEDENCE ###
WORKERS = args.workers

# Text file with the list of jobs
JOBS = args.jobs

'''
CSV file with job requests from workers
Required column names:
Name - name of worker
Job1 - first job choice
Job2 - second job choice
Job3 - third job choice
JobLast - absolutely last job choice
'''
REQUESTS = args.requests

OUTPUT = args.output

# Weights to be used when calculating the cost matrix
PRECEDENCE_WEIGHT_INCREMENT = -5
FIRST_CHOICE  = -6
SECOND_CHOICE = -2
THIRD_CHOICE  = -1
LAST_CHOICE   = 100

NUM_JOB_INTERVALS = args.intervals

with open(JOBS) as f:
	job_names = [line.strip() for line in f.readlines()]


TOTAL_JOBS = NUM_JOB_INTERVALS * len(job_names)

# Load the list of workers
with open(WORKERS) as f:
	precedence_list = [line.strip() for line in f.readlines()]


with open(REQUESTS, newline='') as f:
	reader = csv.reader(f)
	data = namedtuple('Request', next(reader))
	job_requests = [row for row in map(data._make, reader)]

# Sort the request list in order of precedence
ordered_requests = sorted(job_requests, key=lambda req: (len(precedence_list) - precedence_list.index(req.Name)))

MAX_JOB_COST = PRECEDENCE_WEIGHT_INCREMENT * len(ordered_requests) * .25
print(MAX_JOB_COST)


weights = {}
for ndx, req in enumerate(ordered_requests):
	weights[req.Name] = PRECEDENCE_WEIGHT_INCREMENT * ndx

# Assign how many jobs each worker has based on precedence order
# Workers with lower precedence may have to work more jobs
pool = defaultdict(int)
for i in range(TOTAL_JOBS):
	name = ordered_requests[(i % len(ordered_requests))].Name
	pool[name] += 1

cpy = dict(pool)

max_weight = PRECEDENCE_WEIGHT_INCREMENT * len(ordered_requests)

def calc_costs(row, intervals_remaining):
	'''
	Calculates a row in the cost matrix based on the given job requests
	and worker's precedence
	'''
	name = row.Name
	jobs_left = pool[name]

	costs = []
	for job in job_names:
		cost = weights[name]

		# So we don't run into the problem where a worker has more jobs left
		# than intervals remaining
		if jobs_left == intervals_remaining:
			cost = -sys.maxsize - max_weight - weights[name] - 1

		if job == row.Choice1:
			cost += FIRST_CHOICE
		if job == row.Choice2:
			cost += SECOND_CHOICE
		if job == row.Choice3:
			cost += THIRD_CHOICE
		if job == row.ChoiceLast:
			cost += LAST_CHOICE
		costs.append(cost)

	return costs

schedule = defaultdict(list)
for interval in range(NUM_JOB_INTERVALS):
	cost_matrix = []
	ordered_requests = [r for r in ordered_requests if pool[r.Name] > 0]

	for row in ordered_requests:
		# Lessen the order of precedence as interval increases
		# and ignore order completely towards the end 
		cost_matrix.append(np.array(calc_costs(row, NUM_JOB_INTERVALS - interval)))

	# Use the hungarian algorithm to calculate the optimal worker
	# for each job at this interval
	row_ind, col_ind = linear_sum_assignment(cost_matrix)

	for i in range(len(row_ind)):
		worker_ind = row_ind[i]
		job_ind = col_ind[i]

		worker = ordered_requests[worker_ind].Name
		job = job_names[job_ind]

		pool[worker] -= 1
		schedule[job].append(worker)


INTERVAL_HEADERS = ['1', '2', '3', '4', '5', '6']
with open(OUTPUT, 'w') as csvfile:
	writer = csv.writer(csvfile)
	writer.writerow([''] + list(range(NUM_JOB_INTERVALS)))
	for job in schedule:
		writer.writerow([job] + schedule[job])
check = defaultdict(int)
for k in schedule:
	for n in schedule[k]:
		check[n] += 1
for k in check:
	if check[k] != cpy[k]:
		print(k + ': actual shifts ' + str(check[k]) + ', supposed shifts ' + str(cpy[k]))
print(schedule)