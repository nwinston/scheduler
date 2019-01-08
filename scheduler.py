#!/usr/bin/env python3
import csv
import sys
from collections import namedtuple, defaultdict
from scipy.optimize import linear_sum_assignment
import numpy as np
import operator
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
PRECEDENCE_WEIGHT_INCREMENT = -12
FIRST_CHOICE  = -6
SECOND_CHOICE = -4
THIRD_CHOICE  = -3
LAST_CHOICE   = 100

num_periods = args.periods

with open(JOBS) as f:
	job_names = [line.strip() for line in f.readlines()]

# Load the list of workers
with open(WORKERS) as f:
	precedence_list = [line.strip() for line in f.readlines()]

with open(REQUESTS, newline='') as f:
	reader = csv.reader(f)
	data = namedtuple('Request', next(reader))
	job_requests = [row for row in map(data._make, reader)]

# Sort the request list in order of precedence
ordered_requests = sorted(job_requests, key=lambda req: (len(precedence_list) - precedence_list.index(req.Name)))

weights = {}
for ndx, req in enumerate(ordered_requests):
	weights[req.Name] = PRECEDENCE_WEIGHT_INCREMENT * ndx

# Assign how many jobs each worker has based on precedence order
# Workers with lower precedence may have to work more jobs
pool = defaultdict(int)
total_jobs = num_periods * len(job_names)
for i in range(total_jobs):
	name = ordered_requests[(i % len(ordered_requests))].Name
	pool[name] += 1

max_weight = PRECEDENCE_WEIGHT_INCREMENT * len(ordered_requests)

def calc_costs(request, periods_remaining):
	'''
	Calculates a row in the cost matrix based on the given job requests
	and worker's precedence
	'''
	name = request.Name
	jobs_left = pool[name]

	costs = []
	for job in job_names:
		cost = weights[name]

		# So we don't run into the problem where a worker has more jobs left
		# than periods remaining, give workers that have to work during each
		# of the remaining periods a very low cost
		if jobs_left == periods_remaining:
			cost = -sys.maxsize - max_weight - weights[name] - 1

		if job == request.Choice1:
			cost += FIRST_CHOICE
		if job == request.Choice2:
			cost += SECOND_CHOICE
		if job == request.Choice3:
			cost += THIRD_CHOICE
		if job == request.ChoiceLast:
			cost += LAST_CHOICE
		costs.append(cost)

	return costs

schedule = defaultdict(list)
for period in range(num_periods):
	cost_matrix = []
	ordered_requests = [r for r in ordered_requests if pool[r.Name] > 0]

	for request in ordered_requests:
		cost_matrix.append(np.array(calc_costs(request, num_periods - period)))

	# Use the hungarian algorithm to calculate the optimal worker
	# for each job at this period
	row_ind, col_ind = linear_sum_assignment(cost_matrix)

	for i in range(len(row_ind)):
		worker_ind = row_ind[i]
		job_ind = col_ind[i]

		worker = ordered_requests[worker_ind].Name
		job = job_names[job_ind]

		pool[worker] -= 1
		schedule[job].append(worker)

with open(OUTPUT, 'w') as csvfile:
	writer = csv.writer(csvfile)
	writer.writerow([''] + list(range(1, num_periods + 1)))
	for job in schedule:
		writer.writerow([job] + schedule[job])