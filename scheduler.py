import csv
import sys
from collections import namedtuple, defaultdict
from scipy.optimize import linear_sum_assignment
import numpy as np
import operator

# Text file containing the list of workers 
### THIS LIST MUST BE ORDERED BY PRECEDENCE ###
WORKERS = 'example_workers.txt'

# Text file with the list of jobs
JOBS = 'example_joblist.txt'

'''
CSV file with job requests from workers
Required column names:
Name - name of worker
Job1 - first job choice
Job2 - second job choice
Job3 - third job choice
JobLast - absolutely last job choice
'''
REQUESTS = 'example_requests.csv'

OUTPUT = 'jobs.csv'

# Weights to be used when calculating the cost matrix
PRECEDENCE_WEIGHT_INCREMENT = -5
FIRST_CHOICE  = -6
SECOND_CHOICE = -2
THIRD_CHOICE  = -1
LAST_CHOICE   = 100

INTERVAL_HEADERS = ['10:00-10:30', '10:30-11:00', '11:00-11:30', '11:30-12:00', '12:00-12:30', '12:30-Close']

NUM_JOB_INTERVALS = len(INTERVAL_HEADERS)

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

MAX_JOB_COST = PRECEDENCE_WEIGHT_INCREMENT * len(job_requests) * .3
print(MAX_JOB_COST)

# Sort the request list in order of precedence
ordered_requests = sorted(job_requests, key=lambda req: (len(precedence_list) - precedence_list.index(req.Name)))

weights = {}
for ndx, req in enumerate(ordered_requests):
	weights[req.Name] = PRECEDENCE_WEIGHT_INCREMENT * ndx

# Assign how many jobs each worker has based on precedence order
# Workers with lower precedence may have to work more jobs
pool = defaultdict(int)
for i in range(TOTAL_JOBS):
	name = ordered_requests[(i % len(ordered_requests))].Name
	pool[name] += 1

max_jobs = pool[max(pool, key=pool.get)]

cpy = dict(pool)

def calc_costs(row, mult):
	'''
	Calculates a row in the cost matrix based on the given job requests
	and worker's precedence
	'''
	name = row.Name
	jobs_left = pool[name]

	costs = []
	for job in job_names:
		cost = weights[name]
		# For the workers who have one more job than the rest, reduce
		# their cost so their first job starts at an earlier interval
		if jobs_left == max_jobs:
			cost += MAX_JOB_COST

		if job == row.Job1:
			cost += FIRST_CHOICE
		if job == row.Job2:
			cost += SECOND_CHOICE
		if job == row.Job3:
			cost += THIRD_CHOICE
		if job == row.JobLast:
			cost += LAST_CHOICE
		costs.append(cost * mult)

	return costs

schedule = defaultdict(list)
for interval in range(NUM_JOB_INTERVALS):
	cost_matrix = []
	ordered_requests = [r for r in ordered_requests if pool[r.Name] > 0]

	for row in ordered_requests:
		mult = max(.6 - interval / NUM_JOB_INTERVALS, 0)
		cost_matrix.append(np.array(calc_costs(row, mult)))

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

with open(OUTPUT, 'w') as csvfile:
	writer = csv.writer(csvfile)
	writer.writerow([''] + INTERVAL_HEADERS)
	for job in schedule:
		writer.writerow([job] + schedule[job])

check = defaultdict(int)
for k in schedule:
	for n in schedule[k]:
		check[n] += 1
for k in check:
	if check[k] != cpy[k]:
		print(k + ': ' + str(check[k]))
print(cpy == check)
print(cpy)
print('')
print(schedule)

