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
PRECEDENCE_WEIGHT = 1
FIRST_CHOICE  = 1
SECOND_CHOICE = 3
THIRD_CHOICE  = 5
NO_PREF		  = 6
LAST_CHOICE   = 8

num_periods = args.periods

def load_to_list(file):
	with open(file) as f:
		return [line.strip() for line in f.readlines()]


class Scheduler:
	def __init__(self, periods, workers, jobs, requests):
		self.periods = periods
		self.workers = workers
		self.jobs = jobs
		self.requests = requests
		self.schedule = defaultdict(list)

	def _sort_requests(self):
		sort_by_precedence = lambda req: self.workers.index(req.Name)
		self.requests = sorted(requests, reverse=True, key=sort_by_precedence)

	def _create_pool(self):
		total_jobs = self.periods * len(self.jobs)

		pool = defaultdict(int)
		while total_jobs > 0:
			ndx = total_jobs % len(self.requests)
			pool[self.requests[ndx].Name] += 1
			total_jobs -= 1
		return pool

	def calc_costs(self, request, initial_cost):
		'''
		Calculates a row in the cost matrix based on the given job requests
		and worker's precedence
		'''
		name = request.Name

		costs = []
		for job in self.jobs:
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

		return costs


	def assign_schedule(self):
		'''
		'''	
		self.schedule.clear()
		self._sort_requests()
		pool = self._create_pool()

		periods_left = self.periods
		while periods_left > 0:
			cost_matrix = []
			self.requests = [r for r in self.requests if pool[r.Name] > 0]

			for ndx, request in enumerate(requests):
				weight = PRECEDENCE_WEIGHT * ndx
				costs = self.calc_costs(request, weight)
				cost_matrix.append(np.array(costs))

			# Use the hungarian algorithm to calculate the optimal worker
			# for each job at this period
			row_ind, col_ind = linear_sum_assignment(cost_matrix)
			size = len(row_ind)

			workers = [self.requests[i].Name for i in row_ind]
			jobs = [self.jobs[i] for i in col_ind]

			for w,j in zip(workers, jobs):
				pool[w] -= 1
				self.schedule[j].insert(0, w)

			periods_left -= 1


	def to_csv(self, dest):
		with open(dest, 'w') as csvfile:
			writer = csv.writer(csvfile)
			writer.writerow([''] + list(range(1, self.periods + 1)))
			for job in self.schedule:
				writer.writerow([job] + self.schedule[job])


if __name__ == '__main__':
	workers = load_to_list(args.workers)
	jobs = load_to_list(args.jobs)

	with open(args.requests) as f:
		reader = csv.reader(f)
		data = namedtuple('Request', next(reader))
		requests = [row for row in map(data._make, reader)]

	s = Scheduler(args.periods, workers, jobs, requests)
	s.assign_schedule()
	s.to_csv(args.output)
