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

# Weights to be used when calculating the cost matrix
PRECEDENCE_WEIGHT = -5
FIRST_CHOICE  = -3
SECOND_CHOICE = -2
THIRD_CHOICE  = -1
NO_PREF 	  = 0
LAST_CHOICE   = 20

def load_to_list(file):
	with open(file) as f:
		return [line.strip() for line in f.readlines()]


class Scheduler:
	def __init__(self, periods, workers, jobs, requests):
		'''
		Args:
			periods: number of periods to schedule
			workers: list of workers in order of precedence
			jobs: list of jobs
			requests: list of worker requests
		'''
		self.periods = periods
		self.workers = workers
		self.jobs = jobs
		self.requests = requests
		self.schedule = defaultdict(list)

	def _sort_requests(self):
		'''
		Sorts self.requests in order of precedence
		'''
		self.requests = sorted(requests,
								reverse=True,
				 				key=lambda req: self.workers.index(req.Name))

	def _job_counts(self):
		'''
		Assigns number of jobs each worker has to work
		'''
		total_jobs = self.periods * len(self.jobs)

		pool = defaultdict(int)
		for i in range(total_jobs):
			ndx = i % len(self.requests)
			pool[self.requests[ndx].Name] += 1
		return pool

	def calc_costs(self, request, initial_cost):
		'''
		Args:
			- Worker's job requests
			- base cost
		'''
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
		return np.array(costs)

	def assign_schedule(self):
		self.schedule.clear()
		self._sort_requests()
		pool = self._job_counts()

		min_cost = PRECEDENCE_WEIGHT * len(self.requests)

		for period in range(self.periods):
			cost_matrix = []
			periods_left = self.periods - period

			for ndx, request in enumerate(self.requests):
				initial_cost = PRECEDENCE_WEIGHT * ndx
				shifts_left = pool[request.Name]

				# If a worker has as many shifts left as periods,
				# they get highest precedence 
				if shifts_left == periods_left:
					initial_cost += min_cost
				if shifts_left == 0:
					initial_cost = sys.maxsize

				costs = self.calc_costs(request, initial_cost)
				cost_matrix.append(costs)

			# Use the hungarian algorithm to calculate the optimal worker
			# for each job at this period
			row_ind, col_ind = linear_sum_assignment(cost_matrix)
			size = len(row_ind)

			workers = [self.requests[i].Name for i in row_ind]
			jobs = [self.jobs[i] for i in col_ind]

			for w,j in zip(workers, jobs):
				pool[w] -= 1
				self.schedule[j].append(w)

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
