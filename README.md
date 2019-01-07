# Scheduler

This script creates a schedule based on a hierarchical ordering as well as each worker's preferred jobs.


Commad Line Arguments
----------------------
```
usage: scheduler.py [-h] [-workers [WORKERS]] [-jobs [JOBS]]
                    [-requests [REQUESTS]] [-output [OUTPUT]]
                    intervals

positional arguments:
  intervals             number of time periods being scheduled

optional arguments:
  -h, --help            show this help message and exit
  -workers [WORKERS]    list of workers in order of precedence
  -jobs [JOBS]          list of jobs being assigned
  -requests [REQUESTS]  job requests from each worker
  -output [OUTPUT]      schedule output file

```

*defaults*
-----------
-workers = 'workers.txt'

-jobs = 'jobs.txt'

-requests = 'requests.csv'

-output = 'schedule.csv'


Example Usage
--------------
```bash
$ scheduler.py 6
```
_Assigns jobs over six time intervals_


```bash
$ scheduler.py 4 -requests requestsGY.csv
```
_Assigns jobs over 4 intervals, using the requests found in requestsGY.csv_

### Input Files

**Worker List**

A list of all workers in descending order of precedence (e.g. someone in higher standing will have higher precedence)

_sample workers.txt_
```
Worker1
Worker2 
Worker3 
...
Worker11
Worker12
```
**Job List**

Listing of jobs to be assigned

_sample joblist.txt_

```
Job1
Job2
Job3
Job4
Job5
```

**Requests**

CSV file containing workers' job preferences. The CSV file requires the following column headers

1. Name - _worker's name_
2. Choice1 - _first job choice_
3. Choice2 - _second job choice_
4. Choice3 - _third job choice_
5. ChoiceLast - _absolutely last job choice_

**Note:** The names in requests.csv **must** be a subset of those in the _worker list_, and the choices must be a subset of those in the _job list_

_sample requests.csv_

```
Name,Choice1,Choice2,Choice3,ChoiceLast
Worker1,JOB1,JOB2,JOB3,JOB5
Worker2,JOB4,JOB3,JOB1,JOB5
Worker3,JOB2,JOB5,JOB3,JOB1
Worker5,JOB1,JOB4,JOB3,JOB2
Worker7,JOB4,JOB2,JOB3,JOB1
Worker8,JOB2,JOB4,JOB1,JOB5
Worker9,JOB3,JOB1,JOB5,JOB2
Worker10,JOB2,JOB4,JOB1,JOB5
```

Theses requests result in the following schedule



|    |   0   |   1    |   2    |   3    |
|----|-------|--------|--------|--------|
|JOB4|Worker7|Worker2 |Worker7 |Worker10|
|JOB5|Worker5|Worker8 |Worker8 |Worker7 |
|JOB2|Worker3|Worker10|Worker10|Worker8 |
|JOB3|Worker2|Worker9 |Worker9 |Worker9 |
|JOB1|Worker1|Worker1 |Worker3 |Worker5 |



