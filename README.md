
# CS 744 Project: Distributed Key-Value Store System

## Team Members
- Mrityunjay Shukla (24d0363)
- Kalind Karia (24d0361)

## Description

Checkout our slides for the same: [link](https://docs.google.com/presentation/d/1KEE_k0p1B20sGU4wmE_5jrWg4kj9kdCOyOVpfAcpGBM/edit?usp=sharing)

## Run Locally

### Clone the project

```bash
  git clone https://github.com/mrityunjayshukla411/decs_project.git
```

### Go to the project directory

```bash
  cd decs_project
```

### Install dependencies

```bash
  pip3 install -r requirements.txt
```

### Start the server

- Ensure the ports 5000 to 5004 are not in use.

```bash
  bash start_cluster.sh
```

### Run the evaluator for stress-test

```bash
  python3 run_evaluation.py
```

> This will generate the results for throughput, avg. response time and avg. CPU utilization in the `eval_report` directory.


### Results of our experiments

> Check out the `results` directory for the same.
