# Predicting Bus Arrival Delay and Schedule Non-Compliance

A PySpark predictive analytics platform for transport authority operator monitoring.

**Module:** ST5011CEM Big Data Programming Project
**Student:** _[your name]_ — _[your student ID]_
**Supervisor:** Mr. Siddhartha Neupane

---

## Overview

UK bus operators are legally required to publish timetables, live vehicle
positions, fares and disruptions to the Department for Transport's
[Bus Open Data Service (BODS)](https://data.bus-data.dft.gov.uk/). This project
ingests those feeds, reconstructs *observed* stop arrival times from raw vehicle
GPS pings, compares them against the published timetable, and trains models to
predict arrival delay.

The intended stakeholder is a **transport authority monitoring operator
compliance**: the platform reports, per operator and per route, whether services
meet the reliability thresholds defined in the assessment brief.

## Data sources

| Source | Catalogue | Format | Role |
|---|---|---|---|
| BODS | Timetables | GTFS | Scheduled stop times, routes, stop coordinates |
| BODS | Location (AVL) | SIRI-VM | Observed vehicle positions, collected live |
| BODS | Disruptions | SIRI-SX | Incident context joined by operator / service |

All BODS data is published under the
[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

## Setup

```bash
git clone <your-repo-url>
cd bods-project

python -m venv .venv
.venv\Scripts\activate          # Windows
pip install -r requirements.txt
```

### Credentials

The live location feed requires a free API key.

1. Register at <https://data.bus-data.dft.gov.uk/account/signup/>
2. Go to **My Account → Account Settings** and copy your API key
3. Copy `.env.example` to `.env` and paste the key in

`.env` is git-ignored. No credential is ever written into source code.

### Java / Spark

PySpark 3.5 requires a Java 8/11/17 runtime. Confirm with `java -version`.
On Windows, `HADOOP_HOME` must point to a directory containing
`bin/winutils.exe` and `bin/hadoop.dll`.

## Running the pipeline

```bash
# 1. Verify the API key works (single poll, prints one sample record)
python src/collect_avl.py --once

# 2. Start continuous collection — leave this running for several days
python src/collect_avl.py

# 3. In a second terminal, download the static catalogues
python src/download_static.py --region north_west
```

Collected data lands in `data/raw/avl/` as hourly gzip CSV files, one row per
vehicle observation.

## Repository layout

```
bods-project/
├── src/
│   ├── collect_avl.py       # live SIRI-VM collector
│   └── download_static.py   # GTFS timetables + SIRI-SX disruptions
├── data/raw/                # git-ignored
├── docs/
├── requirements.txt
├── .env.example
└── README.md
```

## Licence and attribution

Contains public sector information licensed under the Open Government Licence
v3.0. Data published by bus operators via the Bus Open Data Service, Department
for Transport.
