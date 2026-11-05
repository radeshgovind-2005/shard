# Inputs Needed from the Labs and the Course

Things the repo can't set up by itself. Tick each one off and fill in README → Usage / Configuration and the
`experiments/LOG.md` environment table as they arrive.

## Docker Spark environment (cluster results are mandatory)

- [x] The lab's `docker-compose.yml` / image (or a link to it), to add under `docker/`: based on `labs/compar-spark-docker/docker-spark-env/`, now `docker/docker-compose.yml` (Jupyter dropped, image pinned)
- [x] Spark version and Scala binary version of the image (`sparkVersion` in `gradle.properties` must match; Scala must be 2.13): the lab's `spark:python3` had Spark 4.2.0, and no 4.3.0 image exists, so it's pinned to `spark:4.2.1-scala2.13-java21-ubuntu` with `sparkVersion=4.2.1`
- [x] Java version inside the image (our jar targets Java 21; if the image runs 17, drop `jvmToolchain` to 17): Java 21, so `jvmToolchain(21)` stays
- [x] Topology: number of workers, cores and memory per worker: N workers × 2 cores × 2 GB (`--scale spark-worker=N`, default 2), OrbStack on the M5
- [x] How jobs are submitted (`spark-submit` from inside the master container? master URL?): `docker compose -f docker/docker-compose.yml exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 …`; jar mounted at `/jars/shard-all.jar` (client mode, driver in the master container). Checked with SparkPi on 2026-11-04.
- [x] How the dataset gets into the containers (bind-mounted `data/`? path inside the container?): `../data` bind-mounted read-only at `/data` on the master and workers; outputs go to `/output` (`../output`)
- [ ] Spark UI / History Server ports, and where event logs are written (the profiler and benchmark agents read the REST API): master UI on `8080`, application UI on `4040–4050` (master container). Event logs and the History Server aren't set up yet.

## Lab 3 profiler

- [x] The profiler code from Lab 3 (the spec says we may modify it): `labs/compar-lab03-spark-profiling/notebooks/spark_profiler.py`
- [x] What it measures and how (REST API? event logs? Python-only?), so it can be used on this JVM pipeline: Python. It tags jobs with `setJobGroup`, maps them to stages via `statusTracker`, and sums stage metrics from the UI REST API (wall time, tasks, executor run/CPU time, input/output, shuffle R/W, spill). It's being ported to Kotlin with a `SparkListener` instead of REST: see `roadmap/01-SKELETON-AND-PROFILER.md` Part B.

## DI cluster (if it becomes available)

- [ ] Access: login host, credentials, VPN
- [ ] Resource manager (standalone / YARN / Kubernetes) and the master URL
- [ ] Spark and Java versions installed
- [ ] Node count, cores and memory per node, and our allocation (needed for the 2-node vs full-cluster speedup runs)
- [ ] Where the dataset lives or should be uploaded (HDFS / shared filesystem), and whether it's already there
- [ ] Quotas, time limits and queue rules

## Course admin

- [ ] Submission instructions (the spec says "details on how to submit will be given later")
- [ ] Group members, and the work split for the report's contributions page
