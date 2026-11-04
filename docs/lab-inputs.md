# Inputs Needed from the Labs and the Course

Things the repo can't set up by itself. Tick each one off and fill in README → Usage / Configuration and the
`experiments/LOG.md` environment table as they arrive.

## Docker Spark environment (cluster results are mandatory)

- [ ] The lab's `docker-compose.yml` / image (or a link to it), to add under `docker/`
- [ ] Spark version and Scala binary version of the image (`sparkVersion` in `gradle.properties` must match; Scala must be 2.13)
- [ ] Java version inside the image (our jar targets Java 21; if the image runs 17, drop `jvmToolchain` to 17)
- [ ] Topology: number of workers, cores and memory per worker
- [ ] How jobs are submitted (`spark-submit` from inside the master container? master URL?)
- [ ] How the dataset gets into the containers (bind-mounted `data/`? path inside the container?)
- [ ] Spark UI / History Server ports, and where event logs are written (the profiler and benchmark agents read the REST API)

## Lab 3 profiler

- [ ] The profiler code from Lab 3 (the spec says we may modify it)
- [ ] What it measures and how (REST API? event logs? Python-only?), so it can be used on this JVM pipeline

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
