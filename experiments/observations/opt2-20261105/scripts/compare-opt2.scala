// Runs the OPT-2 comparison defined in opt2-lib.scala (see its header). Feed both files to spark-shell:
//   cat opt2-lib.scala compare-opt2.scala | spark-shell --jars /jars/shard-all.jar ...
Opt2Compare.rates("2025-01", "/output/opt2-off/o1", "/output/opt2-on/o1")
Opt2Compare.rates("2025", "/output/opt2-off-2025/o1", "/output/opt2-on-2025/o1")
Opt2Compare.trips()
System.exit(0)
