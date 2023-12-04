.PHONY: test clean

# Path to your sbt (can be just `sbt` if it's globally available)
SBT=sbt

clean:
	rm -rf spark-warehouse
	rm -rf .bsp
	$(SBT) clean

test:
	$(SBT) coverage test coverageReport
	@make clean
