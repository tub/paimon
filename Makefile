DEFAULT_JAVA := /usr/lib/jvm/java-11-amazon-corretto

ifneq ($(wildcard $(DEFAULT_JAVA)),)
    export JAVA_HOME := $(DEFAULT_JAVA)
    export JAVA := $(JAVA_HOME)/bin/java
    export PATH := $(JAVA_HOME)/bin:$(PATH)
endif

.PHONY: test
test:
	mvn -B clean package -DskipTests -Drat.skip=true -Dcheckstyle.skip
