storm-hellocounter
==================

Storm topology for counting words.

## Installation
* Clone the project ( git clone git@github.com:domenicosolazzo/storm-hellocounter.git )

## Running

* mvn -f pom.xml compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass="TopologyMain" -Dexec.args="src/main/resources/words.txt"
