storm-hellocounter
==================

Storm topology for counting words.



## Running

* Execute the command: mvn -f pom.xml compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass="TopologyMain" -Dexec.args="src/main/resources/words.txt"
