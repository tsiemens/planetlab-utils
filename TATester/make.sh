mkdir bin
javac -cp "java-json.jar:src" src/ca/NetSysLab/UDPClient/*.java -d bin
jar cfm tester.jar manifest.mf -C bin .
