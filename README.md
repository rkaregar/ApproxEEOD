# Discovering approximate Order Dependencies (ODs)

## 1. Compile the code

Go to `src/ca/uwindsor/cs/mkargar/od` folder and run the following command. Note that required libraries are already in the lib forlder 

```
javac -d ../../../../../../out -cp .:../../../../../../lib/* *.java */*.java */*/*.java */*/*/*.java
```

## 2. Set up the config file and dataset

Ensure you fill up the `config.txt` file properly. This file is
located in `/out` folder. A sample config file is located in the root 
direcotry. Also, put the dataset inside the `/out` folder.

## 3. Run the code

Go to `out` folder and run the following command.

```
java -cp .:../lib/* ca.uwindsor.cs.mkargar.od.MainClass
```

