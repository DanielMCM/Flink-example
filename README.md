# Flink-example
Flink implementation example based on university assignment

# 1.- Prerequisites 

(Ubuntu os required)

## 1.1.- Flink installation

 1. Download and unzip Flink --> https://flink.apache.org/downloads.html
 2. Configure ./conf/masters by adding localhost:8081 and ./conf/slaves by adding two localhost lines
 3. Execute ./bin/start-cluster.sh and go lo localhost:8081 to check that the dashboard is running
 
## 1.2.- Git setup & Maven

 1. Download the git repository
 2. Unzip the file in ./master/Data
 3. Install apache maven --> https://maven.apache.org/

# 2.- Execution

 1. cd to the git repository
 2. Execute in command line:
 
 ```
 mvn clean package -Pbuild-jar
 ```
 3. Execute the following code where $PATH_TO_OUTPUT_FOLDER should be the complete path in your computer of ./master/Output and $PATH_TO_INPUT_FILE should be ./master/Data/traffic-3xways (also complete path)
 ```
 flink run -p 10 -c master2018.flink.VehicleTelematics target/flink-quickstart-java.jar $PATH_TO_INPUT_FILE $PATH_TO_OUTPUT_FOLDER
 ````
 4. You should be able to follow the execution in localhost:8081, then 3 files will be created in ./master/Output, they can be empty since only a sample of the dataset is uploaded:
    - speedfines.csv --> cars that overcome the speed limit of 90 mph
    - avgspeedfines.csv --> cars with an average speed higher than 60 mph between segments 52 and 56
    - accidents.csv --> detects stopped vehicles on any segment. A vehicle is stopped when it reports at least 4 consecutive events from the same position.
  
