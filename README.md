# BigDataLab
## How to run task 2.1
### Precondition
- Already set up Hadoop version 3.3.6
- Install WSL2 with Ubuntu (Recommend)
- Install default JRE/JDK (version 11.0.14) or Oracle JDK 11
- Set local variable in .bashrc file:
1. export PATH="$PATH:~/hadoop-3.3.6/bin"
2. export PATH="$PATH:~/hadoop-3.3.6/sbin"
3. export HADOOP_HOME=path/to/location/of/hadoop
4. export HADOOP_COMMON_JAR=$HADOOP_HOME/share/hadoop/common/hadoop-common-3.3.6.jar
5. export HADOOP_MAPREDUCE_JAR=$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.3.6.jar
6. export HADOOP_CLI_JAR=$HADOOP_HOME/share/hadoop/common/lib/commons-cli-1.2.jar
### Step
*** You must be already in the file location of input file, Java file, ...
0. Set up Hadoop browse file system
   - Start the hadoop localhost: $ start-all.sh
   - Make directory of input file: $ hadoop fs -mkdir /path/of/csv/file
   - Upload csv file into file system: $ hadoop fs -put /csv/file
1. Run CSVReader
   - Compile file Java code: $ javac -classpath  $HADOOP_COMMON_JAR:$HADOOP_MAPREDUCE_JAR:HADOOP_CLI_JAR -d CSVReader.classes CSVReader.java
   - Create JAR file: $ jar -cvf CSVReader.jar -C CSVReader.classes/ .
   - Run the JAR file using Hadoop: $ hadoop jar CSVReader.jar CSVReader /path/of/csv/file /path/of/output/txt/file
2. Run KMeans
   - Compile file Java code: $ javac -classpath  $HADOOP_COMMON_JAR:$HADOOP_MAPREDUCE_JAR:HADOOP_CLI_JAR -d KMeans.classes KMeans.java
   - Create JAR file: $ jar -cvf KMeans.jar -C KMeans.classes/ .
   - Run the JAR file using Hadoop: $ hadoop jar KMeans.jar KMeans /path/of/output/txt/file /path/of/output/file number_of_k number_of_iteration
3. Review the result
   - Review the centroids: $ hadoop fs -cat /path/of/output/file/task_2_1_cluster/centroids.txt
   - Review all point which in clusters: $ hadoop fs -cat /path/of/output/file/task_2_1_classes/cluster_0(or 1, 2, ..., k-1).txt or $ hadoop fs -cat /path/of/output/file/final_output/part-r-00000
