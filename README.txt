PageRank of Wikipedia pages.
By Vikas Dayananda, vdayanan@uncc.edu, 800969865

Compiler and Platform
Compiler : Java compiler (javac)
Java version : JDK 1.8.0
Programming : Java
Platform : Hadoop / Cloudera.
--------------------------------------------------------------------------------------------------------------------------------------
Project Files:
1.DriverClass.java
This program executes linkgraph, pagerank and sortlinks in order.

2. LinkGraph.java
This program counts number of pages(n) in the xml and generates link graph for each page with inital PageRank 1/n.
output will be in format: “page <pr>1/n</pr><l>x</l><l>y</l>/.....” where x and y are outlinks. PageRank is enclosed by <pr></pr> and links are enclosed by <l></l>.

3. PageRank.java
This program calculates the PageRank OF each page in 10 iterations.
output will be in format “page <pr>1/n</pr><l>x</l><l>y</l>/.....” where x and y are outlinks. PageRank is enclosed by <pr></pr> and links are enclosed by <l></l>.

4. SotLinks.java
This program sorts the links produced by PageRank.java by descending order of the rank.
The output will be in format: “page rank

---------------------------------------------------------------------------------------------------------
Execution:

Preparing Input and Output.
1. Before you run the program, you must create you must create input and output locations in HDFS. 
Open terminal and follow to steps to create directory in HDFS
i. sudo su hdfs
ii. hadoop fs -mkdir /user/<username>
iii. hadoop fs -chown <username> /user/<username>
iv. exit
v. sudo su <username>
vi. hadoop fs -mkdir /user/<username>/pagerank  /user/<username>/pagerank/input

2. Move wiki files to input directory in HDFS
i. hadoop fs -put <wiki-xml-file-path> /user/<username>/pagerank/input

--------------------------------------------------------------------------------------------------------------------------------------
Execution of DriverClass.java ( Two arguments)
1. Compile the file.
i. mkdir –p build
ii. javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* *.java -d build –Xlint

2. Create jar file
i. jar -cvf pagerank.jar -C build/ .

3. Run the jar file by passing input and output locations as arguments.
i. $ hadoop jar pagerank.jar org.vikas.DriverClass /user/<username>/pagerank/input /user/<username>/pagerank/output

4. See output files
hadoop fs -cat /user/<username>/pagerank/output/Sorted*

--------------------------------------------------------------------------------------------------------------------------------------

Note: All temparory files used for pagerank iterations are deleted.Comparotor is used for Sorting.


By,
Vikas Dayananda
vdayanan@uncc.edu
800969865