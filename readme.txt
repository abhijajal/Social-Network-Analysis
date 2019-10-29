This is the Readme for running the Big Data Assignment 1 by APJ180001.
1.Start the hadoop by typing start-dfs.sh in the terminal.
2.Visit the webpage at http://localhost:50070.
3.If the webpage is not available, means all the nodes are not started.
4.Type following in terminal : hdfs namenode -format
5.Again type start-dfs.sh in terminal.
-----------------------------------------------------------------------------------------------------
Question 1:
Terminal Commands:   
hdfs dfs -put soc-LiveJournal1Adj.txt /

hadoop jar foaf.jar foaf /soc-LiveJournal1Adj.txt output
hdfs dfs -get output

hadoop jar foaf1.jar foaf /soc-LiveJournal1Adj.txt output1	
hdfs dfs -get output1

foaf.jar is for printing all of them, output is attached as Q1dataset.
foaf1.jar is for only selected pairs, output is attached as q1.txt

(1, 29826) is not present since, they don't have any mutual friends in common.
-----------------------------------------------------------------------------------------------------
Question 2:
Terminal Commands:  
hdfs dfs -put Q1dataset / 
hadoop jar foaf2.jar foaf /Q1dataset output2
hdfs dfs -get output2

foaf2.jar prints Top 10 userIdPairs along with the number of mutual friends and list of mutual friend Ids, output is attached as q2.txt
-----------------------------------------------------------------------------------------------------
Question 3:
Terminal Commands: 
hdfs dfs -put Q1dataset / 
hdfs dfs -put userdata.txt / 
hadoop jar foaf3.jar foaf 0 27 /userdata.txt /Q1dataset output3
hdfs dfs -get output3

0,27	[Jose:New York,Martin:Virginia,Paula:Pennsylvania]

foaf3.jar prints userIdPairs along with their mutual friends firstname and state for specificed userIds, output is attached as q3.txt
-----------------------------------------------------------------------------------------------------
Question 4:
Terminal Commands: 
hdfs dfs -put soc-LiveJournal1Adj.txt /
hdfs dfs -put userdata.txt / 
hadoop jar foaf4.jar foaf /userdata.txt /soc-LiveJournal1Adj.txt output4
hdfs dfs -get output4

foaf4.jar prints Top 15 userIds who has highest average age of mutual friends along with their address and average age of mutual friends, output is attached as q4.txt
-----------------------------------------------------------------------------------------------------



