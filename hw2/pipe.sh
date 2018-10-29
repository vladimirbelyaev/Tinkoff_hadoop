#!/usr/bin/env bash
hdfs dfs -rm -r testing/uniqueURL
hdfs dfs -rm -r testing/resultParts
hdfs dfs -rm -r testing/queryCount
hdfs dfs -rm -r testing/partCount
hdfs dfs -rm -r testing/resultQueries
echo "Extracting unique urls"
hadoop jar hw2.jar UniqueURLJob testing/0* testing/uniqueURL
echo "Counting unique parts"
hadoop jar hw2.jar PartCountJob testing/uniqueURL/part* testing/partCount
echo "Sorting unique parts"
hadoop jar hw2.jar SortJob testing/partCount/part* testing/resultParts
echo "Counting unique queries"
hadoop jar hw2.jar QueryCountJob testing/uniqueURL/part* testing/queryCount
echo "Sorting unique queries"
hadoop jar hw2.jar SortJob testing/queryCount/part* testing/resultQueries
echo "Dumping result for parts"
hdfs dfs -cat testing/resultParts/part* | head -n 10 >> testing/resultParts.txt
echo "Dumping result for queries"
hdfs dfs -cat testing/resultQueries/part* | head -n 10 >> testing/resultQueries.txt
echo "Done"

