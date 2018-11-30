*******************************************************CS226-Assignment-2******************************************************************8

KNN folder contains the pom.xml,src and target folder where target got created by running maven package.

Following command is used to run the file:

hadoop jar KNN-1.0-SNAPSHOT.jar edu.ucr.cs.cs226.spith001.KNN points.txt 5 10 3 pointsout.txt

it takes 5 parameters:

1.input file name
2.X data point value
3.Y data point value
4.output file

In the KNN query, the input is a set of points (𝑃) in the Euclidean space, a query point (𝑞), and an
integer (𝑘). The output is the 𝑘 points in 𝑃 that are closest to the query point 𝑞.it is implemented by calculating euclidean distance between the query point and the data points.
