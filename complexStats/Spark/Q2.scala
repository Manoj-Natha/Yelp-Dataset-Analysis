Console.print("Enter the Name : ")

var name=scala.io.StdIn.readLine()

val user=sc.textFile("file///C:/spark-2.0.1-bin-hadoop2.7/data/user.csv").map(lines=>lines.split("\\^"))

val namedUsers = user.filter(line => line(1).equalsIgnoreCase(name)).map(line => (line(0),line(1)))  

val reviews=sc.textFile("file///C:/spark-2.0.1-bin-hadoop2.7/data/review.csv").map(lines=>lines.split("\\^")) 

val sumOfStars=reviews.map(line=>(line(1),line(3).toDouble)).reduceByKey((a,b)=>a+b)

val noOfRatings=reviews.map(line=>(line(1),1)).reduceByKey((a,b)=>a+b)

val avgRatingsInfo = sumOfStars.join(noOfRatings).map(a=>(a._1,a._2._1/a._2._2))

val namedRatings = namedUsers.join(avgRatingsInfo).collect.foreach(x => println(x._1, x._2._2 ))
