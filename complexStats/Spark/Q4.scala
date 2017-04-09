val user=sc.textFile("/yelpdatafall/user/user.csv").map(line=>line.split("\\^"))

val filteredData=user.map(line=>(line(0),line(1).toString))

val review=sc.textFile("/yelpdatafall/review/review.csv").map(line=>line.split("\\^")) 

val count=review.map(line=>(line(1),1)).reduceByKey((a,b)=>a+b).distinct 

val out=filteredData.join(count).distinct.collect() 

val sortedOut=out.sortWith(_._2._2>_._2._2).take(10)

sortedOut.foreach(line=>println(line._1,line._2._1))

