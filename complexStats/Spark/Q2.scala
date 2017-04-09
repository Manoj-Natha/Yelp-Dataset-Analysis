val user=sc.textFile("/yelpdatafall/user/user.csv").map(lines=>lines.split("\\^"))

val review=sc.textFile("/yelpdatafall/review/review.csv").map(lines=>lines.split("\\^"))

val sum=review.map(line=>(line(1),line(3).toDouble)).reduceByKey((a,b)=>a+b)

val count=review.map(line=>(line(1),1)).reduceByKey((a,b)=>a+b)

val rsjoin=sum.join(count) 

val reviewOut=rsjoin.map(a=>(a._1,a._2._1/a._2._2)) 

val input=Console.readLine()

val query=user.filter(line=>line(1).contains(input)).map(line=>(line(0).toString,line(1).toString))

val userData=user.map(line=>(line(0).toString,line(1).toString)) 

val out=reviewOut.join(userData) 

val finalOutput=out.join(query).distinct.collect()

finalOutput.foreach(line=>println(line._1,line._2._1._2, line._2._1._1)) 

