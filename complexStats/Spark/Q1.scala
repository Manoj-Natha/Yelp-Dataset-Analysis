val business=sc.textFile("/yelpdatafall/business/business.csv").map(lines=>lines.split("\\^")) 

val Split=business.map(line=>(line(0),line(1).toString+line(2).toString)) 

val review=sc.textFile("/yelpdatafall/review/review.csv").map(lines=>lines.split("\\^"))

 val sum=review.map(line=>(line(2),line(3).toDouble)).reduceByKey((a,b)=>a+b).distinct 

val count=review.map(line=>(line(2),1)).reduceByKey((a,b)=>a+b).distinct 

val rsJoin=sum.join(count) 

val reviewOut=rsJoin.map(a=>(a._1,a._2._1/a._2._2)) 

val out = Split.join(reviewOut).distinct.collect() 

val sOut=out.sortWith(_._2._2>_._2._2).take(10)

sOut.foreach(line=>println(line._1,line._2._1,line._2._2))

