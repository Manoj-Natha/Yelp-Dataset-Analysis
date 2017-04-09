val business=sc.textFile("/yelpdatafall/business/business.csv").map(line=>line.split("\\^"))

val filteredData= business.filter(line=>line(1).contains("Stanford")).map(line=>(line(0).toString,line(1).toString)) 

val review=sc.textFile("/yelpdatafall/review/review.csv").map(line=>line.split("\\^"))
 
val count=review.map(line=>(line(2),1)).reduceByKey((a,b)=>a+b). distinct

val out=filteredData.join(count).distinct.collect() 

out.foreach(line=>(println(line._1,line._2._2)))

