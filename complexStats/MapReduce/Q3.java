import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class Q3 {
	public static class MultipleMap1 extends Mapper<LongWritable,Text,Text,Text>
	{
	public MultipleMap1() {
		
		}
	Text keyEmit = new Text();
	Text valEmit = new Text();
		public void map(LongWritable k, Text value, Context context) throws IOException, InterruptedException
		{

			String[] words= StringUtils.split(value.toString(),"^");;
			if (words.length ==3) {

			keyEmit.set(words[0]);
			valEmit.set(words[1]);
			
			context.write(keyEmit, valEmit);
			}
			}
	}
	
	public static  class MultipleMap2 extends Mapper<LongWritable,Text,Text,Text>
	{
		public MultipleMap2() {
			
		}
	   
		Text keyEmit = new Text();
		Text valEmit = new Text();
		public void map(LongWritable k, Text v, Context context) throws IOException, InterruptedException
		{

			String[] words= StringUtils.split(v.toString(),"^");
			
			if (words.length ==4) {
			keyEmit.set(words[2]);
			valEmit.set(words[1]+"^"+words[3]);
			}
			context.write(keyEmit, valEmit);
			
		}
		
	}
	
	public static class MultipleReducer extends Reducer<Text,Text,Text,Text>
	{
		Text valEmit = new Text();
		private Map<String, String> UserMap = new HashMap<>();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String merge = "";
			String address = "";
			String uid = "";
			String stars = "";
			for (Text value : values) {

				String[] words = StringUtils.split(value.toString(), "^");
				if (words.length == 1) {
					address = words[0];
				} else {
					UserMap.put(words[0], words[1]);
					
					//uid = words[0];
					//stars = words[1];

				}

			}

			if (address.contains("Stanford")) {
				for (String keys : UserMap.keySet())
				{
				context.write(new Text(keys), new Text(UserMap.get(keys)));
				}
			}

		}
	}

	 
	public static void main(String[] args) throws Exception
	{
		if (args.length != 3 ){
			System.err.println ("Usage :<inputlocation1> <inputlocation2> <outputlocation> >");
			System.exit(0);
		}
		
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path p1=new Path(files[0]);
		Path p2=new Path(files[1]);
		Path p3=new Path(files[2]);
		FileSystem fs = FileSystem.get(c);
		if(fs.exists(p3)){
			fs.delete(p3, true);
		}
		Job job = Job.getInstance(c,"Multiple Job");
		job.setJarByClass(Q3.class);
		MultipleInputs.addInputPath(job, p1, TextInputFormat.class, MultipleMap1.class);
		MultipleInputs.addInputPath(job,p2, TextInputFormat.class, MultipleMap2.class);
		job.setReducerClass(MultipleReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, p3);
		boolean success = job.waitForCompletion(true);
		 System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	
}
	}