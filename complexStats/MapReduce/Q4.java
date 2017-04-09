import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
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

public class Q4 {
	
	public static class MultipleMap1 extends Mapper<LongWritable,Text,Text,Text>
	{
	public MultipleMap1() {
		
		}
	Text keyEmit = new Text();
	Text valEmit = new Text();
	   //User
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

			keyEmit.set(words[1]);
			valEmit.set("");
			}
			context.write(keyEmit, valEmit);
			
		}
		
	}
	
	public static class MultipleReducer extends Reducer<Text,Text,Text,Text>
	{
		Text valEmit = new Text();
		String merge = "";
		private Map<Text, Integer> countMap = new HashMap<>();
		private Map<Text, String> UserMap = new HashMap<>();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int count = 0;

			String user = "";
			for (Text value : values) {

				String words = value.toString();
				if (words.length() > 1) {
					user = words;
					UserMap.put(new Text(key), user);
				} else {
					count++;
				}

			}

   			countMap.put(new Text(key), count);
			

		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, Integer> sortedMap = sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 10) {
                    break;
                }
                context.write(key, new Text(UserMap.get(key).toString()+ " " + countMap.get(key).toString()));
            }
        }
	}
	private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
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
	job.setJarByClass(Q4.class);
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
