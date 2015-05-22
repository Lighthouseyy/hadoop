

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Step1 {

	/**
	 * @param args
	 */
	public static class Step1Mapper extends Mapper<LongWritable, Text, Text, Text>{
		//static IntWritable one = new IntWritable(1);
		Text newKey = new Text();
		Text one = new Text("1");
		protected void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
			String line = value.toString();
			String nodes[] = line.split(" ");
			String strKey = "";//, strValue = "";
			String a = nodes[0];
			String b = nodes[1];
			// a > b
			if (a.length() > b.length() || (a.length() == b.length() && a.compareTo(b) > 0)){
				strKey = b + "," + a;
				//newKey.set(strKey);
			}
			else if (a.length() < b.length() || (a.length() == b.length() && a.compareTo(b) < 0)){
				strKey = a + "," + b;
			}
			if (!strKey.isEmpty()){
				newKey.set(strKey);
				context.write(newKey, one);
			}
		}
	}
	public static class Step1Reducer extends Reducer<Text, Text, Text, Text> {
		static HashSet<String> linkNodes = new HashSet<String>();
		static Text newKey = new Text();
		static String strKey = "";
		static Text newValue = new Text();
		protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException{
				int count = 0;
				for (Text i:values){
					count++;
				}
				//newValue.set("" + count);
				//context.write(key, newValue);
				if (count >= 2){
					String nodes[] = key.toString().split(",");
					if (strKey.isEmpty()){
						strKey = nodes[0];
						linkNodes.add(nodes[1]);
					}
					else if (strKey.equals(nodes[0])){
						linkNodes.add(nodes[1]);
					}
					else{
						if (!linkNodes.isEmpty()){
							newKey.set(strKey);
							String strValue = "";
							Iterator<String> it = linkNodes.iterator();
							while (it.hasNext())	{
								strValue = strValue + it.next() + ",";
							}
							newValue.set(strValue.substring(0, strValue.length() - 1));
							context.write(newKey, newValue);
						}
						strKey = nodes[0];
						linkNodes.clear();
						linkNodes.add(nodes[1]);
					}
				}
			
		}
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException{
			if (!linkNodes.isEmpty()){
			newKey.set(strKey);
			String strValue = "";
			Iterator<String> it = linkNodes.iterator();
			while (it.hasNext())	{
				strValue += it.next() + ",";
			}
			newValue.set(strValue.substring(0, strValue.length() - 1));
			context.write(newKey, newValue);
			}
		}
	}
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		Job job1 = new Job(conf, "job1");
		job1.setJarByClass(Step1.class);
		job1.setNumReduceTasks(8);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		job1.setOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setMapperClass(Step1Mapper.class);
		//job1.setMapOutputKeyClass(Text.class);
		//job1.setMapOutputValueClass(IntWritable.class);
		job1.setReducerClass(Step1Reducer.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
	}

}
