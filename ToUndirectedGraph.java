import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class ToUndirectedGraph {

	public static class ToUndirectedGraphMapper extends Mapper<LongWritable, Text, Text, Text>{

		Text outkey=new Text();
		Text outvalue=new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line=value.toString();
			String ids[]=line.split(" ");
			int a=Integer.parseInt(ids[0]);
			int b=Integer.parseInt(ids[1]);
			if(a>b){
				String newkey=b+"";
				outkey.set(newkey);
				String newvalue=a+"";
				outvalue.set(newvalue);
				context.write(outkey, outvalue);
			}
			else{
				String newkey=a+"";
				outkey.set(newkey);
				String newvalue=b+"";
				outvalue.set(newvalue);
				context.write(outkey, outvalue);
			}
		}
	}
	
	public static class ToUndirectedGraphReducer extends Reducer<Text, Text, Text, Text>{

		Text outvalue=new Text();
		
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String s = "";
            TreeSet<Integer> set = new TreeSet<Integer>();
			for (Text val : values) {
				String str = val.toString();
				int i=Integer.parseInt(str);
				set.add(i);
		    }
			Iterator<Integer> it=set.iterator();
		    while(it.hasNext()){
		    	s=s+it.next()+",";
		    }
			outvalue.set(s.substring(0, s.length()-1));
			context.write(key, outvalue);
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
	    Job job1 = new Job(conf, "To Undirected Graph");
	    job1.setJarByClass(ToUndirectedGraph.class);
	    job1.setInputFormatClass(TextInputFormat.class);
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
	    job1.setMapperClass(ToUndirectedGraphMapper.class);
	    job1.setReducerClass(ToUndirectedGraphReducer.class);
	    job1.setOutputFormatClass(TextOutputFormat.class);
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
	    job1.waitForCompletion(true);
	}

}
