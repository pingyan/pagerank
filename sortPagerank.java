/**
 * INPUT: output of pagerankCal job
 * OUTPUT: entries with Client IP is filtered out, the domains are printed in descending order of their ranks
 * USAGE: to get a globally sorted rank file, specify the number of reducer to be 1
 *        hadoop jar /home/ping/stats/java/dist/Research.jar sortPageRank -D mapred.reduce.tasks=1 /user/ping/pr/2012-11-26-00-6pa
ss /user/ping/pr/2012-11-26-00-6pass-sorted-all
 */

import java.util.regex.*;
import java.util.HashSet;
import java.util.Iterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

//import com.hadoop.mapreduce.LzoTextInputFormat;
//import com.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.InputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.KeyValueOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class sortPageRank extends Configured implements Tool
{

    public static class XMapper
        extends Mapper<LongWritable, Text, DoubleWritable, Text>
    {
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
		StringTokenizer st = new StringTokenizer(value.toString()) ;
		String page = st.nextToken() ;
		st.nextToken() ;
		st.nextToken() ;
		double pagerank = Double.parseDouble(st.nextToken()) ;

		context.write (new DoubleWritable(pagerank), new Text(page)) ;
        }

    }

    private static class DescendingDoubleComparator extends DoubleWritable.Comparator {
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return -super.compare(b1, s1, l1, b2, s2, l2);
		}
		@SuppressWarnings("unchecked")
		public int compare(WritableComparable a, WritableComparable b) {
			return -super.compare(a, b);
		}
	}

    public static class XReducer
	extends Reducer<DoubleWritable, Text, Text, DoubleWritable>
	{
		public void reduce(DoubleWritable key, Iterable<Text> values,
                 Context context) throws IOException, InterruptedException
		{
			Pattern pattern = Pattern.compile("(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(?:25[0-5]|2[0-4][0-9
]|[01]?[0-9][0-9]?)");

			HashSet<String> uniques = new HashSet<String>();
			for (Text val:values) {
				 //filter out the client IP ranks
                        	Matcher matcher = pattern.matcher(val.toString());
				if (! matcher.find()) {
					uniques.add(val.toString());}
			}
			for (String item:uniques){
                        	context.write(new Text(item), key);
			}

		}
	}
    @Override
    public int run(String[] args)
        throws Exception
    {
        if (args.length != 2) {
            System.err.println("Usage: -- hdfs_input hdfs_output");
            System.exit(-1);
        }

        String hdfs_input = args[0];
        String hdfs_output = args[1];

        Configuration conf = getConf();


        Job job = new Job(conf, "sortPageRank");
        //job.setCombinerClass(LongSumReducer.class);
        //job.setInputFormatClass(InputFormat.class);
	//job.setOutputFormat(TextOutputFormat.class);

	job.setJarByClass(sortPageRank.class);
        job.setMapOutputKeyClass(DoubleWritable.class);//matching the output key type; otherwise, it will report error "type misma
tch"
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(DoubleWritable.class);
	job.setOutputValueClass(Text.class);
	job.setMapperClass(sortPageRank.XMapper.class);
	job.setReducerClass(sortPageRank.XReducer.class);
	job.setSortComparatorClass(DescendingDoubleComparator.class);
        FileInputFormat.addInputPath(job, new Path(hdfs_input));
        FileOutputFormat.setOutputPath(job, new Path(hdfs_output));

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
