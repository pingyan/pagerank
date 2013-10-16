/**
 *  2012
 *  INPUT: output of name2IP job, where each ip/domain mapping is printed,and there is a default pr for each entity
 *  OUTPUT: ip/dn | number of outlinks of ip/dn |  pr1 of self | pr0 of self | dn/ip (link) | querycnt
 *  DESCRIPTION: mapreduce job preparing the input for pagerank calculation
 *  USAGE: hadoop jar /home/ping/stats/java/dist/Research.jar pagerankInputPrepare HDFS_IN HDFS_OUT
 */

import java.util.HashSet;
import java.util.Iterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import com.hadoop.mapreduce.LzoTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class pageRankInputPrepare extends Configured implements Tool
{
    /*map job did only one thing, spit the key seperately to have the reducer to aggregate on the key to get the size*/
    public static class XMapper
        extends Mapper<LongWritable, Text, Text, Text>
    {

        @Override
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
		StringTokenizer st = new StringTokenizer(value.toString());
                String self = st.nextToken();

		context.write(new Text(self), new Text(value));
        }

    }
	/*reduce job here only did one thing: get the number of outlinks of the entity/key, and print it out together with the sam
e set of info as map output*/

	public static class XReducer
	        extends Reducer<Text, Text, Text, Text>
	{
	        @Override
		public void reduce(Text key, Iterable<Text> values,
	         Context context) throws IOException, InterruptedException
		{

			List<String> links = new ArrayList<String>(); //List<Array>, growable

			//HashSet<String> uniques = new HashSet<String>();
        		for (Text val: values)
			{	//StringTokenizer st = new StringTokenizer(val.toString());
				links.add(val.toString());

			}
			String size = Integer.toString(links.size());

			for (String temp:links){
				context.write(key, new Text(size +"\t"+ temp));// ip/dn, size, ip/dn, pr1, pr0, dn/ip
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


        Job job = new Job(conf, "pageRankInputPrepare");
        //job.setCombinerClass(LongSumReducer.class);
        //job.setInputFormatClass(LzoTextInputFormat.class);
        job.setJarByClass(pageRankInputPrepare.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(pageRankInputPrepare.XMapper.class);
        job.setReducerClass(pageRankInputPrepare.XReducer.class);
        FileInputFormat.addInputPath(job, new Path(hdfs_input));
        FileOutputFormat.setOutputPath(job, new Path(hdfs_output));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /*public static void main(String[] args) throws Exception {
        Tool tool = new Example();
        int result = ToolRunner.run(tool, args);
        System.exit(result);
    }*/

}
