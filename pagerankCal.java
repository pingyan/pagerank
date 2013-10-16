/**
 *  2012 ping
 *  INPUT:
 *  OUTPUT:

 *  USAGE: ./runName2IP hdfs_input hdfs_output
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

public class pageRankCal extends Configured implements Tool
{

    public static class XMapper
        extends Mapper<LongWritable, Text, Text, Text>
    {

        @Override
	/*swap two entities, BEFORE: each row has its own pr and size attached; AFTER: each row's pr and size is the link's size a
nd pr*/
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
		StringTokenizer st = new StringTokenizer(value.toString());
                String self = st.nextToken();
		String size = st.nextToken();
		String self2 = st.nextToken();
		String pr1 = st.nextToken();
		String pr0 = st.nextToken();
		String link  = st.nextToken();
		context.write(new Text(link), new Text(size+"\t"+link+"\t"+pr1+"\t"+pr0+"\t"+self));
        }

    }

        /*aggregate all pr/size for each key, and print them out. The OUPUT becomes the same row format as MAP JOB's INPUT*/
	public static class XReducer
	        extends Reducer<Text, Text, Text, Text>
	{
	        @Override
		public void reduce(Text key, Iterable<Text> values,
	         Context context) throws IOException, InterruptedException
		{

			List<String> links = new ArrayList<String>(); //List<Array>, growable
			double pr1 = 0;
        		//boolean first = true;
			//HashSet<String> uniques = new HashSet<String>();
        		for (Text val: values)
			{	StringTokenizer st = new StringTokenizer(val.toString());
				int size = Integer.parseInt(st.nextToken()); // size of link
				if (size < 1) {size = 1; }
				st.nextToken();
				double pr0 = Double.parseDouble(st.nextToken());
				pr1 += pr0/size;
				st.nextToken();
				String link = st.nextToken();
				links.add(link.toString());

			}
			String size = Integer.toString(links.size()); // size of self

			for (String temp:links){
				context.write(key, new Text(size +"\t"+ key + "\t"+ pr1 + "\t"+ pr1+ "\t"+ temp));// ip/dn, size,
ip/dn, pr1, pr0, dn/ip
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


        Job job = new Job(conf, "pageRankCal");
        //job.setCombinerClass(LongSumReducer.class);
        //job.setInputFormatClass(LzoTextInputFormat.class);
        job.setJarByClass(pageRankCal.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(pageRankCal.XMapper.class);
        job.setReducerClass(pageRankCal.XReducer.class);
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
