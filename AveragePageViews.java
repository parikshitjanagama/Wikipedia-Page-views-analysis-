/*
*Calculate the average page views for each article 
*/
package wiki.wiki;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AveragePageViews {

	public static class AveragePageViewsMap extends Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String delims = ",";
			String[] wikiData = StringUtils.split(value.toString(), delims);
			Text article = new Text(wikiData[0]);
			LongWritable views = new LongWritable(Long.parseLong(wikiData[2]));
			context.write(article, views);
		}

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
		}
	}

	public static class AveragePageViewsReduce extends Reducer<Text, LongWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			Long views = (long) 0;
			Long count = (long) 0;
			for (LongWritable t : values) {
				views += t.get();
				count++;
			}
			Double average = (double) views/ (double)count; 

			context.write(key, new DoubleWritable(average));

		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "CountWiki");
		job.setJarByClass(AveragePageViews.class);
		job.setMapperClass(AveragePageViewsMap.class);
		job.setReducerClass(AveragePageViewsReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path("/Users/anushakaranam/Downloads/wiki/wikicounts"));
		FileOutputFormat.setOutputPath(job, new Path("AveragePageViews"));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
