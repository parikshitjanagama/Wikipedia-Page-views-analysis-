/*
 * Get the Last N articles based on Total Page Views
 */
package wiki.wiki;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class LastNPageViews {

    public static class LastNPageViewsMap extends Mapper<LongWritable, Text, Text, LongWritable> {

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
    
    /**
     * The reducer retrieves every word and puts it into a Map: if the word already exists in the
     * map, increments its value, otherwise sets it to 1.
     */
    public static class LastNPageViewsReduce extends Reducer<Text, LongWritable, Text, LongWritable> {

        private  Map<Text, LongWritable> countMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        	Long views = (long) 0;
			for (LongWritable t : values) {
				views += t.get();
			}
            // puts the number of occurrences of this word into the map.
            // We need to create another Text object because the Text instance
            // we receive is the same for all the words
            countMap.put(new Text(key), new LongWritable(views));

        }

       @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
    
             Map<Text,LongWritable> sortedMap = sortByValues(countMap);
            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 10) {
                    break;
                }
                
                context.write(key,sortedMap.get(key));
            }
        }
    }

    public static class LastNPageViewsCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        	Long views = (long) 0;
			for (LongWritable t : values) {
				views += t.get();
			}
            context.write(key, new LongWritable(views));
        }
    }

    /*
   * sorts the map by values. Taken from:
   * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
   */
    private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o1.getValue().compareTo(o2.getValue());
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
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("Top N");
        job.setJarByClass(LastNPageViews.class);
        job.setMapperClass(LastNPageViewsMap.class);
        job.setCombinerClass(LastNPageViewsCombiner.class);
        job.setReducerClass(LastNPageViewsReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
       
        FileInputFormat.addInputPath(job, new Path("wikicounts"));
        FileOutputFormat.setOutputPath(job, new Path("LastNPageViews"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}