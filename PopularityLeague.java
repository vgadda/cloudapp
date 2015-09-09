import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
    	Job job = Job.getInstance(this.getConf(), "Popularity League");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(PopularityLeagueMap.class);
        job.setReducerClass(PopularityLeagueReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(PopularityLeague.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
    public static String readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        StringBuilder everything = new StringBuilder();
        String line;
        while( (line = buffIn.readLine()) != null) {
            everything.append(line);
            everything.append("\n");
        }
        return everything.toString();
    }

    public static class PopularityLeagueMap extends Mapper<Object, Text, IntWritable, IntWritable> {
    	List<String> leaguePageIDs; 
    	@Override
		protected void setup(Context context) throws IOException, InterruptedException {
    		Configuration conf = context.getConfiguration();

            String leaguePath = conf.get("league");
            this.leaguePageIDs = Arrays.asList(readHDFSFile(leaguePath, conf).split("\n"));
		}

		@Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    		String pageID = value.toString().split(":")[0];
        	String pageLinksTo = value.toString().split(":")[1];
            IntWritable one = new IntWritable(1);
            
            if(leaguePageIDs.contains(pageID)){
            	context.write(new IntWritable(Integer.parseInt(pageID)), new IntWritable(0));
            }	
            if(pageLinksTo.trim().length()>0){
            	String[] toPageIDs = pageLinksTo.split(" ");
            	for(String toPageID: toPageIDs){
            		if(leaguePageIDs.contains(toPageID)){
            			context.write(new IntWritable(Integer.parseInt(toPageID)), one);
            		}
            	}
            }
        }
    }

    public static class PopularityLeagueReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    	TreeSet<Pair<Integer, Integer>> countToLinks = new TreeSet<Pair<Integer,Integer>>();
    	@Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int pageLinkCount = 0;
        	for(IntWritable value: values){
            	pageLinkCount += value.get();
            }
        	countToLinks.add(new Pair<Integer, Integer>(pageLinkCount, key.get()));
        }
    	
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			int rank = 0;
			Integer prevPageCount = null;
			for(Pair<Integer, Integer> countToLink: countToLinks){
				if(prevPageCount == null){
					prevPageCount = countToLink.first;
				}else if(prevPageCount.intValue() != countToLink.first){
					rank +=1;
					prevPageCount = countToLink.first;
				}
				context.write(new IntWritable(countToLink.second), new IntWritable(rank));
			}
		}
    	
    	
    }
}

class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
