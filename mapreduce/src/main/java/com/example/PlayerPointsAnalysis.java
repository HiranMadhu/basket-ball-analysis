import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PlayerPointsAnalysis {

    public static class PointsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final Pattern POINTS_PATTERN = Pattern.compile("(\\d+) PTS");
        private String extractPlayerName(String description, int pointsIndex) {
            if (pointsIndex <= 0) {
                return null; 
            }
            String subStr = description.substring(0, pointsIndex).trim();
            String[] words = subStr.split(" ");
            int i = 0;
            while (i < words.length && words[i].equals(words[i].toUpperCase())) {
                i++; 
            }
            return i < words.length ? words[i] : null;
        }
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = (value.toString() + " ").split(",");
            if (fields.length != 27) {
                return; 
            }
            String gameID = fields[2].trim(); 
            String description = fields[3].trim() + " " + fields[26].trim(); 
            String playerName = null;
            int points = -1;
            Matcher matcher = POINTS_PATTERN.matcher(description);
            if (matcher.find()) {
                points = Integer.parseInt(matcher.group(1).trim()); 
                playerName = extractPlayerName(description, matcher.start());
            }
            if (playerName == null || points == -1) {
                return; 
            }
            String fullName = null;
            for (int index : new int[]{19, 13, 7}) {
                if (fields[index].contains(playerName)) {
                    fullName = fields[index].trim();
                    break;
                }
            }
            if (fullName == null) {
                return; 
            }
            context.write(new Text(gameID + "_" + fullName.replace(" ", "-")), new IntWritable(points));
        }
    }

    // Reducer Class
    public static class PointsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Reducer logic to sum the points for each player and select the highest score
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int maxPoint = 0;

            for (IntWritable val : values) {
                maxPoint = Math.max(maxPoint, val.get()); 
            }

            context.write(key,  new IntWritable(maxPoint));
        }
    }

    public static class PointsSumMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t"); 

            if (fields.length != 2) {
                return; 
            }

            String compositeKey = fields[0]; 
            int points = Integer.parseInt(fields[1].trim()); 

            String[] keyParts = compositeKey.split("_");
            if (keyParts.length < 2) {
                return; 
            }

            String playerName = keyParts[1]; 
            context.write(new Text(playerName), new IntWritable(points));
        }
    }



public static class PointsSumReducer extends Reducer<Text, IntWritable, Text, Text> {
    private int maxPoints = 0;
    private String topPlayer = "";

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalPoints = 0;

        for (IntWritable val : values) {
            totalPoints += val.get();
        }

        if (totalPoints > maxPoints) {
            maxPoints = totalPoints;
            topPlayer = key.toString();
        }

        String playerName = key.toString().toString().replace("-", " ");
        context.write(new Text(playerName), new Text("scored " + totalPoints + " points in the full tournament"));

    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String playerName = topPlayer.toString().replace("-", " ");
        context.write(new Text(playerName), new Text("scored " + maxPoints + " points in the full tournament"));
    }
}


    // Main method for setting up and running the Hadoop job
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: NBAAnalysis <input path> <temp output path> <final output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();

        // First Job: Find max score per gameID-period-teamName
        Job job1 = Job.getInstance(conf, "Player Points Annalysis");
        job1.setJarByClass(PlayerPointsAnalysis.class);
        job1.setMapperClass(PointsMapper.class);
        job1.setReducerClass(PointsReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }
        // Second Job: Sum points across games for each player
        Job job2 = Job.getInstance(conf, "Player Points Sum");
        job2.setJarByClass(PlayerPointsAnalysis.class);
        job2.setMapperClass(PointsSumMapper.class);
        job2.setReducerClass(PointsSumReducer.class);
        job2.setOutputKeyClass(Text.class); // Player name
        job2.setOutputValueClass(IntWritable.class); // Total points
        FileInputFormat.addInputPath(job2, new Path(args[1])); // Output of Job 1
        FileOutputFormat.setOutputPath(job2, new Path(args[2])); // Final output path
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
