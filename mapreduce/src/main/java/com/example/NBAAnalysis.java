import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class NBAAnalysis {

    // Create Logger instance
    private static final Logger logger = Logger.getLogger(NBAAnalysis.class);

    public static class GameMapper extends Mapper<Object, Text, Text, Text> {
        private static final Map<String, LinkedHashSet<String>> gameTeamsMap = new HashMap<>();
        

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = (value.toString() + " ").split(",");
            if (fields.length != 27) {
                return; // Skip invalid records
            }

            String gameID = fields[2].trim();
            String period = fields[5].trim();
            String score = fields[24].trim();
            String player1_team = fields[11].trim();
            String HomeDescription = fields[3].trim();
            String VisitorDescription = fields[26].trim();

            if(score.isEmpty() || period.isEmpty() || gameID.isEmpty()){
                return;
            }

            // Update the game-to-teams map
            LinkedHashSet<String> teams = gameTeamsMap.getOrDefault(gameID, new LinkedHashSet<>());

            if (teams.size() < 2) {
                if (!player1_team.isEmpty() && !teams.contains(player1_team) && (!HomeDescription.isEmpty() && VisitorDescription.isEmpty())) {
                    teams.add(player1_team);
                }
                if (!player1_team.isEmpty() && !teams.contains(player1_team) && (HomeDescription.isEmpty() && !VisitorDescription.isEmpty())) {
                    teams.add(player1_team);
                }
                gameTeamsMap.put(gameID, teams);
            }

            if (teams.size() < 2) {
                return;
            }

            Iterator<String> iter = teams.iterator();
            String teamA = iter.hasNext() ? iter.next() : "";
            String teamB = iter.hasNext() ? iter.next() : "";

            // Parse and emit scores
            try {
                String[] scores = score.split("-");
                if (scores.length < 2) {
                    return; // Invalid score format
                }
                int scoreA = Integer.parseInt(scores[1].trim());
                int scoreB = Integer.parseInt(scores[0].trim());

                // Emit scores with inferred or stored team names
                context.write(new Text(gameID + "-" + period + "-" + teamA), new Text(String.valueOf(scoreA)));
                context.write(new Text(gameID + "-" + period + "-" + teamB), new Text(String.valueOf(scoreB)));
            } catch (NumberFormatException e) {
                // Log error but don't skip the record
                context.getCounter("GameMapper", "ParseErrors").increment(1);
            }
        }
    }


    // First Reducer: Calculates incremental scores for each period
    public static class GameReducer extends Reducer<Text, Text, Text, Text> {
        private final Map<String, Integer> previousScores = new HashMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keyParts = key.toString().split("-");
            if (keyParts.length < 3) {
                return; // Skip invalid keys
            }

            String gameID = keyParts[0];
            String period = keyParts[1];
            String teamName = keyParts[2];

                        // Calculate cumulative score
            int maxScore = 0;
            for (Text value : values) {
                // /context.write(new Text(gameID + "-" + period + "-" + teamName), new Text(String.valueOf(value)));
                int score = Integer.parseInt(value.toString());
                if (score > maxScore) {
                    maxScore = score;
                }
            }

            // Log the reduced value using Log4j
            logger.info("Reducing for " + teamName + " in game " + gameID + ", period " + period + ": maxScore = " + maxScore);

            // Determine incremental score
            String teamKey = gameID + "-" + teamName;
            int prevScore = previousScores.getOrDefault(teamKey, 0);
            int incrementalScore = maxScore - prevScore;

            // Update previous score for the team
            previousScores.put(teamKey, maxScore);

            // Emit the incremental score
            context.write(new Text(gameID + "-" + period + "-" + teamName), new Text(String.valueOf(incrementalScore)));
        }
    }

    // Second Mapper: Prepares data for final aggregation, emits teamName as key and period:score as value
    public static class FinalMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 2) {
                return; // Skip invalid records
            }

            String[] keyParts = fields[0].split("-"); // "gameID-period-teamName"
            if (keyParts.length < 3) {
                return; // Skip invalid keys
            }

            String teamName = keyParts[2];
            String period = keyParts[1];
            String score = fields[1];

            // Log the values being emitted using Log4j
            logger.info("FinalMapper - teamName: " + teamName + ", period: " + period + ", score: " + score);

            // Emit: (teamName, "period:score")
            context.write(new Text(teamName + "-" + period), new Text(score));
        }
    }

    public static class FinalReducer extends Reducer<Text, Text, Text, Text> {
        
        private Map<String, Map<String, Integer>> teamQuarterScores = new HashMap<>();
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keyParts = key.toString().split("-");
            if (keyParts.length < 2) {
                return; // Skip invalid keys
            }

            String teamName = keyParts[0];
            String period = keyParts[1];

            // Sum up scores for the current key
            int totalScore = 0;
            for (Text value : values) {
                totalScore += Integer.parseInt(value.toString());
            }

            // Store the total score in the teamQuarterScores map
            Map<String, Integer> quarterScores = teamQuarterScores.getOrDefault(teamName, new HashMap<>());
            quarterScores.put(period, quarterScores.getOrDefault(period, 0) + totalScore);
            teamQuarterScores.put(teamName, quarterScores);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Iterate over all teams
            for (Map.Entry<String, Map<String, Integer>> teamEntry : teamQuarterScores.entrySet()) {
                String teamName = teamEntry.getKey();
                Map<String, Integer> quarterScores = teamEntry.getValue();

                // Find the quarter with the maximum score
                String bestQuarter = "";
                int maxScore = Integer.MIN_VALUE;
                
                for (Map.Entry<String, Integer> quarterEntry : quarterScores.entrySet()) {
                    String period = quarterEntry.getKey();
                    int score = quarterEntry.getValue();
                    
                    if (score > maxScore) {
                        maxScore = score;
                        bestQuarter = period;
                    }
                }

                // Emit the result only once per team
                context.write(new Text(teamName), new Text(" scored most of the points in the " + bestQuarter + " quarter"));
            }
        }
    }

    // Main method to configure and run the job
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: NBAAnalysis <input path> <temp output path> <final output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // First Job: Find max score per gameID-period-teamName
        Job job1 = Job.getInstance(conf, "NBA Game Max Score");
        job1.setJarByClass(NBAAnalysis.class);
        job1.setMapperClass(GameMapper.class);
        job1.setReducerClass(GameReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // Second Job: Find best quarter for each team
        Job job2 = Job.getInstance(conf, "NBA Game Best Quarter");
        job2.setJarByClass(NBAAnalysis.class);
        job2.setMapperClass(FinalMapper.class);
        job2.setReducerClass(FinalReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
