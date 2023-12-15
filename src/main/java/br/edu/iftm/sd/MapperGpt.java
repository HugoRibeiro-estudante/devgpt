package br.edu.iftm.sd;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperGpt extends Mapper<Object, Text, Text, IntWritable> {
    
    private final static IntWritable numeroUm = new IntWritable(1);
    private final Text palavra = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tk = new StringTokenizer(value.toString());

        while (tk.hasMoreTokens()) {
            String token = tk.nextToken();

            if (token.equals("\"DateOfConversation\":")) {
                // Combine the next three tokens to get the complete date string
                String month = removeSpecialChars(tk.nextToken());
                String day = removeSpecialChars(tk.nextToken());
                String year = removeSpecialChars(tk.nextToken());
                
                String date = month + " " + day + ", " + year;
                System.out.println(date);

                palavra.set(date);
                context.write(palavra, numeroUm);
            }
        }
    }

    private String removeSpecialChars(String input) {
        // Remove special characters like commas
        return input.replaceAll("[^a-zA-Z0-9]", "");
    }
}
