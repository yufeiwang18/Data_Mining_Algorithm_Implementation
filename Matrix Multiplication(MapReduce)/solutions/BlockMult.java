import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class BlockMult {
    public static int b=3;
    public static int n=2;


    public static class MapperA
            extends Mapper<Object, Text, Text, Text> {

        private Text cont = new Text();
        private Text key1 = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String Afile=value.toString();

            String[] Alist = Afile.split("\n");

            String maped = "";
            for (int i = 0; i < Alist.length; i++) {
                int len = Alist[i].length();
                String s = Alist[i];
                for (int j = 1; j < b + 1; j++) {
                    key1=new Text(s.substring(0, 3) + j + ")");
                    cont=new Text("(A," + s.charAt(3) + s.substring(5, len) + ")" );
                    context.write(key1,cont);
                }
            }
        }
    }


    public static class MapperB
            extends Mapper<Object, Text, Text, Text> {

        private Text cont = new Text();
        private Text key1 = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String Bfile=value.toString();

            String[] Blist = Bfile.split("\n");

            String maped = "";
            for (int i = 0; i < Blist.length; i++) {
                int len = Blist[i].length();
                String s = Blist[i];
                for (int j = 1; j < b + 1; j++) {

                    key1=new Text("(" + j + s.substring(2, 4) + ")");
                    cont=new Text("(B," + s.charAt(1) + s.substring(5, len) + ")");
                    context.write(key1,cont);

                }
            }

        }
    }



    public static class reducer1
            extends Reducer<Text, Text, Text, Text> {
        private Text cont = new Text();
        private Text key1 = new Text();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            HashMap<String, Integer> multiply = new HashMap<String, Integer>();

            for (int p = 1; p < n + 1; p++) {
                for (int q = 1; q < n + 1; q++) {
                    String kk = Integer.toString(p) + "," + Integer.toString(q);
                    multiply.put(kk, 0);
                }
            }
            Iterator<String> mulresult = multiply.keySet().iterator();
            String test="";

            for (Text val : values) {
                test=test+val.toString()+"\n";
            }

            String[] te=test.split("\n");
            for (int i = 0; i < te.length; i++) {
                if (te[i].contains("A")) {
                    String pairkey = te[i].substring(3, 4);

                    String pairvalueA = te[i].substring(7, te[i].length() - 2);
                    for (int j = 0; j < te.length; j++) {
                        if (te[j].contains("B") & te[j].substring(3, 4).equals(pairkey)) {
                            String pairvalueB = te[j].substring(7, te[j].length() - 2);
                            String[] lA = pairvalueA.split("\\),\\(");
                            String[] lB = pairvalueB.split("\\),\\(");
                            for (int h = 0; h < lA.length; h++) {
                                lA[h] = lA[h].replace("[", "").replace("]", "").replace(")", "");
                            }
                            for (int h = 0; h < lB.length; h++) {
                                lB[h] = lB[h].replace("[", "").replace("]", "").replace(")", "");
                            }

                            for (int p = 0; p < lA.length; p++) {
                                String findbk = lA[p].substring(2, 3);
                                int avalue = Integer.parseInt(lA[p].substring(4, 5));
                                String finalrow = lA[p].substring(0, 1);
                                for (int q = 0; q < lB.length; q++) {
                                    if (lB[q].substring(0, 1).equals(findbk)) {
                                        String finalcol = lB[q].substring(2, 3);
                                        String mapkey = finalrow + "," + finalcol;
                                        int finalvalue = avalue * Integer.parseInt(lB[q].substring(4, 5));
                                        multiply.put(mapkey, multiply.get(mapkey) + finalvalue);
                                    }
                                }
                            }

                        }
                    }
                }

            }
            System.out.println("key:  "+key);
            String result = "";
            while (mulresult.hasNext()) {
                String ab=mulresult.next();
                if(multiply.get(ab)!=0) {
                    result = result + "[" + ab + "," + Integer.toString(multiply.get(ab)) + "],";
                }
            }

            if(result.contains(",")){
                result="["+result.substring(0,result.length()-1)+"]";
                key1=key;
                cont=new Text(result);
                context.write(key1,cont);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("error");
            System.exit(2);
        }

        Job job = new Job(conf, "average");
        job.setMapperClass(MapperA.class);
        job.setMapperClass(MapperB.class);
        job.setReducerClass(reducer1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setJarByClass(MultipleInputs.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        Path p1=new Path(otherArgs[0]);
        Path p2=new Path(otherArgs[1]);
        MultipleInputs.addInputPath(job, p1, TextInputFormat.class, MapperA.class);
        MultipleInputs.addInputPath(job, p2, TextInputFormat.class, MapperB.class);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
