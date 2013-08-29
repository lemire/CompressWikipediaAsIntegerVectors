package me.lemire.wikipediacompress;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import me.lemire.integercompression.*;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.benchmark.byTask.feeds.DocMaker;
import org.apache.lucene.benchmark.byTask.feeds.EnwikiContentSource;
import org.apache.lucene.benchmark.byTask.utils.Config;
import org.apache.lucene.document.Document;
import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.*;

/**
 * A simple utility to benchmark various integer compression
 * tools using a representation of Wikipedia as a vector.
 * 
 * You may need to generate a dictionary file using me.lemire.lucene.CreateFreqSortedDictionary.
 * 
 * (Sorry if documentation isn't better, this is a small personal project.)
 * 
 * @author Daniel Lemire
 * 
 */
public class Benchmark {

        public static double bitsperint(IntArray ia, IntegerCODEC c, IntArray ib) {
                ib.ensureCapacity(ia.size() + 1024);
                if(ia.size() == 0) return 0;
                IntWrapper iin = new IntWrapper(0);
                IntWrapper iout = new IntWrapper(0);
                c.compress(ia.array, iin, ia.size(), ib.array, iout);
                if(iin.intValue() != ia.size()) throw new RuntimeException("Your codec failed to consumme all of the data.");
                return iout.intValue() * 4 * 8.0 / ia.size();
        }

        
        public static double entropy(IntArray ia) {
                if(ia.size() == 0) return 0;
                HashMap<Integer,Double> hm = new HashMap<Integer,Double>();
                for(int i = 0; i < ia.size(); ++i) {
                        if(hm.containsKey(ia.get(i)))
                                hm.put(ia.get(i), hm.get(ia.get(i))+1);
                        else
                                hm.put(ia.get(i),1.0);
                }
                double ent = 0;
                
                for (double d : hm.values()) {
                        double p = d / ia.size();
                        ent += - p * Math.log(p)/Math.log(2);
                }
                return ent;
        }
        
        public static void main(String[] args) throws Exception {
                DecimalFormat df = new DecimalFormat("0.00");

                if (args.length <= 1) {
                        printUsage();
                        return;
                }
                File wikipediafile = new File(args[0]);
                if (!wikipediafile.exists()) {
                        System.out.println("Can't find "
                                + wikipediafile.getAbsolutePath());
                        return;
                }
                if (!wikipediafile.canRead()) {
                        System.out.println("Can't read "
                                + wikipediafile.getAbsolutePath());
                        return;
                }
                File dictfile = new File(args[1]);
                if (!dictfile.exists()) {
                        System.out.println("Can't find "
                                + dictfile.getAbsolutePath());
                        return;
                }
                if (!dictfile.canRead()) {
                        System.out.println("Can't read "
                                + dictfile.getAbsolutePath());
                        return;
                }

                // we should be "ok" now

                int MaxN = 1000*1000; //dictionary is limited to 1000000 words
                BufferedReader br = new BufferedReader(new FileReader(dictfile));
                
                System.out.println("#Loading first "+MaxN+" words from dictionary");
                String line;
                HashMap<String,Integer> hm = new HashMap<String,Integer>();
                int code = 0;
                while((line = br.readLine())!= null) {
                        String[] words = line.split("\t");
                        if(words.length!=2) throw new RuntimeException("Format of dictionary should be freq<tab>term"); 
                        hm.put(words[1],code++);
                        if(code > MaxN) break;
                }
                br.close();
                System.out.println("#Loaded "+hm.size()+" words from dictionary.");

                StandardAnalyzer analyzer = new StandardAnalyzer(
                        Version.LUCENE_43);// default
                                           // stop
                                           // words
                DocMaker docMaker = new DocMaker();
                Properties properties = new Properties();
                properties.setProperty("content.source.forever", "false"); 
                properties.setProperty("docs.file",
                        wikipediafile.getAbsolutePath());
                properties.setProperty("keep.image.only.docs", "false");
                Config c = new Config(properties);
                EnwikiContentSource source = new EnwikiContentSource();
                source.setConfig(c);
                source.resetInputs();// though this does not seem needed, it is
                                     // (gets the file opened?)
                docMaker.setConfig(c, source);
                int count = 0;
                System.out.println("#Parsing of Wikipedia dump "
                        + wikipediafile.getAbsolutePath());
                long start = System.currentTimeMillis();
                Document doc;
                IntArray ia = new IntArray();
                IntArray buffer = new IntArray();

                IntegerCODEC bp  = new Composition(new BinaryPacking(), new VariableByte());
                IntegerCODEC fastpfor  = new Composition(new FastPFOR(), new VariableByte());
                IntegerCODEC optpfor  = new Composition(new OptPFDS9(), new VariableByte());
                IntegerCODEC simple9  = new Simple9();
                IntegerCODEC jcopy  = new JustCopy();
                IntegerCODEC vbyte = new VariableByte();
                    System.out.println("# entrop bitsperint(binarypacking) bitsperint(fastpfor) ...");
                try {
                        while ((doc = docMaker.makeDocument()) != null) {
                                ia.clear();
                                if(doc.getField("body") == null) continue;
                                TokenStream stream = doc.getField("body")
                                        .tokenStream(analyzer);
                                CharTermAttribute cattr = stream
                                        .addAttribute(CharTermAttribute.class);

                                stream.reset();
                                while (stream.incrementToken()) {
                                        String token = cattr.toString();
                                        if (hm.containsKey(token)) {
                                            ia.add(hm.get(token));
                                        }

                                }
                                
                                stream.end();
                                stream.close();
                                double ent = Benchmark.entropy(ia);
                                double bpbpi = Benchmark.bitsperint(ia, bp, buffer);
                                double fastpforbpi = Benchmark.bitsperint(ia, fastpfor, buffer);  
                                double optpforbpi = Benchmark.bitsperint(ia, optpfor, buffer);  
                                double simple9bpi = Benchmark.bitsperint(ia, simple9, buffer);  
                                double vbytebpi = Benchmark.bitsperint(ia, vbyte, buffer);  
                                 
                                System.out.println(ia.size()+"\t"+df.format(ent)+"\t"+df.format(bpbpi)+"\t"+df.format(fastpforbpi)+"\t"+df.format(optpforbpi)+"\t"+df.format(simple9bpi) +"\t"+df.format(vbytebpi));
                                ++count;
                        }
                } catch (org.apache.lucene.benchmark.byTask.feeds.NoMoreDataException nmd) {
                        nmd.printStackTrace();
                }
                docMaker.close();

        }

        private static void printUsage() {
                System.out
                        .println("Usage: java -cp <...> me.lemire.wikipediacompress.Benchmark somewikipediadump.xml.gz dictfile");
        }
}


