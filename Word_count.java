import streamql.QL;
import streamql.algo.*;
import streamql.query.*;

import java.util.*;

import java.io.BufferedReader;  
import java.io.FileReader;  
import java.io.IOException; 

public class Word_count {
    static class Word_data{
        private String word;
        private int ts; 

        private Word_data(String word, int t){
            this.word = word;
            this.ts = t;
        }
    
        @Override
        public String toString() {
            return "{"+word+","+ts+"}";
        }
    }


    static class SigGen {
        ArrayList<Word_data> source; 
        Iterator<Word_data> stream; 

        public SigGen(){
            source = new ArrayList<Word_data>();
            String line = "";

            try   
            {  
                BufferedReader br = new BufferedReader(new FileReader("./data/1_million_word_UNIX.csv")); 
                br.readLine(); 
                while ((line = br.readLine()) != null)   
                {  
                String[] row = line.split(",");    // use comma as separator  
                source.add(new Word_data(row[1],Integer.parseInt(row[0]))); 
                } 
                br.close(); 
            }   
            catch (IOException e)   
            {  
                e.printStackTrace();  
            }
            
            stream = source.iterator();
            
        }

        public Word_data getWord(){
            if (this.stream.hasNext()){
                return stream.next();
            }
            else{
                return null;
            }  
        }
    }


    public static void main(String[] args) {
        SigGen src = new SigGen();
        // create the sink for detected peaks
        Sink<Word_data> sink = new Sink<Word_data> (){
            @Override
            public void next(Word_data item) {
                //System.out.println("peak detected");
                //System.out.println("Word: " + item.word + ", " + "Count: " + item.ts);
                //System.out.println("hello");
            }
            @Override
            public void end() {
              System.out.println("Job Done");
            }
          };

        Q<Word_data, Word_data> count_word = QL.groupBy(x->x.word, QL.reduce(0,(x,y)->x+1), (word,count)->new Word_data(word,count));
        //Q<Word_data, Word_data> windowed_count = QL.sWindow(10,1,count_word);


        // execution 
        Algo<Word_data, Word_data> algo = count_word.eval();
        algo.connect(sink);
        algo.init();

        Long startTime = System.nanoTime();
        Word_data curr = src.getWord();
        while (curr != null) {
            //System.out.println(vt.toString());
            algo.next(curr);
            curr = src.getWord();
        } ;
        algo.end();

        Long endTime = System.nanoTime();
        System.out.println(endTime-startTime); 

    }
}

