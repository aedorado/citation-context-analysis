package mcad;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

public class CitationContextFiltering {

    public static void main(String args[]) throws IOException {

        BufferedReader br = new BufferedReader(new FileReader("/home/iiita/aa/citeseerx/stopWordsList.txt"));
        BufferedReader readCitation = new BufferedReader(new FileReader("/home/iiita/Desktop/citation-context-analysis-master/scraper/citations.txt"));
        FileWriter fw = new FileWriter("/home/iiita/Desktop/citation-context-analysis-master/scraper/filteredCitations.txt");

        String str = "";
        HashSet<String> hs = new HashSet();

        while ((str = br.readLine()) != null) {
            hs.add(str);
            br.readLine();
        }

        String doif = "";
        String doit = "";
        String filteredContext = "";
        String contextLine = "";
        String fileEntryLine = "";
        //while(true) {

        contextLine = readCitation.readLine();
        String[] col = contextLine.split("\\*\\*\\*");
        if (col.length == 2) {
            //write this line
            doif = col[0].trim();
            doit = col[1].trim();
            fileEntryLine = doif + " *** " + doit + " *** ";
          //  System.out.println("" + fileEntryLine);
        } else {
            //write to the file the previous entry and filter the current line and continue (read more lines) 
            doif = col[0].trim();
            doit = col[1].trim();
            String context = col[2].trim();
            String[] subContext = context.split(" ");

            for (String word : subContext) {
                if (!hs.contains(word)) {
                    filteredContext += word.trim() + " ";
                }
            }
            filteredContext.trim();
            fileEntryLine = doif + " *** " + doit + " *** " + filteredContext;

        }
        
      //  int i = 0;
        while ((contextLine = readCitation.readLine()) != null) {
          //  System.out.println("" + contextLine);
            String[] cols = contextLine.split("\\*\\*\\*");
            if (cols.length < 2) {
                //only filtering done and read next line
                String context = cols[0].trim();
                String[] subContext = context.split(" ");
                String filteredContext1 = "";
                for (String word : subContext) {
                    if (!hs.contains(word)) {
                        filteredContext1 += word.trim() + " ";
                    }
                }
                filteredContext1.trim();
                fileEntryLine += " " + filteredContext1;
            } else {
                fileEntryLine += " \n";
                fw.write(fileEntryLine);
                //System.out.println("@here1");
                filteredContext = "";
                fileEntryLine = "";
                if (cols.length == 2) {
                    //write this line
                    doif = cols[0].trim();
                    doit = cols[1].trim();
                    fileEntryLine = doif + " *** " + doit + " *** " + filteredContext;
                //    System.out.println("" + fileEntryLine);
                } else {
                    //write to the file the previous entry and filter the current line and continue (read more lines) 
                    doif = cols[0].trim();
                    doit = cols[1].trim();
                    String context = cols[2].trim();
                    String[] subContext = context.split(" ");

                    for (String word : subContext) {
                        if (!hs.contains(word)) {
                            filteredContext += word.trim() + " ";
                        }
                    }
                    filteredContext.trim();
                    fileEntryLine = doif + " *** " + doit + " *** " + filteredContext;
                }
            }
        }
        
        fileEntryLine += " \n";
        fw.write(fileEntryLine);
      //  System.out.println("@here2");
        fw.close();

    }

}