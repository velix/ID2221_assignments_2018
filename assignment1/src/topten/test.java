package topten;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.lang.StringBuffer;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;


public class test {

    public static void main(String[] args) {
        String users_path = "topten/data/users.xml";
        ArrayList<Map<String, String>> xmlRows = readFileContents(users_path);
        
        Iterator xmlRowIt = xmlRows.listIterator();

        while (xmlRowIt.hasNext())
        {
            Map rowMap = (Map)xmlRowIt.next();

            Iterator contentIterator = rowMap.entrySet().iterator();

            while(contentIterator.hasNext()) {
                Map.Entry entry = (Map.Entry)contentIterator.next();
                System.out.print(entry.getKey() + ": " + entry.getValue());
            }
            System.out.println();
        }
    }

    public static ArrayList<Map<String, String>> readFileContents(String filepath)
    {
        ArrayList<Map<String, String>> xmlRows = new ArrayList<>();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(filepath));

            String line;

            while ((line = reader.readLine()) != null) {
                Map xmlRow = transformXmlToMap(line);
                xmlRows.add(xmlRow);
            }

            reader.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        return xmlRows;
    }

    // This helper function parses the stackoverflow into a Map for us.
    // It accepts a single xml row, and returns its content as key, value pairs
    // in the map
    public static Map<String, String> transformXmlToMap(String xml) {
        Map<String, String> map = new HashMap<String, String>();
        try {
            String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
            for (int i = 0; i < tokens.length - 1; i += 2) {
                String key = tokens[i].trim();
                String val = tokens[i + 1];
                map.put(key.substring(0, key.length() - 1), val);
            }
        } catch (StringIndexOutOfBoundsException e) {
            System.err.println(xml);
        }

        return map;
    }



}