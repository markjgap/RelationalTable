import java.io.FileNotFoundException;
import java.util.Random;
import java.util.ArrayList;
import java.util.Arrays;

public class Main {

    public static void main(String[] args) throws FileNotFoundException{
        String cols[] = {"lastname", "firstname", "id"};

        DbTable table = new DbTable(new ArrayList<String>(Arrays.asList(cols)));

        int n=1000;
        for (int i=0; i<n; i++) {
            String vals[] = {"Gap", "Mark", "12345"};
            vals[2] = String.valueOf(i);
            DataObject obj = new DataObject(cols, vals);
            table.insert(obj);
        }

        // Randomly choose k indexes to search for
        int k=1000;
        int search[] = new int[k];
        Random rng = new Random();

        for (int i=0; i < k; i++) {
            search[i] = rng.nextInt(n);
        }

        String columns[] = {"id"};
        String values[] = new String[1];
        DbTable.CompareType ctype[] = {DbTable.CompareType.Equal};

        double averageNonIndexedSearchTime = System.nanoTime();
        for (int i=0; i<k; i++) {
            values[0] = String.valueOf(search[i]);
            ArrayList<DataObject> found = table.select(columns, values, ctype);
            if (found.size() != 1) {
                System.out.println("Error in select method. Did not find object with unique id: " + values[i]);
                return;
            }
        }

        averageNonIndexedSearchTime = System.nanoTime()-averageNonIndexedSearchTime;
        averageNonIndexedSearchTime /= 1e9;

        System.out.println("Average non-indexed search time (n= " + String.valueOf(n) + "): " + averageNonIndexedSearchTime + " sec");



        double timeToCreateIndex = System.nanoTime();
        table.createIndex("id");
        timeToCreateIndex = System.nanoTime() - timeToCreateIndex;
        timeToCreateIndex /= 1e9;

        System.out.println("Time to create index (n= " + String.valueOf(n) + "): " + timeToCreateIndex + " sec");


        double averageIndexedSearchTime = System.nanoTime();

        for (int i=0; i<k; i++) {
            values[0] = String.valueOf(search[i]);
            ArrayList<DataObject> found = table.select(columns, values, ctype);
            if (found.size() != 1) {
                System.out.println("Error in select method. Did not find object with unique id: " + values[i]);
                return;
            }
        }
    }
}
