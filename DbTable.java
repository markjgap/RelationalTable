/*
 * Mark Gapasin
 * Implements a relational database. 
 */

import java.io.IOException;
import java.util.*;
import java.io.File;
import java.io.PrintWriter;

public class DbTable {

    private ArrayList<String> columnNames;

    // Actual data objects
    private LinkedList<DataObject> entries;

    private HashMap<String, TreeMap<String, ArrayList<DataObject>>> indexTrees;

    // Types of comparisons that can be performed
    public enum CompareType {
        LessThan, GreaterThan, LessEq, GreaterEq, Equal, NotEqual
    }

    /**
     * Initializes a table
     *
     * @param columnNames ArrayList of all the column names in the table. 
     * Valid column names are alphanumeric strings (i.e. letters, numbers, and spaces are ok, but no symbols)
     * @throws IllegalArgumentException if columnNames is empty or null, or if any String in columnNames is not alphanumeric
     */
    public DbTable(ArrayList<String> columnNames) throws IllegalArgumentException {
        this.columnNames = new ArrayList<String>(columnNames);
        this.entries = new LinkedList<DataObject>();
        this.indexTrees = new HashMap<String, TreeMap<String, ArrayList<DataObject>>>();
    }

    /**
     * Inserts new DataObject into table
     *
     * @param newRow DataObject to be inserted
     * @throws IllegalArgumentException if newRow is missing any column in columnNames,
     * or any column value is not an alphanumeric String
     */
    public void insert(DataObject newRow) throws IllegalArgumentException {
            if(this.indexTrees.isEmpty()){
                entries.add(newRow);
            }
            else {
                entries.add(newRow);
                for(Map.Entry< String, TreeMap<String, ArrayList<DataObject>> > arr : this.indexTrees.entrySet()){
                    String colName = arr.getKey();
                    // System.out.println("key: " + colName);
                    TreeMap<String, ArrayList<DataObject>> indexTree = arr.getValue();
                    // System.out.println("value before: " + indexTree);
                    String key = newRow.get(colName);
                    if(this.indexTrees.containsKey(key)){
                        ArrayList<DataObject> arr1 = this.indexTrees.get(key).get(key);
                        arr1.add(newRow);
                    }
                    else{
                        ArrayList<DataObject> arr1 = new ArrayList<DataObject>();
                        arr1.add(newRow);
                        indexTree.put(key, arr1);
                    }
                    // System.out.println("value after: " + indexTree);
                }
            }
    }

    /**
     * Returns a subset of the table rows where the columns satisfy ALL of the
     * corresponding conditions specified in the values and ctype Arrays.
     * <p/>
     * For example, if  columns = {"lastname", "score"};
     * values = {"Smith", "20"};
     * ctype = {Equal, LessEq};
     * <p/>
     * Then the select call will return all DataObjects in the table where the
     * "lastname" column is equal to "Smith" AND the "score" column is less
     * than or equal to 20.
     *
     * @param columns String array that specifies which columns are to be compared
     * @param values  String array of the value corresponding to each column
     * @param ctype   Type of comparison to be performed (e.g. equality, less than, etc)
     * @return All the DataObjects in the table that satisfy the conditions
     * @throws IllegalArgumentException if the lengths of columns, values and ctype arrays do not match,
     * or if any of the strings in the columns array do not match strings in this.columnNames
     */
    public ArrayList<DataObject> select(String[] columns,
                                        String[] values,
                                        CompareType[] ctype)
            throws IllegalArgumentException {

        if (columns.length != values.length || values.length != ctype.length) {
            System.out.println("Error: Parameters are not the same length!");
            return null;
        }

        // holds the final selected objects to return
        ArrayList<DataObject> selectedObjects = new ArrayList<DataObject>();

        // For each indexed column in columns we can get an ArrayList of DataObjects that satisfy the condition.
        // indexedObjects stores all of these ArrayLists.
        ArrayList<ArrayList<DataObject>> indexedObjects = new ArrayList<ArrayList<DataObject>>();

        // This tracks all i's where columns[i] is NOT indexed
        ArrayList<Integer> nonIndexedIndices = new ArrayList<Integer>();

        boolean found = false;
        ArrayList<DataObject> tmp = new ArrayList<DataObject>();
            for (int i = 0; i < columns.length; i++) { // iterate columns array
                if (this.indexTrees.containsKey(columns[i])) {
                    // There is an index for this column, so use it
                    TreeMap<String, ArrayList<DataObject>> indexTree = this.indexTrees.get(columns[i]);

                    // This ArrayList will store all the objects that satisfy the condition on columns[i]
                    ArrayList<DataObject> satisfied = new ArrayList<DataObject>();

                    if (ctype[i] == CompareType.Equal) {

                        // Use indexTree.get to find all data objects whose column values are equal to value[i]
                        ArrayList<DataObject> arrayTree = indexTree.get(values[i]);
                        for (int j = 0; j < arrayTree.size(); j++) {
                            satisfied.add(arrayTree.get(j));
                        }
                        //System.out.println("satisfied after equal " + satisfied);
                    } else if (ctype[i] == CompareType.LessThan) {
                        // Use indexTree.headMap to find all data objects whose column values are less than value[i]
                        ArrayList<DataObject> arrayTree = indexTree.get(values[i]);
                            // then add them to satisfied if less than
                                for (ArrayList<DataObject> lessThanArr : indexTree.headMap(values[i], false).values()) {
                                    for (int j = 0; j < lessThanArr.size(); j++) {
                                        satisfied.add(lessThanArr.get(j));
                                    }
                                }
                        System.out.println("satisfied after less than : " + satisfied);
                    } else if (ctype[i] == CompareType.LessEq) {
                        // Use indexTree.headMap to find all data objects whose column values are less than or equal to value[i]
                        ArrayList<DataObject> arrayTree = indexTree.get(values[i]);
                        // then add them to satisfied if less than
                        for (ArrayList<DataObject> lessThanEqArr : indexTree.headMap(values[i], true).values()) {
                            for (int j = 0; j < lessThanEqArr.size(); j++) {
                                satisfied.add(lessThanEqArr.get(j));
                            }
                        }
                        System.out.println("satisfied after less than equal : " + satisfied);
                    } else if (ctype[i] == CompareType.GreaterThan) {
                        // Use indexTree.tailMap to find all data objects whose column values are greater than value[i]
                        ArrayList<DataObject> arrayTree = indexTree.get(values[i]);
                        // then add them to satisfied if less than
                        for (ArrayList<DataObject> greaterThanArr : indexTree.tailMap(values[i], false).values()) {
                            for (int j = 0; j < greaterThanArr.size(); j++) {
                                satisfied.add(greaterThanArr.get(j));
                            }
                        }
                        System.out.println("satisfied after greater than : " + satisfied);
                    } else if (ctype[i] == CompareType.GreaterEq) {
                        // Use indexTree.tailMap to find all data objects whose column values are greater than or equal to value[i]
                        ArrayList<DataObject> arrayTree = indexTree.get(values[i]);
                        // then add them to satisfied if less than
                        for (ArrayList<DataObject> greaterThanEqArr : indexTree.tailMap(values[i], true).values()) {
                            for (int j = 0; j < greaterThanEqArr.size(); j++) {
                                satisfied.add(greaterThanEqArr.get(j));
                            }
                        }
                        System.out.println("satisfied after greater than equal :  " + satisfied);
                    } else if (ctype[i] == CompareType.NotEqual) {
                        // Treat this as non-indexed
                        nonIndexedIndices.add(i);
                        continue;
                    }
                    indexedObjects.add(satisfied);
                }
                else {
                    // There is NOT an index for this column, so add it to the list of nonIndexedColumnsNames
                    nonIndexedIndices.add(i);
                }
            }

        ArrayList<DataObject> toCheck;
        if (indexedObjects.size() > 0) {
            // If there were indexed columns then get all data objects that satisfied ALL of the conditions
            selectedObjects = intersect(indexedObjects);
        } else {
            // If there were not ANY indexed columns, then just scan the whole linked list
            toCheck = new ArrayList<DataObject>(this.entries);
            // Now run your old select code on the toCheck ArrayList:
            for (DataObject obj : toCheck) {
                for (Integer ind : nonIndexedIndices) {
                    String column = columns[ind];
                    String value = values[ind];
                    CompareType type = ctype[ind];
                    int result = obj.get(column).compareTo(value);
                    if (type == CompareType.Equal && result == 0) {
                        selectedObjects.add(obj);
                    } else if (type == CompareType.LessEq && result <= 0) {
                        selectedObjects.add(obj);
                    } else if (type == CompareType.LessThan && result < 0) {
                        selectedObjects.add(obj);
                    } else if (type == CompareType.GreaterEq && result >= 0) {
                        selectedObjects.add(obj);
                    } else if (type == CompareType.GreaterThan && result > 0) {
                        selectedObjects.add(obj);
                    } else if (type == CompareType.LessEq && result <= 0) {
                        selectedObjects.add(obj);
                    } else if (type == CompareType.NotEqual && result != 0) {
                        selectedObjects.add(obj);
                    }
                }
            }
        }
        return selectedObjects;
    }

    /**
     * Removes a DataObject from the table
     *
     * @param toDelete the DataObject to be removed
     */
    public void delete(DataObject toDelete) {
        if(this.indexTrees.isEmpty()){
            this.entries.remove(toDelete);
        }
        else {
            this.entries.remove(toDelete);
            for(Map.Entry< String, TreeMap<String, ArrayList<DataObject>> > arr : this.indexTrees.entrySet()){
                String colName = arr.getKey();
                TreeMap<String, ArrayList<DataObject>> indexTree = arr.getValue();
                String key = toDelete.get(colName);
                if(this.indexTrees.containsKey(colName)){
                    this.indexTrees.get(colName).get(key).remove(toDelete);
                    //if after deletion indexTree is empty, delete indexTree
                    if(this.indexTrees.get(colName).get(key).isEmpty()){
                        this.indexTrees.get(colName).remove(key);
                    }
                }
            }

        }

    }

    /**
     * Writes the contents of the table to an output file.
     * The output file has the following format:
     * <p/>
     * First line:
     * All column names, with each name separated by a comma
     * <p/>
     * Second line:
     * Contents of first data object, with each column value separated by a comma
     * <p/>
     * Third line:
     * Contents of second data object, etc.
     * <p/>
     * Example: If the DbTable had the columns "id", "coursename", and "meetingtime",
     * and had 3 entries with values:
     * ("123", "comp285", "0900"),
     * ("021", "math150", "1500"), and
     * ("421", "math151", "1000"),
     * then the output file contents would look like:
     * <p/>
     * id,coursename,meetingtime
     * 123,comp285,0900
     * 021,math150,1500
     * 421,math151,1000
     *
     * @param outputFileName Name of the file to write the contents to. 
     * If the file already exists then it will be overwritten.
     * @throws java.io.IOException if the file cannot be written to
     */
    public void writeToFile(String outputFileName) throws java.io.IOException {
        String fileName = outputFileName + ".txt";
        PrintWriter writer = new PrintWriter((fileName));
        for (int x = 0; x < columnNames.size(); x++) {
            if (x != columnNames.size() - 1) {
                writer.print(this.columnNames.get(x) + ",");
            } else writer.println(this.columnNames.get(x));
        }
        for (int i = 0; i < entries.size(); i++) {
            for (int j = 0; j < columnNames.size(); j++) {
                if (j != columnNames.size() - 1) {
                    writer.print(this.entries.get(i).get(this.columnNames.get(j)) + ",");
                } else writer.println(this.entries.get(i).get(this.columnNames.get(j)));
            }
        }
        writer.close();
    }

    /**
     * Reads a saved table file (usually one written by the writeToFile method) and re-constructs the DbTable instance.
     * See writeToFile for description of file format.
     *
     * @param inputFileName Name of the file to read
     * @return DbTable instance
     * @throws java.io.FileNotFoundException if the specified file doesn't exist.
     * @throws IllegalArgumentException if the specified file exists but is malformed.
     */
    public static DbTable constructFromFile(String inputFileName)
            throws IllegalArgumentException, java.io.FileNotFoundException {

        File file = new File(inputFileName);
        Scanner fileScanner = new Scanner(file);
        ArrayList<String> inputColumns = new ArrayList<String>();


        // create columnNames variable for DbTable class
        String s = fileScanner.nextLine();
        String[] token = s.split(",");
        for (int i = 0; i < token.length; i++) {
            inputColumns.add(token[i]);
        }

        // initial DbTable object
        DbTable inputTable = new DbTable(inputColumns);

        // create entries variable for DbTable class
        while (fileScanner.hasNextLine()) {
            s = fileScanner.nextLine();
            String[] token_data = s.split(",");
            DataObject data = new DataObject(token, token_data);
            inputTable.insert(data);
        }

        return inputTable;
    }

    /**
     * Creates an index for the specified column in the indexTrees data structure.
     * If the specified column is already indexed then it does nothing.
     *
     * @param columnName The name of the column in this.columnNames to index
     * @throws IllegalArgumentException if columnName is not a string in this.columnNames
     */
    public void createIndex(String columnName) throws IllegalArgumentException {
        // Make sure that columnName is contained in this.columnNames
        if (!this.columnNames.contains(columnName)) {
            throw new IllegalArgumentException("Cannot create an index on a non-existent column");
        }

        // Check to see if there is already an index for columnName
        if (this.indexTrees.get(columnName) != null) {
            return;
        }

        // Create a new index tree for this column
        TreeMap<String, ArrayList<DataObject>> newIndexTree = new TreeMap<String, ArrayList<DataObject>>();

        // Index every DataObject in this.entries
        for (DataObject obj : this.entries) {
            String columnValue = obj.get(columnName);
            if (newIndexTree.containsKey(columnValue)) {
                ArrayList<DataObject> items = newIndexTree.get(columnValue);
                items.add(obj);
            } else {
                ArrayList<DataObject> items = new ArrayList<DataObject>();
                items.add(obj);
                newIndexTree.put(columnValue, items);
            }
        }
        this.indexTrees.put(columnName, newIndexTree);
    }

    /**
     * Takes set of ArrayLists of DataObjects, and returns all DataObjects that belong to all of the sets
     *
     * ex1: If arraySet = {{1,2,3}, {2,3}}, intersect returns {2,3}
     * ex2: If arraySet = {{1,2}, {2,3}, {2, 4}}, intersect returns {2}
     *
     * @param arraySet Sets to intersect
     * @return All data objects that belong to each set
     */
    public static ArrayList<DataObject> intersect(ArrayList<ArrayList<DataObject>> arraySet) {
        HashMap<DataObject, Integer> frequency = new HashMap<DataObject, Integer>();
        for (ArrayList<DataObject> objects : arraySet) {
            for (DataObject object : objects) {

                if (frequency.containsKey(object)) {
                    frequency.put(object, frequency.get(object)+1);
                } else {
                    frequency.put(object, 1);
                }
            }
        }

        ArrayList<DataObject> rtn = new ArrayList<DataObject>();

        for (Map.Entry<DataObject, Integer> entry : frequency.entrySet()) {
            DataObject obj = entry.getKey();
            int count = entry.getValue();

            if (count == arraySet.size()) {
                rtn.add(obj);
            }
        }
        return rtn;
    }
}
