/*
 * Mark Gapasin
 * Data Object that will be stored in a linked list in the DbTable
 *
 */

/**
 * Class for storing an individual row in a database
 */

import java.util.HashMap;

public class DataObject {

    private HashMap<String, String> data;

    /**
     * Initializes a new Data Object with a set of column names and values
     *
     * @param columns   A string array of the column names (must be alphanumeric)
     * @param values    A string array of the values corresponding to each column (must be alphanumeric)
     * @throws          IllegalArgumentException if the lengths of columns and values don't match, or either is null.
     */
    public DataObject(String[] columns, String[] values) throws IllegalArgumentException {

        if (columns.length != values.length || columns == null || values == null) {
            throw new IllegalArgumentException();
        }

        data = new HashMap<String, String>();

        for (int i=0; i < columns.length; i++) {
            data.put(columns[i], values[i]);
        }
    }

    /**
     * Set or alter the value associated with a given column
     *
     * @param column Name of column
     * @param value New value to store
     */
    public void set(String column, String value) {
        data.put(column, value);
    }

    public String[] get(String[] columns) {
        if (columns == null) {
            return null;
        }

        String[] valueArray = new String[columns.length];
        for (int i=0; i < columns.length; i++) {
            valueArray[i] = this.data.get(columns[i]);
        }
        return valueArray;
    }

    public String get(String column) {
        if (column == null) {
            return null;
        }

        String[] strArray = new String[1];
        strArray[0] = column;
        return this.get(strArray)[0];
    }
}
