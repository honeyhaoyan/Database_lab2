package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    private TupleDesc td;
    private RecordId rid;
    private Field[] fieldArray;
    //private ArrayList<Field>FieldList = new ArrayList<Field>();
    //private Map<Integer,Field> map = new HashMap<Integer, Field>();

    //private Map<Integer,Field>map;

    public Tuple(TupleDesc td) {
        // some code goes here
        this.td = td;
        this.fieldArray = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
        //return null;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.rid;
        //return null;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        //this.FieldList.add(f);
        //this.map.put(i,f);
        this.fieldArray[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        //return this.map.get(i);
        //return null;
        return this.fieldArray[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        //return ("aaaaaa!!!!!");
        //throw new UnsupportedOperationException("Implement this");
        String str = "";
        for (int i = 0;i<this.fieldArray.length;i++){
            if(this.fieldArray[i]!=null) str = str+this.fieldArray[i].toString()+"\t";
            else str = str+"null"+"\t";
        }
        str=str.substring(0, str.length() - 1) + "\n";
        return str;

    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        //return this.FieldList.iterator();
        //return null;
        return Arrays.asList(fieldArray).iterator();
    }

    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        this.td = td;
        fieldArray = new Field[td.numFields()];
    }
}
