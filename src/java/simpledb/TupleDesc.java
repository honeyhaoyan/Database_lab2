package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;

        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }


    private ArrayList<simpledb.TupleDesc.TDItem> arrayList = new ArrayList<simpledb.TupleDesc.TDItem>();


    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return this.arrayList.iterator();
        //return null;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        arrayList = new ArrayList<simpledb.TupleDesc.TDItem>();
        for (int i = 0; i<typeAr.length; i++) this.arrayList.add(new TDItem(typeAr[i],fieldAr[i]));
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        arrayList = new ArrayList<simpledb.TupleDesc.TDItem>();
        for (int i = 0; i<typeAr.length; i++){
            this.arrayList.add(new TDItem(typeAr[i],""));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return arrayList.size();
        //return 0;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if ((!(arrayList.size()<i)) && (!(i<0))){
            /*Iterator<TDItem> ite = this.iterator();
            for (int k = 0; k<i-1; k++) ite.next();
            return ite.next().fieldName;*/
            return arrayList.get(i).fieldName;
        }
        else throw new NoSuchElementException("i is not a valid field reference");
        //return null;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if ((!(arrayList.size()<i)) && (!(i<0))){
            Iterator<TDItem> ite = this.iterator();
            for (int k = 0; k<i; k++){
                ite.next();
            }
            return ite.next().fieldType;
        }
        else throw new NoSuchElementException("i is not a valid field reference");
        //return null;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here

        //0-based or 1-based ???
        //if (name==null) throw new NoSuchElementException("null is not a valid field name");
        /*if (name==null)
        {
            //throw new NoSuchElementException("null is not a valid field name");
            return 0;
        }*/
        Iterator<TDItem> ite = this.iterator();
        int i = 0;
        while (ite.hasNext()){
            String str = ite.next().fieldName;
            //System.out.println(str);
            /*if (str==null) {
                i++;
                //return i;
                continue;
            }*/
            if(str == null){
                if (name==null) return i;
                else{
                    i++;
                    continue;
                }
            }
            else if (str.equals(name)){
                return i;
            }
            i++;
        }
        throw (new NoSuchElementException("No field with a matching name is found"));

    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        //return arrayList.size();
        int size = 0;
        Iterator<TDItem> ite = this.iterator();
        while(ite.hasNext()) size = size + ite.next().fieldType.getLen();
        return size;
        //return 0;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int length_1 = td1.numFields();
        int length_2 = td2.numFields();
        int total_length = length_1 + length_2;

        Type[] merged_type = new Type[total_length];
        for (int i = 0; i<length_1; i++) merged_type[i] = td1.getFieldType(i);
        for (int i = 0; i<length_2; i++) merged_type[length_1+i] = td2.getFieldType(i);

        String[] merged_name = new String[total_length];
        for (int i = 0; i<length_1; i++) merged_name[i] = td1.getFieldName(i);
        for (int i = 0; i<length_2; i++) merged_name[length_1+i] = td2.getFieldName(i);

        TupleDesc mergedTuple = new TupleDesc(merged_type,merged_name);
        return  mergedTuple;
        //return null;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        //o = (TupleDesc)o;
        if(o instanceof TupleDesc){
            int len = this.arrayList.size();
            if (len!=((TupleDesc) o).arrayList.size()) return false;
            for (int i = 0; i<len; i++){
                if (!this.getFieldType(i).equals(((TupleDesc) o).getFieldType(i))) return false;
            }
            return true;}
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String ans = null;
        for (int i = 0; i<this.arrayList.size(); i++){
            ans = ans + this.getFieldType(i).toString() + "(" + this.getFieldName(i) + ")" +", ";
        }
        return ans;
        //return "";
    }
}
