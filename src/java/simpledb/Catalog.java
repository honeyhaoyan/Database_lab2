package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public static class Table implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The DbFile of the table
         * */
        public DbFile file;

        /**
         * The name of the table
         * */
        public String name;

        /**
         * The name of the primary key field of the table
         * */
        public String pkeyField;

        public Table(DbFile file, String name, String pkeyField) {
            this.file = file;
            this.name = name;
            this.pkeyField = pkeyField;
        }
    }

    private ArrayList<Table>arrayList = new ArrayList<Table>();
    private ArrayList<Integer>tableIdList = new ArrayList<Integer>();

    public ArrayList<Table> TableList(){
        return arrayList;
    }

    public Catalog() {
        // some code goes here
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        /*
        arrayList.add(new Table(file,name,pkeyField));
        tableIdList.add(file.getId());
        */
        Iterator<Table>ite = this.arrayList.iterator();
        while(ite.hasNext()){
            Table table = ite.next();
            if (table.file.getId()==file.getId()||table.name.equals(name)){
                int id = file.getId();
                ite.remove();
                Iterator<Integer>ite2 = tableIdList.iterator();
                while(ite2.hasNext()){
                    int number = ite2.next();
                    if (number==id) ite2.remove();
                }
            }
        }
        arrayList.add(new Table(file,name,pkeyField));
        tableIdList.add(file.getId());
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        Iterator<Table> ite = this.arrayList.iterator();
        while (ite.hasNext()){
            Table table = ite.next();
            if (table.name.equals(name)){
                return table.file.getId();
            }
        }
        throw new NoSuchElementException("The table with a specified name doesn't exist");
        //return 0;
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        Iterator<Table> ite = this.arrayList.iterator();
        while (ite.hasNext()){
            Table table = ite.next();
            if (table.file.getId()==tableid){
                return table.file.getTupleDesc();
            }
        }
        throw new NoSuchElementException("The TupleDesc with a specified id doesn't exist");
        //return null;
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        Iterator<Table> ite = this.arrayList.iterator();
        while (ite.hasNext()){
            Table table = ite.next();
            if (table.file.getId()==tableid){
                return table.file;
            }
        }
        throw new NoSuchElementException("The DbFile with a specified id doesn't exist");
        //return null;
    }

    public DbFile getDbFile(int tableid) throws NoSuchElementException {
        // some code goes here
        Iterator<Table> ite = this.arrayList.iterator();
        while (ite.hasNext()){
            Table table = ite.next();
            if (table.file.getId()==tableid){
                return table.file;
            }
        }
        throw new NoSuchElementException("The DbFile with a specified id doesn't exist");
        //return null;
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        Iterator<Table> ite = this.arrayList.iterator();
        while (ite.hasNext()){
            Table table = ite.next();
            if (table.file.getId()==tableid){
                return table.pkeyField;
            }
        }
        //throw new NoSuchElementException("The DbFile with a specified id doesn't exist");
        //return null;
        return null;
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return this.tableIdList.iterator();
        //return null;
    }

    public String getTableName(int id) {
        // some code goes here
        Iterator<Table> ite = this.arrayList.iterator();
        while (ite.hasNext()){
            Table table = ite.next();
            if (table.file.getId()==id){
                return table.name;
            }
        }
        throw new NoSuchElementException("The table name with a specified id doesn't exist");
        //return null;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        this.arrayList.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

