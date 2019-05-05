package simpledb;

import java.io.*;
import java.util.*;

public class HeapFileIterator implements DbFileIterator {
    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */

    private HeapFile heapFile;
    private Iterator<Tuple> ite;
    private TransactionId transId;
    private int PageNum;

    public HeapFileIterator(HeapFile heapFile, TransactionId transId){
        this.heapFile = heapFile;
        this.transId = transId;
    }


    public void open() throws NoSuchElementException,TransactionAbortedException, DbException{
        this.ite = TupleListPerPage(0);
        this.PageNum = 0;
    }
    /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
    public boolean hasNext() throws NoSuchElementException,TransactionAbortedException, DbException{
        if (this.ite==null) return false;
        if (this.ite.hasNext()) return true;
        else{
            if (PageNum==this.heapFile.numPages()-1) return false;
            return (TupleListPerPage(PageNum+1).hasNext());
        }
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next() throws NoSuchElementException,TransactionAbortedException, DbException {
        if (!hasNext()) throw new NoSuchElementException();
        if (this.ite.hasNext()) return this.ite.next();
        this.ite = TupleListPerPage(++PageNum);
        return this.ite.next();
    }

    public Iterator<Tuple> TupleListPerPage(int pageNum) throws TransactionAbortedException, DbException{
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();

        PageId pageId = new HeapPageId(this.heapFile.getId(),pageNum);

        //System.out.println("getPage");
        Page page = Database.getBufferPool().getPage(this.transId,pageId,Permissions.READ_ONLY);

        Iterator<Tuple> ite = ((HeapPage)page).iterator();
        while (ite.hasNext()){
            tuples.add(ite.next());
        }
        return tuples.iterator();
    }


    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws NoSuchElementException,TransactionAbortedException, DbException{
        close();open();
    }


    /**
     * Closes the iterator.
     */
    public void close(){
        this.ite = null;
    }
}
