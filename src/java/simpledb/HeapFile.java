package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File f;
    private TupleDesc td;
    //private ArrayList<HeapPage>pages = new ArrayList<HeapPage>();

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
        //return null;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws NoSuchElementException{
        // some code goes here
        try {
        RandomAccessFile file=new RandomAccessFile(f,"r");
        int pageSize = BufferPool.getPageSize();
        byte [] pageContents = new byte[pageSize];
        file.seek(pageSize*pid.pageNumber());
        file.read(pageContents,0,pageSize);
        file.close();
        return new HeapPage((HeapPageId)pid,pageContents);}
        catch (IOException io){

        };
        return null;

    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        //System.out.println((int)Math.ceil(f.length()/BufferPool.getPageSize()));
        return ((int)Math.ceil(f.length()/BufferPool.getPageSize()));
        //return 0;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        //return null;
        // not necessary for lab1
        ArrayList<Page>pages = new ArrayList<Page>();
        for (int i = 0; i < this.numPages(); i++){
            HeapPageId heapPageId = new HeapPageId(this.getId(),i);
            Page page = Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_WRITE);
            HeapPage heapPage= (HeapPage)page;
            if (heapPage.getNumEmptySlots()==0) continue;
            heapPage.insertTuple(t);
            pages.add(heapPage);
        }
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        //return null;
        // not necessary for lab1
        ArrayList<Page>pages = new ArrayList<Page>();
        HeapPageId heapPageId = (HeapPageId) t.getRecordId().getPageId();
        //int tupleNum = t.getRecordId().tupleno();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_WRITE);
        heapPage.insertTuple(t);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        HeapFileIterator ite = new HeapFileIterator(this,tid);
        return ite;
    }

}

