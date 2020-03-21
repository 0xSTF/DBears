package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.Record;

class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    BNLJOperator(QueryOperator leftSource,
                 QueryOperator rightSource,
                 String leftColumnName,
                 String rightColumnName,
                 TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.BNLJ);

        this.numBuffers = transaction.getWorkMemSize();

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().getStats().getNumPages();
        int numRightPages = getRightSource().getStats().getNumPages();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
                numLeftPages;
    }

    /**
     * BNLJ: Block Nested Loop Join
     * See lecture slides.
     * <p>
     * An implementation of Iterator that provides an iterator interface for this operator.
     * <p>
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     * This means you'll probably want to add more methods than those given.
     */
    private class BNLJIterator extends JoinIterator {
        // Iterator over pages of the left relation
        private BacktrackingIterator<Page> leftIterator;
        // Iterator over pages of the right relation
        private BacktrackingIterator<Page> rightIterator;
        // Iterator over records in the current block of left pages
        private BacktrackingIterator<Record> leftRecordIterator = null;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightRecordIterator = null;
        // The current record on the left page
        private Record leftRecord = null;
        // The next record to return
        private Record nextRecord = null;

        private BNLJIterator() {
            super();

            this.leftIterator = BNLJOperator.this.getPageIterator(this.getLeftTableName());
            fetchNextLeftBlock();

            this.rightIterator = BNLJOperator.this.getPageIterator(this.getRightTableName());
            // this will always be one
            this.rightIterator.markNext();
            fetchNextRightPage();

            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Fetch the next non-empty block of B - 2 pages from the left relation. leftRecordIterator
         * should be set to a record iterator over the next B - 2 pages of the left relation that
         * have a record in them, and leftRecord should be set to the first record in this block.
         * <p>
         * If there are no more pages in the left relation with records, both leftRecordIterator
         * and leftRecord should be set to null.
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement
            if (this.leftIterator.hasNext()) {
                this.leftRecordIterator = getBlockIterator(getLeftTableName(), this.leftIterator, numBuffers - 2);
                // this will mark the first record, to be reset
                this.leftRecord = leftRecordIterator.next();
                this.leftRecordIterator.markPrev();
                // leftRecordIterator already at 1th, leftRecord is still as 0th
            } else {
                this.leftRecordIterator = null;
                this.leftRecord = null;
            }
        }

        /**
         * Fetch the next non-empty page from the right relation. rightRecordIterator
         * should be set to a record iterator over the next page of the right relation that
         * has a record in it.
         * <p>
         * If there are no more pages in the left relation with records, rightRecordIterator
         * should be set to null.
         */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement
            // how to check non-empty page? Handled by right/Iterator

            if (leftRecordIterator == null) {
                this.rightRecordIterator = null;
            } else{
                if (this.rightIterator.hasNext()) {
                    this.rightRecordIterator = BNLJOperator.this.getBlockIterator(getRightTableName(), this.rightIterator, 1);
                    this.rightRecordIterator.markNext();
                } else {
                    this.rightIterator.reset();
                    this.rightIterator.markNext();
                    this.rightRecordIterator = BNLJOperator.this.getBlockIterator(getRightTableName(), this.rightIterator, 1);
                    this.rightRecordIterator.markNext();
                }
            }

        }

        /**
         * Fetches the next record to return, and sets nextRecord to it. If there are no more
         * records to return, a NoSuchElementException should be thrown.
         *
         * @throws NoSuchElementException if there are no more Records to yield
         */
        private void fetchNextRecord() {
            /**

        while ():
            1. LRI & RRI:
                RRI.next()
                compare               //compare first, then advance

            2. LRI & !RRI:
                RRI.reset()
                RRI.markN_next()
                LRI.next()            // the act of compare is executed in case 1

            3. !LRI & RI & (!RRI):     // in this case RRI will always be false, so no need to check.
                                        Instead, check if there's a new right page to bring in
                fetchNextRightPage()  // the page iterator will be reset properly below abstraction
                LRI.reset()
                LRI.mark_next()
                LRI.next()

             4. (!LRI &) !RI & LI      // !LRI has to be true at this point
                fetchNextLeftBlock()  // if no more new blocks, LR&LRI = null, in which case is just case 5
                                      // LRI will be set to the 2nd record immediately


            5. Otherwise aka. (!LRI &) !RI & !LI : Done
                throw new NoSuchElementException;
             */
            this.nextRecord = null;
            while (!hasNext()) {
                if (leftRecord != null  && rightRecordIterator.hasNext()) {

                    Record rightRecord = rightRecordIterator.next();

                    DataBox leftJoinValue = this.leftRecord.getValues().get(getLeftColumnIndex());
                    DataBox rightJoinValue = rightRecord.getValues().get(getRightColumnIndex());

                    if (leftJoinValue.equals(rightJoinValue)) {
                        this.nextRecord = joinRecords(this.leftRecord, rightRecord);
                    }


                } else if (leftRecordIterator.hasNext() && !rightRecordIterator.hasNext()) {
                    rightRecordIterator.reset();
                    //rightRecordIterator.markNext();
                    this.leftRecord = leftRecordIterator.next();
                } else if (!leftRecordIterator.hasNext() && rightIterator.hasNext()) {
                    fetchNextRightPage();
                    leftRecordIterator.reset();
                    leftRecordIterator.markNext();
                    this.leftRecord = leftRecordIterator.next();
                } else if (!rightIterator.hasNext() && leftIterator.hasNext()){
                    fetchNextLeftBlock();
                    fetchNextRightPage();
                } else {
                    throw new NoSuchElementException();
                }
            }
            return;
        }

        /**
         * Helper method to create a joined record from a record of the left relation
         * and a record of the right relation.
         *
         * @param leftRecord  Record from the left relation
         * @param rightRecord Record from the right relation
         * @return joined record
         */
        private Record joinRecords(Record leftRecord, Record rightRecord) {
            List<DataBox> leftValues = new ArrayList<>(leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            return new Record(leftValues);
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
