package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     * See lecture slides.
     * <p>
     * Before proceeding, you should read and understand SNLJOperator.java
     * You can find it in the same directory as this file.
     * <p>
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     * This means you'll probably want to add more methods than those given (Once again,
     * SNLJOperator.java might be a useful reference).
     */
    private class SortMergeIterator extends JoinIterator {
        /**
         * Some member variables are provided for guidance, but there are many possible solutions.
         * You should implement the solution that's best for you, using any member variables you need.
         * You're free to use these member variables, but you're not obligated to.
         */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;     // boolean to indicate whether your rightIterator is marked at all
                                    // exist instances when their ain't no mark at all


        private SortMergeIterator() {
            // TODO(proj3_part1): implement
            /*
             * construct two SortOperator object from this.
             */
            super();
            SortOperator left = new SortOperator(SortMergeOperator.this.getTransaction(), this.getLeftTableName(), new LeftRecordComparator());
            SortOperator right = new SortOperator(SortMergeOperator.this.getTransaction(), this.getRightTableName(), new RightRecordComparator());
            String sorted_left_tableName = left.sort();
            String sorted_right_tableName = right.sort();;

            this.leftIterator = SortMergeOperator.this.getRecordIterator(sorted_left_tableName);
            this.rightIterator = SortMergeOperator.this.getRecordIterator(sorted_right_tableName);

            this.nextRecord = null;
            this.marked = false;

            this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;

            // We mark the first record so we can reset to it when we advance the left record.
            if (rightRecord != null) {
                rightIterator.markPrev();
                this.marked = true;
            } else {
                return;
            }

            // always keep in mind that the first call to fetchNewRecord() is here
            // and the first call to rightIterator.markPrev() is up there 👆
            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * Pre-fetches what will be the next record, and puts it in this.nextRecord.
         * Pre-fetching simplifies the logic of this.hasNext() and this.next()
         * set this.nextRecord to null if no more to fetch
         */
        private void fetchNextRecord() {
            if (this.leftRecord == null) { throw new NoSuchElementException("No new record to fetch"); }
            this.nextRecord = null;    // this guarantees that we always enter while loop

            while (!this.hasNext()) {
                if (!this.marked) {

                    while (rightRecord != null && compareRecords(leftRecord, rightRecord) < 0) {
                        nextLeftRecord(); // exception is thrown when reaches the end of rightSource
                    }
                    while (rightRecord != null && compareRecords(leftRecord, rightRecord) > 0) {
                        nextRightRecord(); // this.rightRecord becomes null when reaches the end of rightSource
                    }
                    marked = true;
                }
                if (rightRecord != null && compareRecords(leftRecord, rightRecord) == 0) {
                    mergeRecords();
                    nextRightRecord();
                } else {
                    resetRightRecord();
                    nextLeftRecord();
                    marked = false;
                }
            }

        }

        private void resetRightRecord() {
            if (this.marked) {
                this.rightIterator.reset();
                assert(rightIterator.hasNext());
                rightRecord = rightIterator.next();
            }
        }

        private void nextLeftRecord() {
            // when exception thrown, this.fetchNextRecord (the caller) hands control to its caller.
            if (!leftIterator.hasNext()) { throw new NoSuchElementException("All Done!"); }
            leftRecord = leftIterator.next();
        }

        private void nextRightRecord() {
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
        }

        private void mergeRecords() {
            List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
            List<DataBox> rightValues = new ArrayList<>(rightRecord.getValues());
            leftValues.addAll(rightValues);
            this.nextRecord = new Record(leftValues);
        }

        /**
         * left < right -> -        ;       o1 from leftSource
         * left = right -> 0        ;       o2 from rightSource
         * left > right -> +        ;
         */
        private int compareRecords(Record o1, Record o2) {
            return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                    o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            // TODO(proj3_part1): implement
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
            // TODO(proj3_part1): implement

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

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                        o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                        o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
