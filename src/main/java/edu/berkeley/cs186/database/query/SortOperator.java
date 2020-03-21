package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.DatabaseException;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.memory.Page;

import java.util.*;

public class SortOperator {
    private TransactionContext transaction;
    private String tableName;
    private Comparator<Record> comparator;
    private Schema operatorSchema;
    private int numBuffers;
    private String sortedTableName = null;

    public SortOperator(TransactionContext transaction, String tableName,
                        Comparator<Record> comparator) {
        this.transaction = transaction;
        this.tableName = tableName;
        this.comparator = comparator;
        this.operatorSchema = this.computeSchema();
        this.numBuffers = this.transaction.getWorkMemSize();
    }

    private Schema computeSchema() {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    /**
     * Interface for a run. Also see createRun/createRunFromIterator.
     */
    public interface Run extends Iterable<Record> {
        /**
         * Add a record to the run.
         *
         * @param values set of values of the record to add to run
         */
        void addRecord(List<DataBox> values);

        /**
         * Add a list of records to the run.
         *
         * @param records records to add to the run
         */
        void addRecords(List<Record> records);

        @Override
        Iterator<Record> iterator();

        /**
         * Table name of table backing the run.
         *
         * @return table name
         */
        String tableName();
    }

    /**
     * Returns a NEW run that is the sorted version of the input run.
     * Can do an in memory sort over all the records in this run
     * using one of Java's built-in sorting methods.
     * Note: Don't worry about modifying the original run.
     * Returning a new run would bring one extra page in memory beyond the
     * size of the buffer, but it is done this way for ease.
     */
    public Run sortRun(Run run) {
        // TODO(proj3_part1): implement
        Iterator<Record> iter = run.iterator();
        List<Record> l = new ArrayList<>();
        while (iter.hasNext()) {
            l.add(iter.next());
        }
        l.sort(this.comparator);
        Run result = createRun();
        result.addRecords(l);
        return result;
    }

    /**
     * Given a list of sorted runs, returns a NEW run that is the result
     * of merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run next.
     * It is recommended that your Priority Queue hold Pair<Record, Integer> objects
     * where a Pair (r, i) is the Record r with the smallest value you are
     * sorting on currently unmerged from run i.
     */
    public Run mergeSortedRuns(List<Run> runs) {
        // TODO(proj3_part1): implement

        /**
         * check whether list is too big
         * call iterator on all the the runs (by constructing list of iterators)
         * construct minPQ on all the min records of each run
         */

        if (runs.size() > this.numBuffers - 1) {
            throw new IllegalArgumentException();
        }

        List<Pair<Iterator<Record>, Integer>> l = new ArrayList<>();
        for (Integer i = 0; i < runs.size(); i++) {
            l.add(new Pair(runs.get(i).iterator(), i));
        }

        PriorityQueue<Pair<Record, Integer>> minPQ = new PriorityQueue(new RecordPairComparator());
        for (Pair<Iterator<Record>, Integer> it : l) {
            minPQ.add(new Pair(it.getFirst().next(), it.getSecond()));
        }

        Run result = createRun();

        Record removed_record;
        Integer i;
        Pair<Record, Integer> back_on_queue;

        // int times_removed = 0;    //for testing
        while (!minPQ.isEmpty()) {
            Pair<Record, Integer> curr_min = minPQ.remove();

            removed_record = curr_min.getFirst();
            // times_removed ++;     //for testing
            i = curr_min.getSecond();
            result.addRecord(removed_record.getValues());
            // if (run i) has a next record, then get a new from it
            // else, the minPQ stops fetching from (run i) and minPQ has one less element
            // until minPQ.isEmpty() is tru, breaking out of the while loop
            if (l.get(i).getFirst().hasNext()) {
                back_on_queue = new Pair(l.get(i).getFirst().next(), i);
                minPQ.add(back_on_queue);
            }
        }
        //System.out.println(times_removed); //for testing
        return result;
    }

    /**
     * Given a list of N sorted runs, returns a list of
     * sorted runs that is the result of merging (numBuffers - 1)
     * of the input runs at a time. It is okay for the last sorted run
     * to use less than (numBuffers - 1) input runs if N is not a
     * perfect multiple.
     */
    public List<Run> mergePass(List<Run> runs) {
        // TODO(proj3_part1): implement
        int num_runs = runs.size() / (numBuffers - 1) + 1;
        int num_last_runs = runs.size() % (numBuffers - 1);

        List<List<Run>> inputs = new ArrayList<>();
        for (int i = 0; i < num_runs - 1; i++) {
            inputs.add(runs.subList(i * (numBuffers - 1), (i + 1) * (numBuffers - 1)));
        }
        if (num_last_runs != 0) {
            inputs.add(runs.subList((num_runs - 1) * (numBuffers - 1), runs.size()));
        }

        //int total_runs = 0;
        //for (int j = 0; j < inputs.size(); j++) {
        //    System.out.printf("inputs sublist sizes = %d \n", inputs.get(j).size());
        //    total_runs += inputs.get(j).size();
        //}
        //System.out.printf("total runs processed = %d \n", total_runs);
        //System.out.printf("num runs in runs = %d \n", runs.size());

        List<Run> results = new ArrayList<>();
        for (List<Run> r : inputs) {
            results.add(mergeSortedRuns(r));
        }
        return results;
    }

    /**
     * Does an external merge sort on the table with name tableName
     * using numBuffers.
     * Returns the name of the table that backs the final run.
     */
    public String sort() {
        // TODO(proj3_part1): implement

        // pass 0 prep
        ArrayList<Run> sorted_runs = new ArrayList<>();
        BacktrackingIterator<Page> page_iter = this.transaction.getPageIterator(this.tableName);
        BacktrackingIterator<Record> pass0_iters;

        while (page_iter.hasNext()) {
            pass0_iters = this.transaction.getBlockIterator(this.tableName, page_iter, this.numBuffers);
            sorted_runs.add(this.sortRun(createRunFromIterator(pass0_iters)));
        }

        //Pass 1 and more
        while (sorted_runs.size() > 1) {
            sorted_runs = (ArrayList<Run>) this.mergePass((sorted_runs));
        }
        return sorted_runs.get(0).tableName();
    }

    public Iterator<Record> iterator() {
        if (sortedTableName == null) {
            sortedTableName = sort();
        }
        return this.transaction.getRecordIterator(sortedTableName);
    }

    /**
     * Creates a new run for intermediate steps of sorting. The created
     * run supports adding records.
     *
     * @return a new, empty run
     */
    Run createRun() {
        return new IntermediateRun();
    }

    /**
     * Creates a run given a backtracking iterator of records. Record adding
     * is not supported, but creating this run will not incur any I/Os aside
     * from any I/Os incurred while reading from the given iterator.
     *
     * @param records iterator of records
     * @return run backed by the iterator of records
     */
    Run createRunFromIterator(BacktrackingIterator<Record> records) {
        return new InputDataRun(records);
    }

    private class IntermediateRun implements Run {
        String tempTableName;

        IntermediateRun() {
            this.tempTableName = SortOperator.this.transaction.createTempTable(
                    SortOperator.this.operatorSchema);
        }

        @Override
        public void addRecord(List<DataBox> values) {
            SortOperator.this.transaction.addRecord(this.tempTableName, values);
        }

        @Override
        public void addRecords(List<Record> records) {
            for (Record r : records) {
                this.addRecord(r.getValues());
            }
        }

        @Override
        public Iterator<Record> iterator() {
            return SortOperator.this.transaction.getRecordIterator(this.tempTableName);
        }

        @Override
        public String tableName() {
            return this.tempTableName;
        }
    }

    private static class InputDataRun implements Run {
        BacktrackingIterator<Record> iterator;

        InputDataRun(BacktrackingIterator<Record> iterator) {
            this.iterator = iterator;
            this.iterator.markPrev();
        }

        @Override
        public void addRecord(List<DataBox> values) {
            throw new UnsupportedOperationException("cannot add record to input data run");
        }

        @Override
        public void addRecords(List<Record> records) {
            throw new UnsupportedOperationException("cannot add records to input data run");
        }

        @Override
        public Iterator<Record> iterator() {
            iterator.reset();
            return iterator;
        }

        @Override
        public String tableName() {
            throw new UnsupportedOperationException("cannot get table name of input data run");
        }
    }

    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }
}

