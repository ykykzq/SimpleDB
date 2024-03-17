package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.IntField;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final Field NO_GROUP_FIELD = new StringField("NO_GROUP_FIELD",20);
    /**
     * 需要分组的字段的索引（从0开始
     */
    private int groupByIndex;

    /**
     * 需要分组的字段类型
     */
    private Type groupByType;

    /**
     * 需要聚合的字段的索引（从0开始
     */
    private int aggregateIndex;

    private TupleDesc aggDesc;

    /**
     * 分组计算Map只需要计算count
     */
    private Map<Field, Integer> groupCalMap;

    private Map<Field,Tuple> resultMap;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(what != Op.COUNT){
            throw new IllegalArgumentException("The Op Type != COUNT");
        }

        this.groupByIndex = gbfield;
        this.groupByType = gbfieldtype;
        this.aggregateIndex = afield;

        this.groupCalMap = new ConcurrentHashMap<>();
        this.resultMap = new ConcurrentHashMap<>();

        if (this.groupByIndex >= 0) {
            // 有groupBy
            this.aggDesc = new TupleDesc(new Type[]{this.groupByType,Type.INT_TYPE}, new String[]{"groupVal","aggregateVal"});
        } else {
            // 无groupBy
            this.aggDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupByField = this.groupByIndex == NO_GROUPING ? NO_GROUP_FIELD : tup.getField(this.groupByIndex);
        if(!NO_GROUP_FIELD.equals(groupByField) && groupByField.getType() != groupByType){
            throw new IllegalArgumentException("Except groupType is: "+ this.groupByType + ",But given "+ groupByField.getType());
        }
        if(!(tup.getField(this.aggregateIndex) instanceof StringField)){
            throw new IllegalArgumentException("Except aggType is: 「 StringField 」" + ",But given "+ tup.getField(this.aggregateIndex).getType());
        }

        this.groupCalMap.put(groupByField,this.groupCalMap.getOrDefault(groupByField,0)+1);
        Tuple curCalTuple = new Tuple(aggDesc);
        if (this.groupByIndex >= 0) {
            // 有groupBy
            curCalTuple.setField(0,groupByField);
            curCalTuple.setField(1,new IntField(this.groupCalMap.get(groupByField)));
        } else {
            // 无groupBy
            curCalTuple.setField(0,new IntField(this.groupCalMap.get(groupByField)));
        }
        resultMap.put(groupByField,curCalTuple);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new StringAggTupIterator();
    }
    private class StringAggTupIterator implements OpIterator {
        private boolean open = false;
        private Iterator<Map.Entry<Field, Tuple>> iter;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            iter = resultMap.entrySet().iterator();
            open = true;
        }

        @Override
        public void close() {
            open = false;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (open && iter.hasNext()) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return iter.next().getValue();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.close();
            this.open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return aggDesc;
        }
    }

}
