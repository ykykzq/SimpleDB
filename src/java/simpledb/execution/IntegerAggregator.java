package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int groupByIndex;
    private Type groupByType;
    private int afield;
    private Op aggOp;
    private Map<Field, List<Field>> group;
    private TupleDesc tupleDesc;


    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupByIndex = gbfield;
        this.groupByType = gbfieldtype;
        this.afield = afield;
        this.aggOp = what;
        group  = new HashMap<>();
        if(gbfield != -1){
            Type[] types = new Type[2];
            types[0] = gbfieldtype;
            types[1] = Type.INT_TYPE;
            tupleDesc = new TupleDesc(types);
        }else{
            Type[] types = new Type[1];
            types[0] = Type.INT_TYPE;
            tupleDesc = new TupleDesc(types);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        final Field aggField = tup.getField(afield);
        Field groupField = null;
        if(groupByIndex != -1){
            groupField = tup.getField(groupByIndex);
        }
        if(group.containsKey(groupField)){
            group.get(groupField).add(aggField);
        }else{
            List<Field> list = new ArrayList<>();
            list.add(aggField);
            group.put(groupField,list);
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        switch (aggOp){
            case AVG:
                for (Field field : group.keySet()) {
                    int sum = 0;
                    final Tuple tuple = new Tuple(tupleDesc);
                    tuple.setField(0,field);
                    for(int i=0;i<group.get(field).size();i++){
                        IntField intField = (IntField)group.get(field).get(i);
                        sum += intField.getValue();
                    }
                    // 若field为null，则
                    if(field != null){
                        tuple.setField(1,new IntField(sum/group.get(field).size()));
                    }else{
                        tuple.setField(0, new IntField(sum / group.get(field).size()));
                    }
                    tuples.add(tuple);
                }
                break;
            case MAX:
                for (Field field : group.keySet()) {
                    int max = Integer.MIN_VALUE;
                    final Tuple tuple = new Tuple(tupleDesc);
                    tuple.setField(0,field);
                    for(int i=0;i<group.get(field).size();i++){
                        IntField intField = (IntField)group.get(field).get(i);
                        max = Math.max(max,intField.getValue());
                    }
                    if(field != null){
                        tuple.setField(1,new IntField(max));
                    }else{
                        tuple.setField(0, new IntField(max));
                    }
                    tuples.add(tuple);
                }
                break;
            case COUNT:
                for (Field field : group.keySet()) {
                    Tuple tuple = new Tuple(tupleDesc);
                    tuple.setField(0,field);
                    if(field != null){
                        tuple.setField(1,new IntField(group.get(field).size()));
                    }else{
                        tuple.setField(0, new IntField(group.get(field).size()));
                    }
                    tuples.add(tuple);
                }
                break;
            case SUM:
                for (Field field : group.keySet()) {
                    int sum = 0;
                    final Tuple tuple = new Tuple(tupleDesc);
                    tuple.setField(0,field);
                    for(int i=0;i<group.get(field).size();i++){
                        IntField intField = (IntField)group.get(field).get(i);
                        sum += intField.getValue();
                    }
                    // 若field为null，则
                    if(field != null){
                        tuple.setField(1,new IntField(sum));
                    }else{
                        tuple.setField(0, new IntField(sum));
                    }
                    tuples.add(tuple);
                }
                break;
            case MIN:
                for (Field field : group.keySet()) {
                    int min = Integer.MAX_VALUE;
                    final Tuple tuple = new Tuple(tupleDesc);
                    tuple.setField(0,field);
                    for(int i=0;i<group.get(field).size();i++){
                        IntField intField = (IntField)group.get(field).get(i);
                        min = Math.min(min,intField.getValue());
                    }
                    if(field != null){
                        tuple.setField(1,new IntField(min));
                    }else{
                        tuple.setField(0, new IntField(min));
                    }
                    tuples.add(tuple);
                }
                break;
        }
        return new TupleIterator(tupleDesc,tuples);
    }

}
