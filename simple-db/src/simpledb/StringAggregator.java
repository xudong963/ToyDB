package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int gbFieldNum;  // see gbfield
    private Type gbFieldType;
    private int aggFieldNum;
    private Op aggOp;
    private String groupFieldName;
    private HashMap<Field, Integer> numGroupBy;
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        gbFieldNum = gbfield;
        gbFieldType = gbfieldtype;
        aggFieldNum = afield;
        aggOp = what;
        numGroupBy = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField;
        if(gbFieldNum==NO_GROUPING)
        {
            if(gbFieldType==Type.INT_TYPE) groupField = new IntField(0);
            else
                groupField = new StringField("", 100);
        }else
            groupField = tup.getField(gbFieldNum);
        groupFieldName = tup.getTupleDesc().getFieldName(gbFieldNum);
        if(!numGroupBy.containsKey(groupField))
        {
            numGroupBy.put(groupField, 1);
        }else
        {
            numGroupBy.put(groupField, numGroupBy.get(groupField)+1);
            // only need to support count
        }
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
        if(gbFieldNum!=NO_GROUPING)
        {
            TupleDesc tupleDesc = new TupleDesc(
                    new Type[] {gbFieldType, Type.INT_TYPE},
                    new String[] {groupFieldName, aggOp.toString()}
            );
            ArrayList<Tuple> tuples = new ArrayList<>();
            for(Field group: numGroupBy.keySet())
            {
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, group);
                tuple.setField(1, new IntField(numGroupBy.get(group)));
                tuples.add(tuple);
            }
            return new TupleIterator(tupleDesc, tuples);
        }else
        {
            TupleDesc tupleDesc = new TupleDesc(
                    new Type[] {Type.INT_TYPE},
                    new String[] {aggOp.toString()}
            );
            ArrayList<Tuple> tuples = new ArrayList<>();
            for(Field group : numGroupBy.keySet())
            {
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, new IntField(numGroupBy.get(group)));
                tuples.add(tuple);
            }
            return new TupleIterator(tupleDesc, tuples);
        }
    }
}
