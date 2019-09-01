package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */

public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    private int gbFieldNum;
    private Type gbFieldType;
    private int aggFieldNum;
    private Op aggOp;
    private OpIterator opIterator;
    private String groupFieldName;
    private HashMap<Field, Integer> numGroupBy;
    private HashMap<Field, Integer> valGroupBy;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        gbFieldNum = gbfield;
        gbFieldType = gbfieldtype;
        aggFieldNum = afield;
        aggOp = what;
        numGroupBy = new HashMap<>();
        valGroupBy = new HashMap<>();
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
        Field groupField;
        if(gbFieldNum == NO_GROUPING)
        {
            if(gbFieldType == Type.INT_TYPE) groupField = new IntField(0);
            else
                groupField = new StringField("", 100);
        } else
        {
            groupField = tup.getField(gbFieldNum);
        }
        groupFieldName = tup.getTupleDesc().getFieldName(gbFieldNum);
        IntField aggField = (IntField) tup.getField(aggFieldNum);
        if(!numGroupBy.containsKey(groupField)){
            numGroupBy.put(groupField, 1);
            valGroupBy.put(groupField, aggField.getValue());
        }else
        {
            numGroupBy.put(groupField, numGroupBy.get(groupField)+1);
            Integer value = valGroupBy.get(groupField);
            Integer aggVal = aggField.getValue();
            switch (aggOp)
            {
                case MAX:
                    valGroupBy.put(groupField, Math.max(value, aggVal));
                    break;
                case AVG:
                    valGroupBy.put(groupField, (value+aggVal));
                    break;
                case MIN:
                    valGroupBy.put(groupField, Math.min(value, aggVal));
                    break;
                case SUM:
                    valGroupBy.put(groupField, value+aggVal);
                    break;
                case COUNT:
                    valGroupBy.put(groupField, numGroupBy.get(groupField));
                    break;
                default:
                    break;
            }
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
        if(gbFieldNum!=NO_GROUPING)
        {
            TupleDesc tupleDesc = new TupleDesc(
                    new Type[] {gbFieldType, Type.INT_TYPE},
                    new String[] {groupFieldName, aggOp.toString()});
            ArrayList<Tuple> tuples = new ArrayList<>();
            for(Field group : numGroupBy.keySet())
            {
                Tuple tuple = new Tuple(tupleDesc);
                tuple.setField(0, group);
                if(aggOp==Op.AVG)
                    tuple.setField(1, new IntField(valGroupBy.get(group)/numGroupBy.get(group)));
                else
                    tuple.setField(1, new IntField(valGroupBy.get(group)));
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
                tuple.setField(0, new IntField(valGroupBy.get(group)));
                tuples.add(tuple);
            }
            return new TupleIterator(tupleDesc, tuples);
        }
    }
}
