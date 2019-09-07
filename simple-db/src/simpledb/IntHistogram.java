package simpledb;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    private int [] height;
    private int buckets;
    private int minVal;
    private int maxVal;
    private int width;
    private int ntups;
    private int lastWidth;

    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        minVal = min;
        maxVal = max;
        height = new int [buckets];
        for(int i=0; i<buckets; ++i)
            height[i] = 0;
        width  = Math.max((maxVal-minVal+1) / buckets, 1);
        ntups = 0;
        lastWidth = (max-min+1) - (buckets-1) * width;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        assert v>=minVal && v <= maxVal : "out of bound";
        ntups ++;
        int k = Math.min((v-minVal) / width, buckets-1) ;
        assert k<buckets : "array out of bound";
        height[k]++;
    }
    private double estimateGT(int bucket, int v, int wid)
    {
        if(v<minVal) return 1.0;
        if(v>=maxVal) return 0;
        double b = 1.0*(width * bucket+minVal - v) * height[bucket] / wid /ntups;
        double sum = 0;
        for(int i=bucket+1; i<buckets; ++i)
            sum += height[i];
        return sum/ntups + b;

    }
    private double estimateEQ(int bucket, int v, int wid)
    {

        if(v<minVal || v>maxVal) return 0;
        return 1.0 * height[bucket] / ntups/wid;
    }
    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here

        int bucket = Math.min((v-minVal) / width,buckets-1) ;
        int wid = bucket < buckets - 1? width : lastWidth;
        assert bucket<buckets : "array out of bound";
        double ans;
        switch (op)
        {
            case GREATER_THAN:
                ans = estimateGT(bucket, v, wid);
                break;
            case EQUALS:
                ans = estimateEQ(bucket, v, wid);
                break;
            case LESS_THAN:
                ans = 1.0 - estimateGT(bucket, v, wid) - estimateEQ(bucket, v, wid);
                break;
            case LESS_THAN_OR_EQ:
                ans = 1.0 - estimateGT(bucket, v, wid);
                break;
            case GREATER_THAN_OR_EQ:
                ans = estimateEQ(bucket, v, wid) + estimateGT(bucket, v, wid);
                break;
            case NOT_EQUALS:
                ans = 1.0 - estimateEQ(bucket, v, wid);
                break;
            default:
                return -1;
        }
        return ans;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return Arrays.toString(height);
    }
}
