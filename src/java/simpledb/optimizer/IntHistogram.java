package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int[] buckets;
    private int min;
    private int max;
    private double width;

    /**
     * 元组记录总数
     */
    private int ntups;

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
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.width = Math.max(1,(max - min + 1.0) / buckets);
        this.ntups = 0;

    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if(v >= min && v <= max && getIndex(v) != -1){
            buckets[getIndex(v)]++;
            ntups++;
        }
    }
    public int getIndex(int v){
        int index = (int) ((v - min) / width);
        if(index < 0 || index >= buckets.length){
            return -1;
        }
        return index;
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
        switch(op){
            case GREATER_THAN:
                if(v > max){
                    return 0.0;
                } else if (v <= min){
                    return 1.0;
                } else {
                    int index = getIndex(v);
                    double tuples = 0;
                    for(int i = index + 1; i < buckets.length; i++){
                        tuples += buckets[i];
                    }
                    tuples += (min + (getIndex(v) + 1) * width - 1 - v) *  (1.0 *buckets[index] / width);
                    return tuples / ntups;
                }
            case LESS_THAN:
                return 1 - estimateSelectivity(Predicate.Op.GREATER_THAN_OR_EQ,v);
            case EQUALS:
                return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) -
                        estimateSelectivity(Predicate.Op.LESS_THAN, v);
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS,v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN,v-1);
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN,v+1);
            default:
                throw new UnsupportedOperationException("Op is illegal");
        }
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
        double sum = 0;
        for (int bucket : buckets) {
            sum += (1.0 * bucket / ntups);
        }
        return sum / buckets.length;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return "IntHistogram{" +
                "buckets = " + Arrays.toString(buckets) +
                ", min = " + min +
                ", max =" + max +
                ", width =" + width +
                "}";
    }
}
