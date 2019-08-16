import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class Test
{
    public static void main(String[] args) {
        Integer [] a = {1,3,4,5,6};
        ArrayList<Integer> arrayList = new ArrayList<>(5);
        Collections.addAll(arrayList, a);
        Iterator<Integer> it = arrayList.iterator();
        while (it.hasNext())
        {
            System.out.println(it.next());
        }
    }
}
