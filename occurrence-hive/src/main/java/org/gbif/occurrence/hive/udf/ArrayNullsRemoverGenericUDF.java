package org.gbif.occurrence.hive.udf;

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

/**
 * A simple UDF that remove nulls from a list.
 */
public class ArrayNullsRemoverGenericUDF extends GenericUDF {

  private StandardListObjectInspector retValInspector;
  private PrimitiveObjectInspector primitiveObjectInspector;

  @Override
  public Object evaluate(DeferredObject[] arg0) throws HiveException {
    List list = retValInspector.getList(arg0[0].get());
    List result = Lists.newArrayList();
    if (list != null && !list.isEmpty()) {
      if(primitiveObjectInspector != null && primitiveObjectInspector.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
        result = handlerStringArray(list);
      } else {
        result = Lists.newArrayList(list);
        result.removeAll(Collections.singleton(null));
      }
    }
    return result.isEmpty() ? null : result;
  }

  /**
   * Removes empty Strings and null values.
   */
  private List handlerStringArray(List list) {
    List result = Lists.newArrayList();
    for (Object oElement : list) {
      Object stdObject = ObjectInspectorUtils.copyToStandardJavaObject(oElement, primitiveObjectInspector);
      if(stdObject != null && ((String)stdObject).trim().length() != 0) {
        result.add(stdObject);
      }
    }
    return result;
  }

  @Override
  public String getDisplayString(String[] arg0) {
    return "removeNulls( " + arg0[0] + ")";
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arg0)
    throws UDFArgumentException {
    if (arg0.length != 1) {
      throw new UDFArgumentException("removeNulls takes an array as argument");
    }
    if (arg0[0].getCategory() != Category.LIST) {
      throw new UDFArgumentException("removeNulls takes an array as argument");
    }
    retValInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(arg0[0]);
    if (retValInspector.getListElementObjectInspector().getCategory() != Category.PRIMITIVE) {
      primitiveObjectInspector = (PrimitiveObjectInspector)retValInspector.getListElementObjectInspector();
    }
    return retValInspector;
  }
}
