package org.hammerlab.suffixes.pdc3

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.hammerlab.suffixes.pdc3.PDC3.Name

import scala.collection.mutable

class Registrar extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Joined])
    kryo.register(classOf[Name])
    kryo.register(Class.forName("scala.reflect.ClassTag$$anon$1"))
    kryo.register(classOf[mutable.WrappedArray.ofRef[_]])
    kryo.register(classOf[mutable.WrappedArray.ofLong])
    kryo.register(classOf[mutable.WrappedArray.ofByte])
    kryo.register(classOf[java.lang.Class[_]])
    kryo.register(classOf[Array[String]])
    kryo.register(classOf[Array[Int]])
    kryo.register(classOf[Array[Option[_]]])
    kryo.register(Class.forName("scala.collection.immutable.Map$EmptyMap$"))
  }
}
