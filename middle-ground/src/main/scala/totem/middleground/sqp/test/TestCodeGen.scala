/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println

package totem.middleground.sqp.test

import scala.collection.immutable.HashSet
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

abstract class FindString {

  protected val hashSet = HashSet("aaa", "bbb", "ccc")

  def findString(str: String): String
}

object TestCodeGen {

  def main(args: Array[String]): Unit = {
    val tb = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()

    val classDef = tb.parse {
      """
        |import totem.middleground.sqp.test.FindString
        |
        |private class MyFindString extends FindString {
        |    override def findString(str: String): String = {
        |     if (hashSet.contains(str)) "exist"
        |     else "not exist"
        |    }
        |}
        |
        |scala.reflect.classTag[MyFindString].runtimeClass
      """.stripMargin
    }

    val clazz = tb.compile(classDef).apply().asInstanceOf[Class[FindString]]

    val instance = clazz.getConstructor().newInstance()
    println(instance.findString("aaa"))
  }

}
