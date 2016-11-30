/*
 * Copyright (C) 2017 EIS Uni-Bonn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
  * Created by dcollarana on 11/30/2016.
  */
trait mFuhsionTrait {

  def mFuhsion (rtl_1: RTL, rtl_2: RTL, similarity: Array[Array[Float]], threshold: Float, table_1: List[String], table_2: List[String]) : List[Tuple2[String, String]]

  def stage (rtl: RTL, own_table:List[String], dif_table:List[String], similarity: Array[Array[Float]], threshold: Float) : List[Tuple2[String, String]]

  def insert(rtl: RTL, table: List[String])

  def probe (rtl: RTL, table: List[String], similarity: Array[Array[Float]], threshold: Float): List[Tuple2[String, String]]

  def sim (rtl_1: RTL, rtl_2: RTL, similarity: Array[Array[Float]]): Float

}

case class RTL (head: Tuple2[String, Int], tail: List[Tuple2[String, String]])