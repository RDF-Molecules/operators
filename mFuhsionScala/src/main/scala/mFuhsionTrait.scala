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

  def mFuhsion (rtl_1: RTL, rtl_2: RTL, similarity: Array[Array[Double]], threshold: Double, table_1: List[RTL], table_2: List[RTL]) : List[Tuple2[String, String]]

  def stage (rtl: RTL, own_table:List[RTL], dif_table:List[RTL], similarity: Array[Array[Double]], threshold: Double) : List[Tuple2[String, String]]

  def insert(rtl: RTL, table: List[RTL])

  def probe (rtl: RTL, table: List[RTL], similarity: Array[Array[Double]], threshold: Double): List[Tuple2[String, String]]

  def sim (rtl_1: RTL, rtl_2: RTL, similarity: Array[Array[Double]]): Double

}

case class RTL (head: Tuple2[String, Int], tail: List[Tuple2[String, String]])