/*
 * Copyright 2014–2016 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ygg.table

import scalaz._, Scalaz._
import ygg._, common._, json._

class ColumnarTable extends ColumnarTableModule {
  type TableCompanion = ColumnarTableCompanion

  def fromSlices(slices: NeedSlices, size: TableSize): Table = new Table(slices, size)
  def toJson(dataset: Table): Need[Stream[JValue]]           = dataset.toJson.map(_.toStream)
  def toJsonSeq(table: Table): Seq[JValue]                   = toJson(table).copoint

  object Table extends ColumnarTableCompanion
  class Table(slices: NeedSlices, size: TableSize) extends ColumnarTable(slices, size) {
    /** XXX FIXME */
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder): NeedTable = ???
    def load(tpe: JType): NeedTable                                       = ???

    override def toString = jvalueStream mkString "\n"
  }
}
object ColumnarTable extends ColumnarTable {
  def empty: Table               = fromSlices(emptyStreamT(), ExactSize(0))
  def apply(json: String): Table = fromJson(JParser.parseManyFromString(json).fold(throw _, x => x))
  def apply(file: jFile): Table  = apply(file.slurpString)
}

abstract class BlockTable extends BlockTableModule {
  def toJson(dataset: Table): Need[Stream[JValue]] = dataset.toJson.map(_.toStream)
  def toJsonSeq(table: Table): Seq[JValue]         = toJson(table).copoint
}
object BlockTable extends BlockTable {
  def projections = Map[Path, Projection]()

  def apply(json: String): Table = fromJson(JParser.parseManyFromString(json).fold(throw _, x => x))
  def apply(file: jFile): Table  = apply(file.slurpString)
}
