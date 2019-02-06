/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.qscript

import slamdata.Predef.{List, StringContext}
import quasar.{IdStatus, ParseInstruction, RenderTree}

import monocle.macros.Lenses
import scalaz.{Equal, Show}
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.show._

@Lenses final case class InterpretedRead[A](
  path: A,
  idStatus: IdStatus,
  instructions: List[ParseInstruction])

object InterpretedRead {

  implicit def equal[A: Equal]: Equal[InterpretedRead[A]] =
    Equal.equalBy(r => (r.path, r.idStatus, r.instructions))

  implicit def show[A: Show]: Show[InterpretedRead[A]] =
    RenderTree.toShow

  implicit def renderTree[A: Show]: RenderTree[InterpretedRead[A]] =
    RenderTree.simple(
      List("InterpretedRead"),
      r => some(s"${r.path.shows},  ${r.idStatus.shows}, ${r.instructions.shows}"))
}
