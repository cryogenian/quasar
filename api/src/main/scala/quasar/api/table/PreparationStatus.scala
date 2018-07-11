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

package quasar.api.table

import java.lang.Throwable
import java.time.{Duration, OffsetDateTime}
import slamdata.Predef.{Option, Product, Serializable}

sealed trait PreparationStatus extends Product with Serializable

object PreparationStatus {
  case object Unprepared
      extends PreparationStatus

  final case class Preparing(startedAt: OffsetDateTime, previous: Option[Prepared])
      extends PreparationStatus

  final case class Prepared(startedAt: OffsetDateTime, duration: Duration)
      extends PreparationStatus

  final case class Errored(startedAt: OffsetDateTime, duration: Duration, error: Throwable, previous: Option[Prepared])
      extends PreparationStatus
}