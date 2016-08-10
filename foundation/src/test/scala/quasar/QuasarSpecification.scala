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

package quasar

import quasar.Predef._
import quasar.fp._
import org.specs2.mutable._
import org.specs2.scalaz.ScalazMatchers
import scalaz._

trait QuasarSpecification extends SpecificationLike with ScalazMatchers with PendingWithAccurateCoverage {
  // Fail fast and repot all timings when running on CI.
  if (scala.sys.env contains "TRAVIS") {
    args(stopOnFail=true)
    args.report(showtimes = true)
  }

  implicit class Specs2ScalazOps[A : Equal : Show](lhs: A) {
    def must_=(rhs: A) = lhs must equal(rhs)
  }
}
