/*
 * Copyright 2020 Precog Data
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

package quasar.impl

import quasar.api.datasource.DatasourceType
import quasar.api.datasource.DatasourceError.ConfigurationError
import quasar.connector.datasource.{HeavyweightDatasourceModule, LightweightDatasourceModule, Reconfiguration}

import scala.Long
import scala.util.Either

import argonaut.Json

import cats.effect.Sync

sealed trait DatasourceModule {
  def kind: DatasourceType

  def sanitizeConfig(version: Long, config: Json): Json

  def minVersion: Long

  def migrateConfig[F[_]: Sync](from: Long, to: Long, config: Json)
      : F[Either[ConfigurationError[Json], Json]]

  def reconfigure(version: Long, original: Json, patch: Json)
      : Either[ConfigurationError[Json], (Reconfiguration, Json)]
}

object DatasourceModule {
  final case class Lightweight(lw: LightweightDatasourceModule) extends DatasourceModule {
    def kind = lw.kind

    def sanitizeConfig(version: Long, config: Json): Json =
      lw.sanitizeConfig(version, config)

    def minVersion: Long =
      lw.minVersion

    def migrateConfig[F[_]: Sync](from: Long, to: Long, config: Json)
        : F[Either[ConfigurationError[Json], Json]] =
      lw.migrateConfig(from, to, config)

    def reconfigure(version: Long, original: Json, patch: Json)
        : Either[ConfigurationError[Json], (Reconfiguration, Json)] =
      lw.reconfigure(version, original, patch)
  }

  final case class Heavyweight(hw: HeavyweightDatasourceModule) extends DatasourceModule {
    def kind = hw.kind

    def minVersion: Long =
      hw.minVersion

    def sanitizeConfig(version: Long, config: Json): Json =
      hw.sanitizeConfig(version, config)

    def migrateConfig[F[_]: Sync](from: Long, to: Long, config: Json)
        : F[Either[ConfigurationError[Json], Json]] =
      hw.migrateConfig(from, to, config)

    def reconfigure(version: Long, original: Json, patch: Json)
        : Either[ConfigurationError[Json], (Reconfiguration, Json)] =
      hw.reconfigure(version, original, patch)
  }
}
