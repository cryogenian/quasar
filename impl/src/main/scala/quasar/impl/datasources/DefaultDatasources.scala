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

package quasar.impl.datasources

import slamdata.Predef._

import quasar.Condition
import quasar.api.datasource._
import quasar.api.datasource.DatasourceError._
import quasar.api.resource._
import quasar.connector.datasource.Reconfiguration
import quasar.contrib.scalaz.MonadError_
import quasar.impl.{CachedGetter, IndexedSemaphore, QuasarDatasource, ResourceManager}, CachedGetter.Signal._
import quasar.impl.storage.IndexedStore

import cats.~>
import cats.data.{EitherT, OptionT}
import cats.effect.{Concurrent, ContextShift, Sync, Resource}
import cats.implicits._

import fs2.Stream

import scalaz.{Equal}

import shims.{equalToCats, functorToCats}

private[impl] final class DefaultDatasources[
    T[_[_]],
    F[_]: Sync: MonadError_[?[_], CreateError[C]],
    G[_], H[_],
    I: Equal, C: Equal, R] private (
    semaphore: IndexedSemaphore[F, I],
    freshId: F[I],
    refs: IndexedStore[F, I, DatasourceRef[C]],
    modules: DatasourceModules[T, F, G, H, I, C, R, ResourcePathType],
    getter: CachedGetter[F, I, DatasourceRef[C]],
    cache: ResourceManager[F, I, QuasarDatasource[T, G, H, R, ResourcePathType]],
    errors: DatasourceErrors[F, I],
    byteStores: ByteStores[F, I])
    extends Datasources[F, Stream[F, ?], I, C] {

  def addDatasource(ref: DatasourceRef[C]): F[Either[CreateError[C], I]] = for {
    i <- freshId
    c <- addRef[CreateError[C]](i, Reconfiguration.Preserve, ref)
  } yield Condition.eitherIso.get(c).as(i)

  def allDatasourceMetadata: F[Stream[F, (I, DatasourceMeta)]] =
    Sync[F].pure(refs.entries.evalMap {
      case (i, DatasourceRef(k, n, _)) =>
        errors.datasourceError(i) map { e =>
          (i, DatasourceMeta.fromOption(k, n, e))
        }
    })

  def datasourceRef(i: I, supported: Option[DatasourceType]): F[Either[ExistentialError[I], DatasourceRef[C]]] =
    EitherT(lookupRef[ExistentialError[I]](i))
      .flatMap({(ref: DatasourceRef[C]) => supported match {
        case None =>
          EitherT.pure[F, ExistentialError[I]](ref)
        case Some(typ) if typ.name =!= ref.kind.name =>
          EitherT.leftT[F, DatasourceRef[C]](datasourceNotFound[I, ExistentialError[I]](i))
        case Some(typ) =>
          modules.migrateRef(typ.version, ref).leftMap(x => datasourceNotFound[I, ExistentialError[I]](i))
      }})
      .map(modules.sanitizeRef(_))
      .value

  def datasourceStatus(i: I): F[Either[ExistentialError[I], Condition[Exception]]] =
    EitherT(lookupRef[ExistentialError[I]](i))
      .flatMap(_ => EitherT.right[ExistentialError[I]](errors.datasourceError(i)))
      .map(Condition.optionIso.reverseGet(_))
      .value

  def removeDatasource(i: I): F[Condition[ExistentialError[I]]] =
    refs.delete(i).ifM(
      dispose(i, true).as(Condition.normal[ExistentialError[I]]()),
      Condition.abnormal(datasourceNotFound[I, ExistentialError[I]](i)).pure[F])

  def replaceDatasource(i: I, ref: DatasourceRef[C]): F[Condition[DatasourceError[I, C]]] =
    replaceDatasourceImpl(i, Reconfiguration.Reset, ref)

  private def replaceDatasourceImpl(i: I, reconf: Reconfiguration, ref: DatasourceRef[C]): F[Condition[DatasourceError[I, C]]] = {
    lazy val notFound =
      Condition.abnormal(datasourceNotFound[I, DatasourceError[I, C]](i))

    def doReplace(prev: DatasourceRef[C]): F[Condition[DatasourceError[I, C]]] =
      if (ref === prev)
        Condition.normal[DatasourceError[I, C]]().pure[F]
      else if (DatasourceRef.atMostRenamed(ref, prev))
        setRef(i, ref)
      else
        addRef[DatasourceError[I, C]](i, reconf, ref)

    throughSemaphore(i) {
      getter(i).flatMap {
        // We're replacing, emit abnormal condition if there was no ref
        case Empty =>
          notFound.pure[F]

        // it's removed, but resource hasn't been finalized
        case Removed(_) =>
          dispose(i, true).as(notFound)

        // The last value we knew about has since been externally updated,
        // but our update replaces it, so we ignore the new value and
        // replace the old.
        case Updated(_, old) =>
          doReplace(old)

        case Present(value) =>
          doReplace(value)
      } <* getter(i)
    }
  }

  def reconfigureDatasource(datasourceId: I, patch: C, patchVersion: Long)
      : F[Condition[DatasourceError[I, C]]] =
    lookupRef[DatasourceError[I, C]](datasourceId) flatMap {
      case Left(err) => Condition.abnormal(err: DatasourceError[I, C]).pure[F]
      case Right(ref) => modules.reconfigureRef(ref, patch, patchVersion).value flatMap {
        case Left(err) =>
          Condition.abnormal(err: DatasourceError[I, C]).pure[F]

        case Right((reconf, patched)) =>
          replaceDatasourceImpl(datasourceId, reconf, patched)
      }
    }

  def renameDatasource(datasourceId: I, name: DatasourceName)
      : F[Condition[DatasourceError[I, C]]] =
    lookupRef[DatasourceError[I, C]](datasourceId) flatMap {
      case Left(err) => Condition.abnormal(err: DatasourceError[I, C]).pure[F]
      case Right(ref) => replaceDatasource(datasourceId, ref.copy(name = name))
    }

  def copyDatasource(datasourceId: I, modifyName: DatasourceName => DatasourceName): F[Either[DatasourceError[I, C], I]] = {
    val action = for {
      ref <- EitherT(lookupRef[DatasourceError[I, C]](datasourceId))
      ref0 = ref.copy(name = modifyName(ref.name))
      id <- EitherT(addDatasource(ref0)).leftMap(x => x: DatasourceError[I, C])
    } yield id
    action.value
  }

  def supportedDatasourceTypes: F[Set[(DatasourceType, Long)]] =
    modules.supportedTypes

  type QDS = QuasarDatasource[T, G, H, R, ResourcePathType]

  def quasarDatasourceOf(i: I): F[Option[QDS]] = {
    def create(ref: DatasourceRef[C]): F[QDS] =
      for {
        allocated <- createErrorHandling(modules.create(i, ref)).allocated
        _ <- cache.manage(i, allocated)
      } yield allocated._1

    def fromCacheOrCreate(ref: DatasourceRef[C]): F[QDS] =
      OptionT(cache.get(i)) getOrElseF create(ref)

    throughSemaphore(i) {
      getter(i) flatMap {
        case Empty =>
          (None: Option[QDS]).pure[F]

        case Removed(_) =>
          dispose(i, true).as(None: Option[QDS])

        case Updated(incoming, old) if DatasourceRef.atMostRenamed(incoming, old) =>
          fromCacheOrCreate(incoming).map(_.some)

        case Updated(incoming, old) =>
          dispose(i, true) >> create(incoming).map(_.some)

        case Present(value) =>
          fromCacheOrCreate(value).map(_.some)
      }
    }
  }

  private def addRef[E >: CreateError[C] <: DatasourceError[I, C]](i: I, reconf: Reconfiguration, ref: DatasourceRef[C]): F[Condition[E]] = {
    val clearBS = reconf match {
      case Reconfiguration.Reset => true
      case Reconfiguration.Preserve => false
    }

    val action: EitherT[F, E, Unit] = for {
      _ <- verifyNameUnique[E](ref.name, i)
      // Grab managed ds and if it's presented shut it down
      mbCurrent <- EitherT.right(cache.get(i))
      _ <- EitherT.right(mbCurrent.fold(().pure[F])(_ => dispose(i, clearBS)))
      allocated <- EitherT(modules.create(i, ref).value.allocated map {
        case (Left(e), _) => Left(e: E)
        case (Right(a), finalize) => Right((a, finalize))
      })
      _ <- EitherT.right(refs.insert(i, ref))
      _ <- EitherT.right(cache.manage(i, allocated))
    } yield ()

    action.value.map(Condition.eitherIso.reverseGet(_))
  }

  private def setRef(i: I, ref: DatasourceRef[C]): F[Condition[DatasourceError[I, C]]] = {
    val action: EitherT[F, DatasourceError[I, C], Unit] = for {
      _ <- verifyNameUnique[DatasourceError[I, C]](ref.name, i)
      _ <- EitherT.right(refs.insert(i, ref))
    } yield ()

    action.value.map(Condition.eitherIso.reverseGet(_))
  }

  private def lookupRef[E >: ExistentialError[I] <: DatasourceError[I, C]](i: I): F[Either[E, DatasourceRef[C]]] =
    refs.lookup(i).map {
      case None => datasourceNotFound[I, E](i).asLeft
      case Some(a) => a.asRight
    }

  private def verifyNameUnique[E >: CreateError[C] <: DatasourceError[I, C]](name: DatasourceName, i: I): EitherT[F, E, Unit] =
    EitherT {
      refs.entries
        .exists(t => t._2.name === name && t._1 =!= i)
        .compile
        .fold(false)(_ || _)
        .map((x: Boolean) => if (x) datasourceNameExists[E](name).asLeft[Unit] else ().asRight)
    }

  private def dispose(i: I, clear: Boolean): F[Unit] =
    cache.shutdown(i) >> byteStores.clear(i).whenA(clear)

  private val createErrorHandling: EitherT[Resource[F, ?], CreateError[C], ?] ~> Resource[F, ?] =
    λ[EitherT[Resource[F, ?], CreateError[C], ?] ~> Resource[F, ?]]( inp =>
      inp.value.flatMap(_.fold(
        (x: CreateError[C]) => Resource.liftF(MonadError_[F, CreateError[C]].raiseError(x)),
        _.pure[Resource[F, ?]])))

  private def throughSemaphore(i: I): F ~> F =
    λ[F ~> F](fa => semaphore.get(i).use(_ => fa))
}

object DefaultDatasources {
  private[impl] def apply[
      T[_[_]],
      F[_]: Concurrent: ContextShift: MonadError_[?[_], CreateError[C]],
      G[_], H[_],
      I: Equal, C: Equal, R](
      freshId: F[I],
      refs: IndexedStore[F, I, DatasourceRef[C]],
      modules: DatasourceModules[T, F, G, H, I, C, R, ResourcePathType],
      cache: ResourceManager[F, I, QuasarDatasource[T, G, H, R, ResourcePathType]],
      errors: DatasourceErrors[F, I],
      byteStores: ByteStores[F, I])
      : F[DefaultDatasources[T, F, G, H, I, C, R]] = for {
    semaphore <- IndexedSemaphore[F, I]
    getter <- CachedGetter(refs.lookup(_))
  } yield new DefaultDatasources(semaphore, freshId, refs, modules, getter, cache, errors, byteStores)
}
