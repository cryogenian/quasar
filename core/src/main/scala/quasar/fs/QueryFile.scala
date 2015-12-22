package quasar
package fs

import quasar.Predef._
import quasar.fp._
import quasar.fs.{Path => QPath}
import quasar.recursionschemes._, Recursive.ops._

import pathy.Path._
import scalaz._, Scalaz._
import scalaz.stream.Process

sealed trait QueryFile[A]

object QueryFile {
  final case class ExecutePlan(lp: Fix[LogicalPlan], out: AFile)
    extends QueryFile[(PhaseResults, FileSystemError \/ ResultFile)]

  final case class Explain(lp: Fix[LogicalPlan], out: AFile)
    extends QueryFile[(PhaseResults, Option[FileSystemError])]

  /** TODO: While this is a bit better in one dimension here in `QueryFile`,
    *       `@mossprescott` points out it is still a bit of a stretch to include
    *       in this algebra. We need to revisit this and probably add algebras
    *       over multiple dimensions to better organize these (and other)
    *       operations.
    *
    *       For more discussion, see
    *       https://github.com/quasar-analytics/quasar/pull/986#discussion-diff-45081757
    */
  final case class ListContents(dir: ADir)
    extends QueryFile[FileSystemError \/ Set[Node]]

  /** File used in explain output when a `LogicalPlan` does not contain any
    * file references.
    */
  val DefaultExplainOut: AFile = rootDir </> dir("explain") </> file("out")

  final class Ops[S[_]](implicit S0: Functor[S], S1: QueryFileF :<: S) {
    import ResultFile._

    type F[A] = Free[S, A]
    type M[A] = FileSystemErrT[F, A]

    val transforms = Transforms[F]
    import transforms._

    /** Returns the path to the result of executing the given [[LogicalPlan]],
      * using the provided path if possible.
      *
      * Execution of certain plans may return a result file other than the
      * requested file if it is more efficient to do so (i.e. to avoid copying
      * lots of data for a plan consisting of a single `ReadF(...)`).
      */
    def execute(plan: Fix[LogicalPlan], out: AFile): ExecM[ResultFile] =
      EitherT(WriterT(lift(ExecutePlan(plan, out))): G[FileSystemError \/ ResultFile])

    /** Returns the path to the result of executing the given [[LogicalPlan]] */
    def execute_(plan: Fix[LogicalPlan])
                (implicit M: ManageFile.Ops[S]): ExecM[ResultFile] = {
      for {
        out <- toExec(firstFile(plan) cata (M.tempFileNear, M.anyTempFile))
        rf0 <- execute(plan, out)
        rf1 =  user.getOrModify(rf0)
                 .map(usr => if (usr == out) Temp(usr) else User(usr))
                 .merge
      } yield rf1
    }

    /** Returns a description of how the the given logical plan will be executed
      * along with any error encountered during planning.
      */
    def explain(plan: Fix[LogicalPlan])
               (implicit M: ManageFile.Ops[S])
               : F[(PhaseResults, Option[FileSystemError])] = {

      firstFile(plan)
        .cata(M.tempFileNear, M.anyTempFile)
        .flatMap(f => lift(Explain(plan, f)))
    }

    /** Returns the source of values from the result of executing the given
      * [[LogicalPlan]].
      */
    def evaluate(plan: Fix[LogicalPlan])
                (implicit R: ReadFile.Ops[S], M: ManageFile.Ops[S])
                : Process[ExecM, Data] = {

      val hoistFS: FileSystemErrT[F, ?] ~> ExecM =
        Hoist[FileSystemErrT].hoist[F, G](liftMT[F, PhaseResultT])

      def values(f: AFile) =
        R.scanAll(f).translate[ExecM](hoistFS)

      def handleTemp(tmp: AFile) = {
        val cleanup = (hoistFS(M.delete(tmp)): ExecM[Unit])
                        .liftM[Process].drain
        values(tmp) onComplete cleanup
      }

      execute_(plan).liftM[Process] flatMap {
        case Case.User(f) => values(f)
        case Case.Temp(f) => handleTemp(f)
      }
    }

    /** Returns the path to the result of executing the given SQL^2 query
      * using the given output file if possible.
      */
    def executeQuery(query: sql.Expr, vars: Variables, out: AFile)
                    : CompExecM[ResultFile] = {

      compileAnd(query, vars)(execute(_, out))
    }

    /** Returns the path to the result of executing the given SQL^2 query. */
    def executeQuery_(query: sql.Expr, vars: Variables)
                     (implicit M: ManageFile.Ops[S]): CompExecM[ResultFile] = {
      compileAnd(query, vars)(execute_)
    }

    /** Returns the source of values from the result of executing the given
      * SQL^2 query.
      */
    def evaluateQuery(query: sql.Expr, vars: Variables)
                     (implicit R: ReadFile.Ops[S], M: ManageFile.Ops[S])
                     : Process[CompExecM, Data] = {

      def comp = compToCompExec(queryPlan(query, vars)).liftM[Process]

      comp flatMap (lp => evaluate(lp).translate[CompExecM](execToCompExec))
    }

    /** Returns a description of how the the given SQL^2 query will be executed
      * along with any error encountered during planning.
      */
    def explainQuery(query: sql.Expr, vars: Variables)
                    (implicit M: ManageFile.Ops[S])
                    : SemanticErrsT[F, (PhaseResults, Option[FileSystemError])] = {

      type E[A, B] = EitherT[F, A, B]

      queryPlan(query, vars).run.run match {
        case (prs, \/-(lp)) =>
          explain(lp).map(_.leftMap(prs ++ _)).liftM[SemanticErrsT]

        case (_, -\/(semErrs)) =>
          semErrs.raiseError[E, (PhaseResults, Option[FileSystemError])]
      }
    }

    /** Returns immediate children of the given directory, fails if the
      * directory does not exist.
      */
    def ls(dir: ADir): M[Set[Node]] =
      EitherT(lift(ListContents(dir)))

    /** The children of the root directory. */
    def ls: M[Set[Node]] =
      ls(rootDir)

    /** Returns all files in this directory and all of it's sub-directories
      * Fails if the directory does not exist.
      */
    def descendantFiles(dir: ADir): M[Set[RFile]] = {
      type S[A] = StreamT[M, A]

      def lsR(desc: RDir): StreamT[M, RFile] =
        StreamT.fromStream[M, Node](ls(dir </> desc) map (_.toStream))
          .flatMap(n => refineType(n.path).fold(
            d => lsR(desc </> d),
            f => (desc </> f).point[S]))

      lsR(currentDir).foldLeft(Set.empty[RFile])(_ + _)
    }

    /** Returns whether the given file exists. */
    def fileExists(file: AFile): F[Boolean] = {
      val parent = fileParent(file)

      ls(parent)
        .map(_ flatMap (_.file.map(parent </> _).toSet) exists (identicalPath(file, _)))
        .getOrElse(false)
    }

    ////

    private def compileAnd[A](query: sql.Expr, vars: Variables)
                             (f: Fix[LogicalPlan] => ExecM[A])
                             : CompExecM[A] = {
      compToCompExec(queryPlan(query, vars))
        .flatMap(lp => execToCompExec(f(lp)))
    }

    private def firstFile(lp: Fix[LogicalPlan]): Option[AFile] =
    // This implementation, although more resource efficient, cannot be used right now because it triggers
    // a compiler crash in conjunction with the scoverage compiler plugin. As a compromise in order to have
    // code coverage, we currently have a less efficient implementation that sidesteps the compiler crash.
    // https://github.com/scoverage/scalac-scoverage-plugin/issues/147
    //  Tag.unwrap(lp foldMap[FirstOption[QPath]] {
    //    case Fix(LogicalPlan.ReadF(p)) => Tag(Some(p))
    //    case _                         => Tag(None)
    //  }) flatMap pathToAbsFile
    lp.collect{case Fix(LogicalPlan.ReadF(p)) => p}.headOption.flatMap(pathToAbsFile)

    private def pathToAbsFile(p: QPath): Option[AFile] =
      p.file map (fn =>
        p.asAbsolute.dir
          .foldLeft(rootDir[Sandboxed])((d, n) => d </> dir(n.value)) </>
          file(fn.value))

    private def lift[A](qf: QueryFile[A]): F[A] =
      Free.liftF(S1.inj(Coyoneda.lift(qf)))
  }

  object Ops {
    implicit def apply[S[_]](implicit S0: Functor[S], S1: QueryFileF :<: S): Ops[S] =
      new Ops[S]
  }

  class Transforms[F[_]: Monad] {
    type G[A] = PhaseResultT[F, A]
    type H[A] = SemanticErrsT[G, A]

    type ExecM[A]     = FileSystemErrT[G, A]
    type CompExecM[A] = FileSystemErrT[H, A]

    val execToCompExec: ExecM ~> CompExecM =
      Hoist[FileSystemErrT].hoist[G, H](liftMT[G, SemanticErrsT])

    val compToCompExec: CompileM ~> CompExecM = {
      val hoistW: PhaseResultW ~> G = Hoist[PhaseResultT].hoist(pointNT[F])
      val hoistC: CompileM ~> H     = Hoist[SemanticErrsT].hoist(hoistW)
      liftMT[H, FileSystemErrT] compose hoistC
    }

    val toExec: F ~> ExecM =
      liftMT[G, FileSystemErrT] compose liftMT[F, PhaseResultT]

    val toCompExec: F ~> CompExecM =
      execToCompExec compose toExec
  }

  object Transforms {
    def apply[F[_]: Monad]: Transforms[F] =
      new Transforms[F]
  }
}
