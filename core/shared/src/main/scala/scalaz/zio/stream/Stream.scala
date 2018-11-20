package scalaz.zio.stream

import scalaz.zio._

/**
 * A `Stream[E, A]` represents an effectful stream that can produce values of
 * type `A`, or potentially fail with a value of type `E`.
 *
 * Streams have a very similar API to Scala collections, making them immediately
 * familiar to most developers. Unlike Scala collections, streams can be used
 * on effectful streams of data, such as HTTP connections, files, and so forth.
 *
 * Streams do not leak resources. This guarantee holds in the presence of early
 * termination (not all of a stream is consumed), failure, or even interruption.
 *
 * Thanks to only first-order types, appropriate variance annotations, and
 * specialized effect type (ZIO), streams feature extremely good type inference
 * and should almost never require specification of any type parameters.
 *
 */
trait Stream[+E, +A] { self =>
  import Stream._

  /**
   * Executes an effectful fold over the stream of values.
   */
  def fold[E1 >: E, A1 >: A, S](s: S)(f: (S, A1) => IO[E1, Step[S]]): IO[E1, Step[S]] =
    foldLazy[E1, A1, Step[S]](Step.cont(s)) {
      case Step.Cont(_) => true
      case _            => false
    }((s, a) => f(s.extract, a))

  def foldLazy[E1 >: E, A1 >: A, S](s: S)(cont: S => Boolean)(f: (S, A1) => IO[E1, S]): IO[E1, S] =
    fold[E1, A1, S](s) { (s, a) =>
      if (cont(s)) f(s, a).map(Step.cont)
      else IO.now(Step.stop(s))
    }.map(_.extract)

  def foldLeft[A1 >: A, S](s: S)(f: (S, A1) => S): IO[E, S] =
    foldLazy(s)(_ => true)((s, a) => IO.now(f(s, a)))

  /**
   * Concatenates the specified stream to this stream.
   */
  final def ++[E1 >: E, A1 >: A](that: => Stream[E1, A1]): Stream[E1, A1] =
    new Stream[E1, A1] {
      override def fold[E2 >: E1, A2 >: A1, S](s: S)(f: (S, A2) => IO[E2, Step[S]]): IO[E2, Step[S]] =
        self.fold[E2, A, S](s)(f).flatMap {
          case Step.Cont(s) => that.fold[E2, A1, S](s)(f)
          case s            => IO.now(s)
        }
    }

  /**
   * Filters this stream by the specified predicate, retaining all elements for
   * which the predicate evaluates to true.
   */
  def filter(pred: A => Boolean): Stream[E, A] = new Stream[E, A] {
    override def foldLazy[E1 >: E, A1 >: A, S](s: S)(cont: S => Boolean)(f: (S, A1) => IO[E1, S]): IO[E1, S] =
      self.foldLazy[E1, A, S](s)(cont) { (s, a) =>
        if (pred(a)) f(s, a)
        else IO.now(s)
      }
  }

  /**
   * Filters this stream by the specified predicate, removing all elements for
   * which the predicate evaluates to true.
   */
  final def filterNot(pred: A => Boolean): Stream[E, A] = filter(a => !pred(a))

  /**
   * Consumes elements of the stream, passing them to the specified callback,
   * and terminating consumption when the callback returns `false`.
   */
  final def foreach0[E1 >: E, A1 >: A](f: A1 => IO[E1, Boolean]): IO[E1, Unit] = {
    val Cont = Step.Cont(IO.unit)
    val Stop = Step.Stop(IO.unit)

    IO.flatten(
      fold[E1, A, IO[E1, Unit]](IO.unit)((io, a) => (io *> f(a)).map(b => if (b) Cont else Stop)).map(_.extract)
    )
  }

  /**
   * Performs a filter and map in a single step.
   */
  def collect[B](pf: PartialFunction[A, B]): Stream[E, B] =
    new Stream[E, B] {
      override def fold[E1 >: E, B1 >: B, S](s: S)(f: (S, B1) => IO[E1, Step[S]]): IO[E1, Step[S]] =
        self.fold[E1, A, S](s)((s, a) => if (pf.isDefinedAt(a)) f(s, pf(a)) else IO.now(Step.Cont(s)))
    }

  /**
   * Drops the specified number of elements from this stream.
   */
  final def drop(n: Int): Stream[E, A] =
    self.zipWithIndex.filter(_._2 > n - 1).map(_._1)

  /**
   * Drops all elements of the stream for as long as the specified predicate
   * evaluates to `true`.
   */
  def dropWhile(pred: A => Boolean): Stream[E, A] = new Stream[E, A] {
    override def fold[E1 >: E, A1 >: A, S](s: S)(f: (S, A1) => IO[E1, Step[S]]): IO[E1, Step[S]] =
      (self
        .fold[E1, A, (Boolean, S)](true -> s) {
          case ((true, s), a) if pred(a) => IO.now(Step.Cont(true   -> s))
          case ((_, s), a)               => f(s, a).map(_.map(false -> _))
        })
        .map(_.map(_._2))
  }

  /**
   * Maps each element of this stream to another stream, and returns the
   * concatenation of those streams.
   */
  final def flatMap[E1 >: E, B](f0: A => Stream[E1, B]): Stream[E1, B] = new Stream[E1, B] {
    override def fold[E2 >: E1, B1 >: B, S](s: S)(f: (S, B1) => IO[E2, Step[S]]): IO[E2, Step[S]] =
      self.fold[E2, A, S](s)((s, a) => f0(a).fold[E2, B1, S](s)(f))
  }

  /**
   * Consumes all elements of the stream, passing them to the specified callback.
   */
  final def foreach[E1 >: E, A1 >: A](f: A1 => IO[E1, Unit]): IO[E1, Unit] =
    foreach0(f.andThen(_.const(true)))

  /**
   * Repeats this stream forever.
   */
  def forever: Stream[E, A] =
    new Stream[E, A] {
      override def fold[E1 >: E, A1 >: A, S](s: S)(f: (S, A1) => IO[E1, Step[S]]): IO[E1, Step[S]] = {
        def loop(s: S): IO[E1, Step[S]] =
          self.fold[E1, A, S](s)(f).flatMap {
            case s @ Step.Stop(_) => IO.now(s)
            case Step.Cont(s)     => loop(s)
          }

        loop(s)
      }
    }

  /**
   * Joins two streams together with a specified join function.
   */
  final def joinWith[E1 >: E, B, C](that: Stream[E1, B], lc: Int = 1, rc: Int = 1)(
    f0: (IO[E1, Option[A]], IO[E1, Option[B]]) => IO[E1, Option[C]]
  ): Stream[E1, C] =
    new Stream[E1, C] {
      override def fold[E2 >: E1, A1 >: C, S](s: S)(f: (S, A1) => IO[E2, Step[S]]): IO[E2, Step[S]] = {
        def loop(q1: Queue[Take[E1, A]], q2: Queue[Take[E1, B]], s: S): IO[E2, Step[S]] =
          f0(Take.option(q1.take), Take.option(q2.take)).flatMap {
            case None => IO.now(Step.Stop(s))
            case Some(c) =>
              f(s, c).flatMap {
                case Step.Cont(s) => loop(q1, q2, s)
                case s            => IO.now(s)
              }
          }

        self.toQueue[E1, A](lc).use(q1 => that.toQueue[E1, B](rc).use(q2 => loop(q1, q2, s)))
      }
    }

  /**
   * Maps over elements of the stream with the specified function.
   */
  def map[B](f0: A => B): Stream[E, B] = new Stream[E, B] {
    override def foldLazy[E1 >: E, B1 >: B, S](s: S)(cont: S => Boolean)(f: (S, B1) => IO[E1, S]): IO[E1, S] =
      self.foldLazy[E1, A, S](s)(cont)((s, a) => f(s, f0(a)))
  }

  /**
   * Maps each element to a chunk, and flattens the chunks into the output of
   * this stream.
   */
  def mapConcat[B](f0: A => Chunk[B]): Stream[E, B] = new Stream[E, B] {
    override def fold[E1 >: E, B1 >: B, S](s: S)(f: (S, B1) => IO[E1, Step[S]]): IO[E1, Step[S]] = {
      def loop(s: S, c: Chunk[B], i: Int): IO[E1, Step[S]] =
        if (i >= c.length) IO.now(Step.cont(s))
        else
          f(s, c(i)) flatMap {
            case Step.Cont(s) => loop(s, c, i + 1)
            case s            => IO.now(s)
          }

      self.fold[E1, A, S](s)((s, a) => loop(s, f0(a), 0))
    }
  }

  /**
   * Maps over elements of the stream with the specified effectful function.
   */
  final def mapM[E1 >: E, B](f0: A => IO[E1, B]): Stream[E1, B] = new Stream[E1, B] {
    override def fold[E2 >: E1, B1 >: B, S](s: S)(f: (S, B1) => IO[E2, Step[S]]): IO[E2, Step[S]] =
      self.fold[E2, A, S](s)((s, a) => f0(a).flatMap(f(s, _)))
  }

  /**
   * Merges this stream and the specified stream together.
   */
  final def merge[E1 >: E, A1 >: A](that: Stream[E1, A1], capacity: Int = 1): Stream[E1, A1] =
    self.mergeWith(that, capacity)(identity, identity)

  /**
   * Merges this stream and the specified stream together to produce a stream of
   * eithers.
   */
  final def mergeEither[E1 >: E, B](that: Stream[E1, B], capacity: Int = 1): Stream[E1, Either[A, B]] =
    self.mergeWith(that, capacity)(Left(_), Right(_))

  /**
   * Merges this stream and the specified stream together to a common element
   * type with the specified mapping functions.
   */
  final def mergeWith[E1 >: E, B, C](that: Stream[E1, B], capacity: Int = 1)(l: A => C, r: B => C): Stream[E1, C] =
    new Stream[E1, C] {
      override def fold[E2 >: E1, C1 >: C, S](s: S)(f: (S, C1) => IO[E2, Step[S]]): IO[E2, Step[S]] = {
        type Elem = Either[Take[E2, A], Take[E2, B]]

        def loop(leftDone: Boolean, rightDone: Boolean, s: S, queue: Queue[Elem]): IO[E2, Step[S]] =
          queue.take.flatMap {
            case Left(Take.Fail(e))  => IO.fail(e)
            case Right(Take.Fail(e)) => IO.fail(e)
            case Left(Take.End) =>
              if (rightDone) IO.now(Step.Cont(s))
              else loop(true, rightDone, s, queue)
            case Left(Take.Value(a)) =>
              f(s, l(a)).flatMap {
                case Step.Cont(s)     => loop(leftDone, rightDone, s, queue)
                case s @ Step.Stop(_) => IO.now(s)
              }
            case Right(Take.End) =>
              if (leftDone) IO.now(Step.Cont(s))
              else loop(leftDone, true, s, queue)
            case Right(Take.Value(b)) =>
              f(s, r(b)).flatMap {
                case Step.Cont(s)     => loop(leftDone, rightDone, s, queue)
                case s @ Step.Stop(_) => IO.now(s)
              }
          }

        (for {
          queue  <- Queue.bounded[Elem](capacity)
          putL   = (a: A) => queue.offer(Left(Take.Value(a))).void
          putR   = (b: B) => queue.offer(Right(Take.Value(b))).void
          catchL = (e: E2) => queue.offer(Left(Take.Fail(e)))
          catchR = (e: E2) => queue.offer(Right(Take.Fail(e)))
          endL   = queue.offer(Left(Take.End)).forever
          endR   = queue.offer(Right(Take.End)).forever
          _      <- (self.foreach(putL) *> endL).catchAll(catchL).fork
          _      <- (that.foreach(putR) *> endR).catchAll(catchR).fork
          step   <- loop(false, false, s, queue)
        } yield step).supervised
      }
    }

  /**
   * Peels off enough material from the stream to construct an `R` using the
   * provided `Sink`, and then returns both the `R` and the remainder of the
   * `Stream` in a managed resource. Like all `Managed` resources, the provided
   * remainder is valid only within the scope of `Managed`.
   */
  final def peel[E1 >: E, A1 >: A, R](sink: Sink[E1, A1, A1, R]): Managed[E1, (R, Stream[E1, A1])] = {
    type Folder = (Any, A1) => IO[E1, Step[Any]]
    type Fold   = (Any, Folder)
    type State  = Either[sink.State, Fold]
    type Result = (R, Stream[E1, A1])

    def feed[S](chunk: Chunk[A1], i: Int = 0)(s: S, f: (S, A1) => IO[E1, Step[S]]): IO[E1, Step[S]] =
      if (i >= chunk.length) IO.now(Step.Cont(s))
      else {
        val a1 = chunk(i)

        f(s, a1).flatMap {
          case Step.Stop(s) => IO.now(Step.Stop(s))
          case Step.Cont(s) => feed(chunk, i + 1)(s, f)
        }
      }

    def tail(resume: Promise[Nothing, Fold], done: Promise[E1, Step[Any]]): Stream[E1, A1] =
      new Stream[E1, A1] {
        override def fold[E2 >: E1, A2 >: A1, S](s: S)(f: (S, A2) => IO[E2, Step[S]]): IO[E2, Step[S]] =
          resume.complete(s -> f.asInstanceOf[Folder]) *>
            done.get.asInstanceOf[IO[E2, Step[S]]]
      }

    def acquire(lstate: sink.State): IO[Nothing, (Fiber[E1, Step[State]], Promise[E1, Result])] =
      for {
        resume <- Promise.make[Nothing, Fold]
        done   <- Promise.make[E1, Step[Any]]
        result <- Promise.make[E1, Result]
        fiber <- self
                  .fold[E1, A1, State](Left(lstate)) {
                    case (Left(lstate), a) =>
                      sink.step(lstate, a).flatMap { step =>
                        if (Sink.Step.cont(step)) IO.now(Step.Cont(Left(Sink.Step.state(step))))
                        else {
                          val lstate = Sink.Step.state(step)
                          val as     = Chunk(a) ++ Sink.Step.leftover(step)

                          sink.extract(lstate).flatMap { r =>
                            result.complete(r -> tail(resume, done)) *>
                              resume.get
                                .flatMap(t => feed(as)(t._1, t._2).map(step => step.map(s => Right(s -> t._2))))
                          }
                        }
                      }
                    case (Right((rstate, f)), a) =>
                      f(rstate, a).flatMap {
                        case Step.Stop(rstate) =>
                          IO.now(Step.Stop(Right(rstate -> f)))

                        case Step.Cont(rstate) =>
                          IO.now(Step.Cont(Right(rstate -> f)))
                      }
                  }
                  .onError(result.done(_).void)
                  .fork
                  .flatMap(fiber => fiber.observe.flatMap(done.done(_).fork.void).const(fiber))
      } yield (fiber, result)

    Managed
      .liftIO(sink.initial)
      .flatMap(step => Managed(acquire(Sink.Step.state(step)))(_._1.interrupt).flatMap(t => Managed.liftIO(t._2.get)))
  }

  /**
   * Repeats the entire stream using the specified schedule. The stream will execute normally,
   * and then repeat again according to the provided schedule.
   */
  def repeat(schedule: Schedule[Unit, _], clock: Clock = Clock.Live): Stream[E, A] =
    new Stream[E, A] {
      override def foldLazy[E1 >: E, A1 >: A, S](s: S)(cont: S => Boolean)(f: (S, A1) => IO[E1, S]) = {
        def loop(s: S, sched: schedule.State): IO[E1, S] =
          self.foldLazy[E1, A1, S](s)(cont)(f).seq(schedule.update((), sched, clock)).flatMap {
            case (s, decision) =>
              if (decision.cont) IO.unit.delay(decision.delay) *> loop(s, decision.state)
              else IO.now(s)
          }

        schedule.initial(clock).flatMap(loop(s, _))
      }
    }

  /**
   * Repeats elements of the stream using the provided schedule.
   */
  def repeatElems[B](schedule: Schedule[A, B], clock: Clock = Clock.Live): Stream[E, A] =
    new Stream[E, A] {
      override def foldLazy[E1 >: E, A1 >: A, S](s: S)(cont: S => Boolean)(f: (S, A1) => IO[E1, S]) = {
        def loop(s: S, sched: schedule.State, a: A): IO[E1, S] =
          schedule.update(a, sched, clock).flatMap { decision =>
            if (decision.cont)
              IO.unit.delay(decision.delay) *> f(s, a).flatMap(loop(_, decision.state, a))
            else IO.now(s)
          }

        schedule.initial(clock).flatMap { sched =>
          self.foldLazy[E1, A, S](s)(cont) { (s, a) =>
            loop(s, sched, a)
          }
        }
      }
    }

  /**
   * Runs the sink on the stream to produce either the sink's result or an error.
   */
  def run[E1 >: E, A0, A1 >: A, B](sink: Sink[E1, A0, A1, B]): IO[E1, B] =
    sink.initial
      .flatMap(
        state =>
          self
            .foldLazy[E1, A1, Sink.Step[sink.State, A0]](state)(Sink.Step.cont)(
              (s, a) => sink.step(Sink.Step.state(s), a)
            )
            .flatMap(step => sink.extract(Sink.Step.state(step)))
      )

  /**
   * Statefully maps over the elements of this stream to produce new elements.
   */
  def scan[S1, B](s1: S1)(f1: (S1, A) => (S1, B)): Stream[E, B] = new Stream[E, B] {
    override def fold[E1 >: E, B1 >: B, S](s: S)(f: (S, B1) => IO[E1, Step[S]]): IO[E1, Step[S]] =
      self
        .fold[E1, A, (S, S1)](s -> s1) {
          case ((s, s1), a) =>
            val (s2, b) = f1(s1, a)

            f(s, b).map(_.map(s => s -> s2))
        }
        .map(_.map(_._1))
  }

  /**
   * Statefully and effectfully maps over the elements of this stream to produce
   * new elements.
   */
  final def scanM[E1 >: E, S1, B](s1: S1)(f1: (S1, A) => IO[E1, (S1, B)]): Stream[E1, B] = new Stream[E1, B] {
    override def fold[E2 >: E1, B1 >: B, S](s: S)(f: (S, B1) => IO[E2, Step[S]]): IO[E2, Step[S]] =
      self
        .fold[E2, A, (S, S1)](s -> s1) {
          case ((s, s1), a) =>
            f1(s1, a).flatMap {
              case (s1, b) =>
                f(s, b).map(_.map(s => s -> s1))
            }
        }
        .map(_.map(_._1))
  }

  /**
   * Takes the specified number of elements from this stream.
   */
  final def take(n: Int): Stream[E, A] =
    self.zipWithIndex.takeWhile(_._2 == n - 1).map(_._1)

  /**
   * Takes all elements of the stream for as long as the specified predicate
   * evaluates to `true`.
   */
  def takeWhile(pred: A => Boolean): Stream[E, A] = new Stream[E, A] {
    override def fold[E1 >: E, A1 >: A, S](s: S)(f: (S, A1) => IO[E1, Step[S]]): IO[E1, Step[S]] =
      self.fold[E1, A, S](s)((s, a) => if (pred(a)) f(s, a) else IO.now(Step.Stop(s)))
  }

  /**
   * Converts the stream to a managed queue. After managed queue is used, the
   * queue will never again produce values and should be discarded.
   */
  final def toQueue[E1 >: E, A1 >: A](capacity: Int = 1): Managed[Nothing, Queue[Take[E1, A1]]] =
    for {
      queue    <- Managed.liftIO(Queue.bounded[Take[E1, A1]](capacity))
      offerVal = (a: A) => queue.offer(Take.Value(a)).void
      offerErr = (e: E) => queue.offer(Take.Fail(e))
      enqueuer = (self.foreach[E, A](offerVal).catchAll(offerErr) *> queue.offer(Take.End).forever).fork
      _        <- Managed(enqueuer)(_.interrupt)
    } yield queue

  /**
   * Applies a transducer to the stream, which converts one or more elements
   * of type `A` into elements of type `C`.
   */
  final def transduce[E1 >: E, A1 >: A, C](sink: Sink[E1, A1, A1, C]): Stream[E1, C] =
    new Stream[E1, C] {
      override def fold[E2 >: E1, C1 >: C, S2](s2: S2)(f: (S2, C1) => IO[E2, Step[S2]]): IO[E2, Step[S2]] = {
        def feed(s1: sink.State, s2: S2, a: Chunk[A1]): IO[E2, Step[(sink.State, S2)]] =
          sink.stepChunk(s1, a).flatMap { step =>
            if (Sink.Step.cont(step)) IO.now(Step.cont((Sink.Step.state(step), s2)))
            else {
              sink
                .extract(Sink.Step.state(step))
                .flatMap(
                  c =>
                    f(s2, c).flatMap {
                      case Step.Cont(s2) =>
                        sink.initial.flatMap(s1 => feed(Sink.Step.state(s1), s2, Sink.Step.leftover(s1)))
                      case Step.Stop(s2) => IO.now(Step.Stop((s1, s2)))
                    }
                )
            }
          }

        sink.initial
          .flatMap(
            s1 =>
              self.fold[E2, A, (sink.State, S2)]((Sink.Step.state(s1), s2)) {
                case ((s1, s2), a) => feed(s1, s2, Chunk(a))
              }
          )
          .map(_.map(_._2))
      }
    }

  /**
   * Adds an effect to consumption of every element of the stream.
   */
  final def withEffect[E1 >: E, A1 >: A](f0: A1 => IO[E1, Unit]): Stream[E1, A] =
    new Stream[E1, A] {
      override def fold[E2 >: E1, A2 >: A, S](s: S)(f: (S, A2) => IO[E2, Step[S]]): IO[E2, Step[S]] =
        self.fold[E2, A, S](s)((s, a2) => f0(a2) *> f(s, a2))
    }

  /**
   * Zips this stream together with the specified stream.
   */
  final def zip[E1 >: E, B](that: Stream[E1, B], lc: Int = 1, rc: Int = 1): Stream[E1, (A, B)] =
    self.zipWith(that, lc, rc)(
      (left, right) => left.flatMap(a => right.map(a -> _))
    )

  /**
   * Zips two streams together with a specified function.
   */
  final def zipWith[E1 >: E, B, C](that: Stream[E1, B], lc: Int = 1, rc: Int = 1)(
    f0: (Option[A], Option[B]) => Option[C]
  ): Stream[E1, C] =
    new Stream[E1, C] {
      override def fold[E2 >: E1, A1 >: C, S](s: S)(f: (S, A1) => IO[E2, Step[S]]): IO[E2, Step[S]] = {
        def loop(q1: Queue[Take[E2, A]], q2: Queue[Take[E2, B]], s: S): IO[E2, Step[S]] =
          Take.option(q1.take).seqWith(Take.option(q2.take))(f0).flatMap {
            case None => IO.now(Step.Cont(s))
            case Some(c) =>
              f(s, c).flatMap {
                case Step.Cont(s) => loop(q1, q2, s)
                case s            => IO.now(s)
              }
          }

        self.toQueue[E2, A](lc).use(q1 => that.toQueue[E2, B](rc).use(q2 => loop(q1, q2, s)))
      }
    }

  /**
   * Zips this stream together with the index of elements of the stream.
   */
  def zipWithIndex: Stream[E, (A, Int)] = new Stream[E, (A, Int)] {
    override def fold[E1 >: E, A1 >: (A, Int), S](s: S)(f: (S, A1) => IO[E1, Step[S]]): IO[E1, Step[S]] =
      self
        .fold[E1, A, (S, Int)]((s, 0)) {
          case ((s, index), a) => f(s, (a, index)).map(_.map(s => (s, index + 1)))
        }
        .map(_.map(_._1))
  }
}

object Stream {
  sealed trait Step[+A] {
    final def extract: A = this match {
      case Step.Cont(value) => value
      case Step.Stop(value) => value
    }

    final def map[B](f: A => B): Step[B] = this match {
      case Step.Cont(value) => Step.Cont(f(value))
      case Step.Stop(value) => Step.Stop(f(value))
    }

    final def duplicate: Step[Step[A]] = this match {
      case Step.Cont(value) => Step.Cont(Step.Cont(value))
      case Step.Stop(value) => Step.Stop(Step.Stop(value))
    }

    final def fold[Z](cont: A => Z, stop: A => Z): Z = this match {
      case Step.Cont(a) => cont(a)
      case Step.Stop(a) => stop(a)
    }
  }
  object Step {
    final case class Cont[A](value: A) extends Step[A]
    final case class Stop[A](value: A) extends Step[A]

    def cont[A](a: A): Step[A] = Cont(a)
    def stop[A](a: A): Step[A] = Stop(a)
  }

  final def apply[A](as: A*): Stream[Nothing, A] = fromIterable(as)

  /**
   * Constructs a pure stream from the specified `Iterable`.
   */
  final def fromIterable[A](it: Iterable[A]): Stream[Nothing, A] = new StreamPure[A] {
    override def fold[E >: Nothing, A1 >: A, S](s: S)(f: (S, A1) => IO[E, Step[S]]): IO[E, Step[S]] = {
      val iterator = it.iterator

      def loop(s: S): IO[E, Step[S]] =
        IO.flatten(IO.sync {
          if (iterator.hasNext) f(s, iterator.next).flatMap {
            case Step.Cont(s) => loop(s)
            case s            => IO.now(s)
          } else IO.now(Step.Cont(s))
        })

      loop(s)
    }

    override def foldPure[A1 >: A, S](s: S)(f: (S, A1) => Step[S]): Step[S] = {
      val iterator = it.iterator

      def loop(s: S): Step[S] =
        if (iterator.hasNext)
          f(s, iterator.next) match {
            case Step.Cont(s) => loop(s)
            case s            => s
          } else Step.cont(s)

      loop(s)
    }
  }

  final def fromChunk[@specialized A](c: Chunk[A]): Stream[Nothing, A] =
    new StreamPure[A] {
      override def fold[E >: Nothing, A1 >: A, S](s: S)(f: (S, A1) => IO[E, Step[S]]): IO[E, Step[S]] =
        c.foldM[E, Step[S]](Step.cont(s)) { (step, a) =>
          step.fold(f(_, a), s => IO.now(Step.stop(s)))
        }

      override def foldPure[A1 >: A, S](s: S)(f: (S, A1) => Step[S]): Step[S] = {
        def loop(i: Int, s: S): Step[S] =
          if (i >= c.length) Step.cont(s)
          else {
            val step = f(s, c(i))
            step match {
              case Step.Cont(_) => loop(i + 1, step.extract)
              case _            => step
            }
          }

        loop(0, s)
      }
    }

  /**
   * Returns the empty stream.
   */
  final val empty: Stream[Nothing, Nothing] = new StreamPure[Nothing] {
    override def fold[E >: Nothing, A1 >: Nothing, S](s: S)(f: (S, A1) => IO[E, Step[S]]): IO[E, Step[S]] =
      IO.now(Step.Cont(s))

    override def foldPure[A1 >: Nothing, S](s: S)(f: (S, A1) => Step[S]): Step[S] = Step.cont(s)
  }

  /**
   * Constructs a singleton stream.
   */
  final def point[A](a: => A): Stream[Nothing, A] = new StreamPure[A] {
    override def fold[E >: Nothing, A1 >: A, S](s: S)(f: (S, A1) => IO[E, Step[S]]): IO[E, Step[S]] =
      f(s, a)

    override def foldPure[A1 >: A, S](s: S)(f: (S, A1) => Step[S]): Step[S] = f(s, a)
  }

  /**
   * Lifts an effect producing an `A` into a stream producing that `A`.
   */
  final def lift[E, A](fa: IO[E, A]): Stream[E, A] = new Stream[E, A] {
    override def fold[E1 >: E, A1 >: A, S](s: S)(f: (S, A1) => IO[E1, Step[S]]): IO[E1, Step[S]] =
      fa.flatMap(f(s, _))
  }

  /**
   * Flattens a stream of streams into a stream, by concatenating all the
   * substreams.
   */
  final def flatten[E, A](fa: Stream[E, Stream[E, A]]): Stream[E, A] =
    fa.flatMap(identity)

  /**
   * Unwraps a stream wrapped inside of an `IO` value.
   */
  final def unwrap[E, A](stream: IO[E, Stream[E, A]]): Stream[E, A] =
    new Stream[E, A] {
      override def fold[E1 >: E, A1 >: A, S](s: S)(f: (S, A1) => IO[E1, Step[S]]): IO[E1, Step[S]] =
        stream.flatMap(_.fold[E1, A1, S](s)(f))
    }

  /**
   * Constructs a stream from a resource that must be acquired and released.
   */
  final def bracket[E, A, B](
    acquire: IO[E, A]
  )(release: A => IO[Nothing, Unit])(read: A => IO[E, Option[B]]): Stream[E, B] =
    managed(Managed(acquire)(release))(read)

  final def managed[E, A, B](m: Managed[E, A])(read: A => IO[E, Option[B]]) =
    new Stream[E, B] {
      override def fold[E1 >: E, B1 >: B, S](s: S)(f: (S, B1) => IO[E1, Step[S]]): IO[E1, Step[S]] =
        m use { a =>
          read(a).flatMap {
            case None    => IO.now(Step.Stop(s))
            case Some(b) => f(s, b)
          }
        }
    }

  /**
   * Constructs an infinite stream from a `Queue`.
   */
  final def fromQueue[A](queue: Queue[A]): Stream[Nothing, A] =
    unfoldM(())(_ => queue.take.map(a => Some((a, ()))))

  /**
   * Constructs a stream from effectful state. This method should not be used
   * for resources that require safe release. See `Stream.fromResource`.
   */
  final def unfoldM[S, E, A](s: S)(f0: S => IO[E, Option[(A, S)]]): Stream[E, A] =
    new Stream[E, A] {
      override def fold[E1 >: E, A1 >: A, S2](s2: S2)(f: (S2, A1) => IO[E1, Step[S2]]): IO[E1, Step[S2]] = {
        def loop(s: S, s2: S2): IO[E1, Step[(S, S2)]] =
          f0(s).flatMap {
            case None         => IO.now(Step.Stop((s, s2)))
            case Some((a, s)) => f(s2, a).map(_.map(s2 => (s, s2)))
          }

        loop(s, s2).map(_.map(_._2))
      }
    }

  /**
   * Constructs a stream from state.
   */
  final def unfold[S, A](s: S)(f0: S => Option[(A, S)]): Stream[Nothing, A] =
    new StreamPure[A] {
      override def fold[E, A1 >: A, S2](s2: S2)(f: (S2, A1) => IO[E, Step[S2]]): IO[E, Step[S2]] = {
        def loop(s: S, s2: S2): IO[E, Step[(S, S2)]] =
          f0(s) match {
            case None         => IO.now(Step.Stop((s, s2)))
            case Some((a, s)) => f(s2, a).map(_.map(s2 => (s, s2)))
          }

        loop(s, s2).map(_.map(_._2))
      }

      override def foldPure[A1 >: A, S2](s2: S2)(f: (S2, A1) => Step[S2]): Step[S2] = {
        def loop(s: S, s2: S2): Step[(S, S2)] =
          f0(s) match {
            case None         => Step.Stop((s, s2))
            case Some((a, s)) => f(s2, a).map(s2 => (s, s2))
          }

        loop(s, s2).map(_._2)
      }
    }

  /**
   * Constructs a stream from a range of integers (inclusive).
   */
  final def range(min: Int, max: Int): Stream[Nothing, Int] =
    unfold[Int, Int](min)(cur => if (cur > max) None else Some((cur, cur + 1)))
}
