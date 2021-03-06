package scalaz.zio

import org.specs2.Specification
import scalaz.zio.ExitResult.Cause

trait AbstractRTSSpec extends Specification with RTS {
  override def defaultHandler: Cause[Any] => IO[Nothing, Unit] = _ => IO.unit
}
