package controllers

import play.api.mvc._
import org.squeryl.{Session, SessionFactory}
import org.squeryl.dsl.QueryDsl
import play.api.libs.iteratee._

import play.api.libs.concurrent.Execution.Implicits._

object DistributedAction {
  /**
   * Convenience method which delegates to apply
   */
  def DistributedAction(action: DistributedSession => EssentialAction) = apply(action)

  /**
   * Create a new Action, which accepts a DistributedSession
   */
  def apply(action: DistributedSession => EssentialAction) = {
    /*
     * The action itself. Rather than just returning the response,
     * it handles it. If it's a PlainResponse, it cleans up and returns
     * it. If it's an AsyncResponse, it adds a cleanup phase with map.
     */
    EssentialAction {request =>
      val session = if (Session.hasCurrentSession) {
        Session.currentSession
      } else {
        SessionFactory.newSession
      }

      val distSession = new DistributedSession(session)
      val innerAction = action(distSession)
      try {
        val responseIteratee = innerAction(request)

        def closeIteratee[A,B](it: Iteratee[A,B]): Iteratee[A,B] = it.pureFlatFold {
          case Step.Done(a, in) =>
            distSession.close()
            Done(a, in)
          case Step.Cont(cont) =>
            Cont((in: Input[A]) => closeIteratee(cont(in)))
          case Step.Error(err, input) =>
            distSession.abort(err)
            Error(err, input)
        }

        def closeEnumerator[A](en: Enumerator[A]) = {
          new Enumerator[A] {
            override def apply[B](it: Iteratee[A,B]) = {
              en(closeIteratee(it))
            }
          }
        }

        def closeResponse(r: Result): Result = {
          r match {
            case res @ SimpleResult(header, body) =>
              val newBody = closeEnumerator(body)
              SimpleResult(header, newBody)(res.writeable)
            case res @ ChunkedResult(header, chunks) =>
              type A = res.BODY_CONTENT
              def newChunks(it: Iteratee[A, Unit]) = chunks(closeIteratee(it))
              ChunkedResult(header, newChunks)(res.writeable)
            case res: PlainResult =>
              distSession.close()
              res
            case asResp: AsyncResult =>
              val promise = asResp.result
              AsyncResult(promise.transform ({resp =>
                  closeResponse(resp)
                }, {ex =>
                  distSession.closeWithException(ex)
                })
              )
          }
        }

        responseIteratee.map(closeResponse)

      } catch {
        case e: Throwable =>
          throw distSession.closeWithException(e)
      }
    }
  }

  /**
   * Convenience function, which borrows a transaction from an implicit DistributedSession
   */
  def borrowTransaction[A](f: => A)(implicit dsl: QueryDsl, session: DistributedSession) = {
    session.borrowTransaction(f)
  }

	/**
	 * A class to keep track of everything needed for a distributed transaction.
	 */
}
class DistributedSession(private[this] var _session: Session) {
	  var exception: Option[Throwable] = None
	  private val lock = new Object
	  private var closed = false

	  def conn = session.connection

	  def session = _session

	  conn.setAutoCommit(false)

	  def close() = {
	    lock.synchronized {
	      if (!closed) {
	        closed = true
		    exception match {
		      case None =>
		        must(conn.commit())
		        must(session.close)
		        _session = null
		      case Some(ex) =>
		        must(conn.rollback())
		        must(session.close)
		        _session = null
		        throw ex
		    }
	        play.Logger.debug("Closed distributed transaction")
	      }
	    }
	  }

	  def closeWithException(t: Throwable) = {
	    lock.synchronized {
	      if (!closed) {
	        closed = true
	        exception = Some(t)
		    must(conn.rollback())
		    must(session.close)
		    _session = null
		    play.Logger.debug("Closed distributed transaction")
	      }
	      t
		}
	  }

	  private[this] def must[A](f: => A) = {
	    try {
	      Some(f)
	    } catch {
	      case t: Throwable =>
	        try {
	          play.Logger.error("An exception was thrown during a \"must\" block", t)
	        } catch {
	          case _: Throwable => // Do nothing: we are beyond hope
	        }
	        None
	    }
	  }

	  private[this] def innerAbort(message: String = "Aborted") = {
	    if (!closed) {
	        closed = true
	        exception = Some(new Exception(message))
	        must(conn.rollback())
	        must(session.close)
	        _session = null
		    true
	     } else {
	       false
	     }
	  }

	  def abort(message: String = "Aborted") = {
	    lock.synchronized {
	      if (innerAbort(message)) {
	        play.Logger.debug("Closed distributed transaction")
	      }
	    }
	  }

	  def borrowTransaction[A](f: => A)(implicit dsl: QueryDsl) = {
	    lock.synchronized {
	      (exception, closed) match {
	        case (None, false) =>
	          try {
	            dsl.using(session)(f)
	          } catch {
	            case e: Throwable =>
	              exception = Some(e)
	              throw e
	          }
	      case (Some(e), false) =>
	        throw e
	      case (_, true) =>
	        throw new Exception("Session already closed")
	      }
	    }
	  }

	protected override def finalize() {
	  /*
	   * Eww, a finalizer. Let us hope we never need it. Might want to look at
	   * replacing with a PhantomReference based solution in future, if memory
	   * churn becomes an issue.
	   */
	  if (innerAbort()) {
	    System.err.println(
"""!!!!WARNING!!!!
Garbage collection cleaned up a DistributedSession.
This should never happen.
This probably means you have a session leak.
It may also mean that user transactions did not complete correctly.""")
	  }
	  super.finalize()
	}
}

