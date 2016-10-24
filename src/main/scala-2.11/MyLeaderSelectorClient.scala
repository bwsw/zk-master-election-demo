import java.io.{Closeable, IOException}
import java.util.concurrent.{ExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatchListener, LeaderSelector, LeaderSelectorListenerAdapter, Participant}
import org.apache.zookeeper.KeeperException.NoNodeException

import scala.annotation.tailrec
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global


// create a leader selector using the given path for management
// all participants in a given leader selection must use the same path
// ExampleClient here is also a LeaderSelectorListener but this isn't required
class MyLeaderSelectorClient(client: CuratorFramework,val path: String,val id: String) extends
  LeaderSelectorListenerAdapter with Closeable {
  private val leaderSelector: LeaderSelector =  new LeaderSelector(client, path, this)
  private val resettableCountDownLatch = new ResettableCountDownLatch(1)
  @volatile private var IS_STARTED = false

  override def equals(other: Any): Boolean = other match {
    case that: MyLeaderSelectorClient =>
      path == that.path &&
      id == that.id
    case _ => false
  }

  leaderSelector.setId(id)

  val leaderCount: AtomicInteger = new AtomicInteger

  @throws[IOException]
  def start() = {
    // the selection for this instance doesn't start until the leader selector is started
    // leader selection is done in the background so this call to leaderSelector.start() returns immediately
    if (IS_STARTED) leaderSelector.requeue() else {leaderSelector.start(); IS_STARTED = true}
  }

  def requeue() = start()

  def isStarted = IS_STARTED

  def hasLeadership: Boolean = getLeader.getId == id
  def hasNotLeadership: Boolean = !hasLeadership

  final def getLeader: Participant = {
    import org.apache.zookeeper.KeeperException.NoNodeException
    def timeToSleep = TimeUnit.MILLISECONDS.sleep(30L)
    @tailrec
    def helper(times: Int): Participant = {
      val tryGetLeader = scala.util.Try(leaderSelector.getLeader)
      tryGetLeader match {
        case Success(leader) => if (leader.getId == "") {
          timeToSleep
          helper(times-1)
        } else {
          leader
        }
        case Failure(error) => error match {
          case noNode: NoNodeException => {
            timeToSleep
            helper(times - 1)
          }
          case _ => throw error
        }
      }
    }
    helper(20)
  }

  def release() = {
    def timeToSleep = TimeUnit.MILLISECONDS.sleep(30L)
    //Thread sleep makes a differ ,otherwise it shouldn't work properly
    resettableCountDownLatch.countDown()
    timeToSleep
    resettableCountDownLatch.reset
  }

  @throws[IOException]
  def close() {
    scala.util.Try(leaderSelector.close()) match {
      case Success(_) => ()
      case Failure(error) => error match {
        case _: IllegalMonitorStateException => println("Need to update zookeeper client version to 2.11.1")
      }
    }
  }

  @throws[Exception]
  def takeLeadership(client: CuratorFramework) {resettableCountDownLatch.await()}
}
