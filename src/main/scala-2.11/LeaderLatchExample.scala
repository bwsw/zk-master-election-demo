import java.io.{Closeable, EOFException, IOException}

import com.google.common.base.Preconditions
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.{InterProcessMutex, LockInternals, LockInternalsSorter, StandardLockInternalsDriver}
import org.apache.curator.framework.state.{ConnectionState, ConnectionStateListener}
import org.apache.curator.utils.{PathUtils, ThreadUtils, ZKPaths}
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

import com.google.common.annotations.VisibleForTesting
import org.apache.curator.framework.api.{BackgroundCallback, CuratorEvent}
import org.apache.curator.framework.listen.ListenerContainer
import org.apache.curator.framework.recipes.AfterConnectionEstablished
import org.apache.curator.framework.recipes.leader.{LeaderLatchListener, LeaderSelector, LeaderSelectorListener, Participant}
import org.apache.zookeeper.{CreateMode, KeeperException, WatchedEvent, Watcher}

class LeaderLatchExample()
  extends Closeable
{
  import LeaderLatchExample._

  private val log: Logger = LoggerFactory.getLogger(getClass)
  private var client: CuratorFramework = _
  private var latchPath: String = _
  private var id: String = _
  private val state: AtomicReference[LeaderLatchExample.State.Value] = new AtomicReference[LeaderLatchExample.State.Value](State.LATENT)
  private val hasLeadership: AtomicBoolean = new AtomicBoolean(false)
  private val ourPath: AtomicReference[String] = new AtomicReference[String]
  private val listeners: ListenerContainer[LeaderLatchListener] = new ListenerContainer[LeaderLatchListener]
  private var closeMode: LeaderLatchExample.CloseMode.Value = CloseMode.SILENT
  private val startTask: AtomicReference[Future[_]] = new AtomicReference[Future[_]]

  private val listener: ConnectionStateListener = new ConnectionStateListener() {
    def stateChanged(client: CuratorFramework, newState: ConnectionState) {
      handleStateChange(newState)
    }
  }

  private val LOCK_NAME: String = "latch-"

  private val sorter: LockInternalsSorter = new LockInternalsSorter() {
    def fixForSorting(str: String, lockName: String): String = StandardLockInternalsDriver.standardFixForSorting(str, lockName)
  }

  def this(client: CuratorFramework, latchPath: String) {
    this()
    this.id = ""
    this.closeMode = CloseMode.SILENT
  }

  def this(client: CuratorFramework, latchPath: String, id: String) {
    this()
    this.id = id
    this.closeMode = CloseMode.SILENT
  }

  def this(client: CuratorFramework, latchPath: String, id: String, closeMode: LeaderLatchExample.CloseMode.Value) {
    this()
    this.client = Preconditions.checkNotNull(client, "client cannot be null")
    this.latchPath = PathUtils.validatePath(latchPath)
    this.id = Preconditions.checkNotNull(id, "id cannot be null")
    this.closeMode = Preconditions.checkNotNull(closeMode, "closeMode cannot be null")
  }

  @throws[Exception] def start() {
    Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once")
    startTask.set(AfterConnectionEstablished.execute(client, new Runnable() {
      def run() {
        try
          internalStart()
        finally startTask.set(null)
      }
    }))
  }

  @throws[IOException] def close() {
    close(closeMode)
  }

  @throws[IOException] def close(closeMode: CloseMode.Value) {
    Preconditions.checkState(state.compareAndSet(State.STARTED, State.CLOSED), "Already closed or has not been started")
    Preconditions.checkNotNull(closeMode, "closeMode cannot be null")
    cancelStartTask
    try
      setNode(null)

    catch {
      case e: Exception => {
        ThreadUtils.checkInterrupted(e)
        throw new IOException(e)
      }
    } finally {
      client.getConnectionStateListenable.removeListener(listener)

      import LeaderLatchExample.CloseMode._
      closeMode match {
        case NOTIFY_LEADER => {
          setLeadership(false)
          listeners.clear()
        //  break //todo: break is not supported
        }
        case _ => {
          listeners.clear()
          setLeadership(false)
        //  break //todo: break is not supported
        }
      }
    }
  }

  @VisibleForTesting protected def cancelStartTask: Boolean = {
    val localStartTask: Future[_] = startTask.getAndSet(null)
    if (localStartTask != null) {
      localStartTask.cancel(true)
      return true
    }
    false
  }

  def addListener(listener: LeaderLatchListener) {
    listeners.addListener(listener)
  }

  def addListener(listener: LeaderLatchListener, executor: Executor) {
    listeners.addListener(listener, executor)
  }

  def removeListener(listener: LeaderLatchListener) {
    listeners.removeListener(listener)
  }

  @throws[InterruptedException]
  @throws[EOFException]
  def await() {
    this synchronized {
      while ((state.get eq State.STARTED) && !hasLeadership.get) wait()
    }
    if (state.get ne State.STARTED) throw new EOFException
  }

  @throws[InterruptedException]
  def await(timeout: Long, unit: TimeUnit): Boolean = {
    var waitNanos: Long = TimeUnit.NANOSECONDS.convert(timeout, unit)
    this synchronized {
      while ((waitNanos > 0) && (state.get eq State.STARTED) && !hasLeadership.get) {
        val startNanos: Long = System.nanoTime
        TimeUnit.NANOSECONDS.timedWait(this, waitNanos)
        val elapsed: Long = System.nanoTime - startNanos
        waitNanos -= elapsed
      }
    }
    hasLeadership
  }

  def getId: String = id
  def getState: LeaderLatchExample.State.Value = state.get

  @throws[Exception]
  def getParticipants: java.util.Collection[Participant] = {
    import org.apache.curator.framework.recipes.leader.LeaderSelector
    val participantNodes: java.util.Collection[String] = LockInternals.getParticipantNodes(client, latchPath, LOCK_NAME, sorter)
    LeaderSelector.getParticipants(client, participantNodes)
  }

  @throws[Exception]
  def getLeader: Participant = {
    import org.apache.curator.framework.recipes.leader.LeaderSelector
    val participantNodes: java.util.Collection[String] = LockInternals.getParticipantNodes(client, latchPath, LOCK_NAME, sorter)
    LeaderSelector.getLeader(client, participantNodes)
  }

  def hasLeadership: Boolean = (state.get eq State.STARTED) && hasLeadership.get

  @VisibleForTesting private[leader] var debugResetWaitLatch: CountDownLatch = _

  @VisibleForTesting
  @throws[Exception]
  private[leader] def reset() {
    setLeadership(false)
    setNode(null)
    val callback: BackgroundCallback = new BackgroundCallback() {
      @throws[Exception]
      def processResult(client: CuratorFramework, event: CuratorEvent) {
        if (debugResetWaitLatch != null) {
          debugResetWaitLatch.await()
          debugResetWaitLatch = null
        }
        if (event.getResultCode == KeeperException.Code.OK.intValue) {
          setNode(event.getName)
          if (state.get eq State.CLOSED) setNode(null)
          else getChildren()
        }
        else log.error("getChildren() failed. rc = " + event.getResultCode)
      }
    }
    client.create.creatingParentContainersIfNeeded.withProtection.withMode(CreateMode.EPHEMERAL_SEQUENTIAL).inBackground(callback).forPath(ZKPaths.makePath(latchPath, LOCK_NAME), LeaderSelector.getIdBytes(id))
  }

  private def internalStart() {
    if (state.get eq State.STARTED) {
      client.getConnectionStateListenable.addListener(listener)
      try
        reset()

      catch {
        case e: Exception => {
          ThreadUtils.checkInterrupted(e)
          log.error("An error occurred checking resetting leadership.", e)
        }
      }
    }
  }

  @throws[Exception]
  private def checkLeadership(children: java.util.List[String]) {
    val localOurPath: String = ourPath.get
    val sortedChildren: java.util.List[String] = LockInternals.getSortedChildren(LOCK_NAME, sorter, children)
    val ourIndex: Int = if (localOurPath != null) sortedChildren.indexOf(ZKPaths.getNodeFromPath(localOurPath))
    else -1
    if (ourIndex < 0) {
      log.error("Can't find our node. Resetting. Index: " + ourIndex)
      reset()
    }
    else if (ourIndex == 0) setLeadership(true)
    else {
      val watchPath: String = sortedChildren.get(ourIndex - 1)
      val watcher: Watcher = new Watcher() {
        def process(event: WatchedEvent) {
          if ((state.get eq State.STARTED) && (event.getType eq Watcher.Event.EventType.NodeDeleted) && (localOurPath != null)) try
            getChildren()

          catch {
            case ex: Exception => {
              ThreadUtils.checkInterrupted(ex)
              log.error("An error occurred checking the leadership.", ex)
            }
          }
        }
      }
      val callback: BackgroundCallback = new BackgroundCallback() {
        @throws[Exception] def processResult(client: CuratorFramework, event: CuratorEvent) {
          if (event.getResultCode == KeeperException.Code.NONODE.intValue) {
            // previous node is gone - reset
            reset()
          }
        }
      }
      // use getData() instead of exists() to avoid leaving unneeded watchers which is a type of resource leak
      client.getData.usingWatcher(watcher).inBackground(callback).forPath(ZKPaths.makePath(latchPath, watchPath))
    }
  }

  @throws[Exception] private def getChildren() {
    val callback: BackgroundCallback = new BackgroundCallback() {
      @throws[Exception] def processResult(client: CuratorFramework, event: CuratorEvent) {
        if (event.getResultCode == KeeperException.Code.OK.intValue) checkLeadership(event.getChildren)
      }
    }
    client.getChildren.inBackground(callback).forPath(ZKPaths.makePath(latchPath, null))
  }

  private def handleStateChange(newState: ConnectionState) {
    import ConnectionState._
    newState match {
      case RECONNECTED => {
        try
          reset()
        catch {
          case e: Exception => {
            ThreadUtils.checkInterrupted(e)
            log.error("Could not reset leader latch", e)
            setLeadership(false)
          }
        }
      }
      case SUSPENDED =>
      case LOST => {
        setLeadership(false)
      }
      case _ => {
      }
    }
  }

  private def setLeadership(newValue: Boolean) { ???
//    import
//    val oldValue: Boolean = hasLeadership.getAndSet(newValue)
//    if (oldValue && !newValue) {
//      // Lost leadership, was true, now false
//      listeners.forEach(new Function[LeaderLatchListener, Void]() {
//        def apply(listener: LeaderLatchListener): Void = {
//          listener.notLeader()
//          null
//        }
//      })
//    }
//    else if (!oldValue && newValue) {
//      // Gained leadership, was false, now true
//      listeners.forEach(new Function[LeaderLatchListener, Void]() {
//        def apply(input: LeaderLatchListener): Void = {
//          input.isLeader()
//          null
//        }
//      })
//    }
//    notifyAll()
  }

  @throws[Exception] private def setNode(newValue: String) {
    val oldPath: String = ourPath.getAndSet(newValue)
    if (oldPath != null) client.delete.guaranteed.inBackground.forPath(oldPath)
  }



}

object LeaderLatchExample {

  object State extends Enumeration {
    val LATENT, STARTED, CLOSED = Value
  }


  object CloseMode extends Enumeration {
    val SILENT, NOTIFY_LEADER = Value
  }

}
