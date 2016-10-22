import java.io.{BufferedReader, InputStreamReader}
import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.utils.ThreadUtils

import scala.collection.mutable.ArrayBuffer

object MyLeaderSelector extends App {
  private val CLIENT_QTY: Int = 1
  private val PATH: String = "/examples/leader4"
  // all of the useful sample code is in ExampleClient.java
  System.out.println("Create " + CLIENT_QTY + " clients, have each negotiate for leadership and then wait a random number of seconds before letting another leader election occur.")
  System.out.println("Notice that leader election is fair: all clients will become leader and will do so the same number of times.")


  val clients: ArrayBuffer[CuratorFramework] = ArrayBuffer()
  val examples: ArrayBuffer[MyLeaderSelectorClient] = ArrayBuffer()
  val connectionString = "192.168.99.100:32770"
  try {
    (0 to CLIENT_QTY) foreach { i =>
      val client = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(100, 2))
      val example = new MyLeaderSelectorClient(client, PATH, "Client #" + i)

      clients += client
      examples += example

      client.start()
      example.start()
    }
  }


  val currentLeader = examples.head.getLeader.getId

  val exampleOldLeaderIndex = examples.indexWhere(_.id == currentLeader)
  val exampleNewLeaderIndex = if (exampleOldLeaderIndex == 1) 0 else 1

  examples foreach(example=> println(example.getLeader))
  println()

  examples(exampleOldLeaderIndex).release()

  examples foreach(example=> println(example.getLeader))
  println()

  examples(exampleNewLeaderIndex).release()

  examples(exampleOldLeaderIndex).requeue()

  examples foreach(example=> println(example.getLeader))
  println()

 // examples(1).release()

  //examples(0).requeue

//  println()
//  examples foreach(example=> println(example.getLeader))

//  examples(2).release()
//  Thread.sleep(300)

//  examples foreach(example=> println(example.leaderSelector.getLeader))
//  println()

  //MyLeaderSelectorClient.resettableCountDownLatch.countDown()

//  examples(3).release()
//  Thread.sleep(300)
//
//  examples foreach(example=> println(example.leaderSelector.getLeader))
//  println()

  clients foreach (_.close())
  System.out.println(s"Press enter/return to quit\n")
  new BufferedReader(new InputStreamReader(System.in)).readLine()



//  val master = examples(scala.util.Random.nextInt(examples.length-1)).leaderSelector
//  println(master.requeue())
//  examples(0).leaderSelector.requeue()
//  println(master.getLeader)
 // examples foreach (x=> println(x.leaderSelector.getParticipants()))
 // examples foreach (x=> x.leaderSelector.close())
//  clients  foreach (x=> x.close())
//  val clientNew = CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(100, 2))
//  val exampleNew = new MyLeaderSelectorClient(clientNew, PATH, "Client #" + 12,serivce)
//  clientNew.start()
//  clients += clientNew
//  exampleNew.start()
//  examples += exampleNew
//
}

