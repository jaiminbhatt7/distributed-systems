import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.ActorContext
import scala.math._

object project extends App {
  override def main(args: Array[String]): Unit = {
    val system = ActorSystem("Lucas")
      // default Actor constructor
    val a :Long= args(0).toLong
      val b :Long= args(1).toLong
      val supervisor = system.actorOf(Props[Supervisor],"s1")
      //workers(15) ! "hello"
      try{ 
      supervisor ! (a+1,b)}

      catch
      {
        case e: Exception => e.printStackTrace();
      }
      //supervisor ! "skmd"
    
  }
}
  
class Worker extends Actor {
  def receive = {
    case (c:Long,d:Long,tSize:Long) => 
      for(i <- c until (c+tSize))
      {
        var (m,n) = checkSquareSum(i,d)
        if(m == 1)
        {
          //println((n,p,q))
          sender ! n
        }
        else
        {
          sender ! "not found"
        }

      }


    case _       => println("huh?")
  }

  def checkSquareSum(ll:Long, ul:Long) : (Int,Long) = //,Long,Double) =
  {
    var i: Long = 0L
    var sum :Long = 0L
    var bulean: Int = 0
    for(i<- ll until (ll+ul))
    {
        sum = sum + ((i*i).toLong) 
    }
    //println(sqrt(sum))
    if(sqrt(sum) % 1 == 0)
    {
      bulean = 1
    }
    //println(bulean,ll)
    (bulean,ll)  //,sum,sqrt(sum))
  }

}

class Supervisor extends Actor{
  var count:Long = 0L
  var aCopy:Long = 0L
	def receive = {

    case (ans: Long) =>
      count  = count + 15
      println(ans)
      //println(count)
      if(count >= aCopy-2){
        context.system.shutdown()
      }

    case (s: String) =>
      count  = count + 1
      //println(count)
      if(count >= aCopy-2){
        context.system.shutdown()
      }

    

		case (a:Long ,b:Long) => 
      var wrkercount: Long = 12L//*Runtime.getRuntime.availableProcessors).toLong
      var workers = Array(context.actorOf(Props[Worker]))
      var i: Long = 0L
      aCopy = a
      for(i <- 0L until wrkercount)
        {
          workers +:= context.actorOf(Props[Worker],"node"+i.toString)
        }
        
        println("I'm repeating")
      i = 1L
      var k = 0
      var chunkSize: Long = 300L //(a/wrkercount).toLong
      while(i < a)
      {
     
        if(chunkSize > 0 && (a-i) > chunkSize)
        {
          var t1 = i.toInt
          var t2 = wrkercount.toInt
          workers(k % t2) ! (i,b,chunkSize)
	  k = (k +1) % t2
          i = i + chunkSize
          //println(i)
        }
        else
        {  
          var t3 = i.toInt
          var t4 = wrkercount.toInt
          workers(k%t4) ! (i,b,a-i)
          //workers(i%wrkercount) ! (i,b,chunkSize)
          i = i+a
          //println(i+a-i)
        }        
      }
    
      println("I received"+a+" and "+b)

    case _ => println("sdf")

	}
  
}

