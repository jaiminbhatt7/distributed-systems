import scala.actors.Actor
import scala.actors.Actor._
import scala.math
import scala.actors.remote.Node
import scala.actors.TIMEOUT
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer


class PastryNode(bootStrap:bootStrapper,bSize:Int) extends Actor {
  
  val routeTable = new Array[Row](bSize)
  var fLeaf = new ArrayBuffer[Int]
  
  var nPeers:Int = 127
  var myBinary:String =""
  var maxL = -1
  var minL = -1
  
  
  var peers:Int = 0
  var net = new ArrayBuffer[PastryNode]
  
  var myID:Int = 0;
  var pid:Int = 0;
  var isPeer:Int = 0;
  var netList= new ArrayBuffer[Int]
 
  
  type Row = ArrayBuffer[Int]
  type conn_type = ArrayBuffer[PastryNode]
 

  var i=0
  while (i<bSize){
    routeTable(i) = new ArrayBuffer[Int]
    i += 1
  }

  

  def flipBits(bCode: String,loc:Int): String = {
        var length = bCode.length()
        var mBin:String ="";
        var locFlag = 0
        var inc = 0
        while(inc<length){
                if(loc == inc + 1){
                        if(bCode(inc) == '1'){
                                mBin = mBin + '0'
                        } else {
                                mBin = mBin + '1'
                        }
                locFlag = 1
                } else {
                        if(locFlag == 0){
                                mBin = mBin + bCode(inc)
                        } else {
                                mBin = mBin + '0'
                        }
                }

                inc += 1
        }
        return mBin
  }
  
  def toBin(x: Int): String = {
   var b = Integer.toBinaryString(x);
          if(b.length()<bSize) {
            var diff = bSize - b.length();
            while(diff >0){
              b = '0' + b;
              diff -= 1;
            }
          }
   
        return b
  }

  

  def checkLeaf(target:Int) : Int = {
    var nearest = 1000000000;
 

         i = 0
         var diff = 1000
          while(i<4) { // num of leaf nodes
            if(fLeaf(i) == target){
              return fLeaf(i);
            } else {
                if(math.abs(target - fLeaf(i)) < diff){
                        nearest = (i)
                        diff = math.abs(target - fLeaf(i))
                }
            }
            i += 1
          }
         if (diff >= 2) {
           return -999
         } else {
           return fLeaf(nearest)
         }

    return -999
  }
  
  def FindMatchBits(bCodeOne: String,bCodeTwo: String):
Int = { //Assuming strings are of equal length
    var i:Int = 0
    var length:Int = bCodeOne.length()
    while(i<length){
      try{
      if(bCodeOne(i) == bCodeTwo(i)){
        i += 1;
      } else {
        return i
      }
      } catch{
         case e: Exception =>

      }
    }
    return i
  }

        def act(){
                 loop {
                        react{
                          case "exit" =>
                            exit()
                         case TIMEOUT =>
                         //  println("hrllo")

                         case x :(ArrayBuffer[PastryNode],Int) =>
                           myID = x._2
                           net = x._1
                         case x: (Int,ArrayBuffer[Int],String) =>
                           //println(myID)
                           pid = x._1
                           netList = x._2
                           isPeer   = 1
                            myBinary = toBin(x._1)
                          
                         case  (x: (Int,ArrayBuffer[Int],String,Int,Int)) =>// receiving Peer ID

                          if(x._3 == "Join1"){ 
                             peers = x._4
                           
                            /* Routing Table */
                             var curBit = 0
                             while(curBit<bSize){
                                 routeTable(/*curBit*/0) +=Integer.parseInt(flipBits(myBinary,(curBit + 1)),2)
                                 curBit += 1
                             }
                             /* Routing Table End */

                             /* Leaf Build */
                              nPeers = 7           // setting the total number of peers to be 8, so old join code works fine.
                                  if (pid + 1 > nPeers - 1) {
                                          fLeaf += 0;
                                          fLeaf += 1;
                                  } else if(pid + 2 > nPeers -1){
                                          fLeaf += (pid + 1);
                                          fLeaf += 0;
                                  } else {
                                          fLeaf += (pid + 1);
                                          fLeaf += (pid + 2);
                                          maxL = pid + 2;
                                  }

                                  if (pid - 1 < 0) {
                                          fLeaf += nPeers - 2;
                                          fLeaf += nPeers - 1;
                                  } else if(pid - 2 < 0){
                                          fLeaf += (pid - 1);
                                          fLeaf += nPeers -1;
                                  } else {
                                          fLeaf += (pid - 1);
                                          fLeaf += (pid - 2);
                                          minL = pid - 2;
                                  }

                                  /* fLeaf += (pid + 1);
                                RearLeaf += (pid + 2);*/

                                  /* Leaf Build  End*/


                          // BUILD BINARY (4DIGIT)
                                  sender ! "ready"
                                  bootStrap ! "acknowledged"
                          } else { // Joining node by node after default 8 nodes.(join2)
                                   //send everyone in the current peer list to update their leafset and routing table accordingly.
                                   peers = x._4
                                /* Routing Table */
                                   //println(pid+ " : Join2 for the node "+x._5)
                                   if( x._1 == x._5){// For join2 do this only if this is the new node getting added to network.
                                        //   println("received Join2 for node "+x._1)
                                           var curBit = 0
                                           while(curBit<bSize){
                                                   routeTable(/*curBit*/0) += Integer.parseInt(flipBits(myBinary,(curBit + 1)),2)
                                           curBit += 1
                                           }
                                   }
                             /* Routing Table End */

                             /* Leaf Build */
                              nPeers = peers           // setting the total number of peers to be CurrPeers, so old join code works fine.
                              fLeaf.clear
                              if (pid + 1 > nPeers - 1) {
                                          fLeaf += 0;
                                          fLeaf += 1;
                                  } else if(pid + 2 > nPeers -1){
                                          fLeaf += (pid + 1);
                                          fLeaf += 0;
                                  } else {
                                          fLeaf += (pid + 1);
                                          fLeaf += (pid + 2);
                                          maxL = pid + 2;
                                  }

                                  if (pid - 1 < 0) {
                                          fLeaf += nPeers - 2;
                                          fLeaf += nPeers - 1;
                                  } else if(pid - 2 < 0){
                                          fLeaf += (pid - 1);
                                          fLeaf += nPeers -1;
                                  } else {
                                          fLeaf += (pid - 1);
                                          fLeaf += (pid - 2);
                                          minL = pid - 2;
                                  }

                                  if( x._1 == x._5){
                                    sender ! "ready"
                                    bootStrap ! "acknowledged"
                                  }
                          }

                         case x : (Int,Int,String,String,String,Int) =>
                          // println("recevied message passing")
                                if(x._3  == "MessagePassing"){
                                //  println("I (" + myBinary + ") got the start Q")
                                  /* Check Leaf Nodes*/
                                  if (x._1 == pid) {
                                         // println("Reached destination from "+ x._6 + " in "+x._2 +" hops")
                                          bootStrap ! (x._6,x._2)
                                  } else {
                                        var nextPeer = checkLeaf(x._1)
                                        if (nextPeer != -999){ // not in leaf

                                               net(netList(nextPeer)) ! (x._1,x._2 + 1,"MessagePassing","abcd","abcd",x._6)

                                        } else {
                                          /* check routing table */

                                          var len = routeTable(0).size
                                          var i=0
                                          var flag =0
                                          var curr = (toBin(pid))
//                                          while(i<len && (FindMatchBits(curr,toBin(routeTable(0)(i))) < x._2)) {
//                                              i +=1
//                                          }
                                          i = FindMatchBits(curr,toBin(x._1))
                                                if (i >= len) {
                                                //println("Didn't " + routeTable(0)) // probably here out logic og join
                                                  
                                                } else {
                                                   net(netList(routeTable(0)(i))) ! (x._1,x._2 + 1,"MessagePassing","abcd","abcd",x._6)
                                                }
                                        }
                                  }
                                }//case ending

                        }//react ending
                 }
         }
}

object project3{
  def main(args: Array[String]) : Unit = {
    var numNodes:Double = 1024
    var numRequests:Int = 10
    if(args.length >1){
      
     numNodes = args(0).toInt
     numRequests = args(1).toInt

    }
    var x: Double = 0
    x = math.log(numNodes)/math.log(2)
    x = x.ceil
    numNodes = math.pow(2, x)
  //  println("new numNodes is " + numNodes)

   var net = new NetWork(numNodes.toInt,numRequests)
   net.start()
   net ! "ready"
  }
}


class bootStrapper(numPeers:Int,buffer:ArrayBuffer[Int],net:ArrayBuffer[PastryNode],numRequests:Int)
extends Actor{
  var readyPeers:Int = 0
  var nPeers:Int = numPeers
  var i:Int = 0
  var sum = Array[Int](numPeers+1)
  var r = new scala.util.Random
    var rgen = r.nextInt(nPeers);
  var totalmessages:Int = numRequests*(numPeers)
  var rxmessages:Int = 0
  var j:Int = 0
  sum = Array.fill(numPeers+1)(0)
  var count:Double = 0

    def act(){
        loop {
                react{
                  /*    case x: ArrayBuffer[PastryNode] =>
                        net = x
                    */
                        case "acknowledged" =>
                                readyPeers += 1
                                if(readyPeers == nPeers) {
                                        //ready to send message
                                       
                                        j = 0
                                        while(j<numRequests){
                                                i = 0
                                                while(i<nPeers){
                                                        //println(buffer(i) + " sending messages to " + rgen)
                                                        //net(buffer(i)) !
(buffer(rgen),1,"MessagePassing","MessagePassing",buffer(i))
                                                        net(buffer(i)) !
(buffer(rgen),1,"MessagePassing","abcd","abcd",i)
                                                        rgen = r.nextInt(nPeers-1)
                                                        i += 1
                                                }
                                                j +=1
                                        }
                                }
                        case x:(Int,Int) =>
                          //    println("bootStrapper received "+x._1 + "  " + x._2 + " "+rxmessages + " " + totalmessages+ " "+ i)
                                sum(x._1) = sum(x._1) + x._2
                                rxmessages += 1

                                if(rxmessages == totalmessages){
                                  j = 0
                                  while(j<nPeers+1){
                                    count = count + sum(j)
                                    j = j+1
                                  }

                                 // println("$$$$$ get the program closed and exit..." + count)
                                  println("Average number of hops "+ count/((nPeers+1)*numRequests))
                                  j= 0
                                  while(j<nPeers){
                                    net(buffer(j)) ! "exit"
                                    j += 1
                                  }
                                  exit()

                                }
                }
                }
        }
}


class NetWork(numNodes1:Int,numRequests:Int) extends Actor {
        //val numNodes = 127
        var numNodes = numNodes1-1
        var numPeers = numNodes  //Change number of bits from here
        var k:Int = numNodes-1     // for intitalizing our array  always -2
        val bSize = (math.log(numPeers + 1)/math.log(2)).toInt
        var peers:Int = 0
        println("The size " + bSize)
    var list = new ArrayBuffer[Int]()
    var buffer = new ArrayBuffer[Int]()
    var m:Int = 0
    for(i <- 0 to k){
      list += i
    }


  //  var numNodesCount:Int = 126
    var r = new scala.util.Random
    var rgen = r.nextInt(k);

    var range = 0 to (k);
        while(k >= 0){
                if (k != 0){
                        rgen = r.nextInt(k)
                } else {
                        rgen = 0
                }
                buffer += list(rgen)
                list.remove(rgen)
                k -= 1
    }
    //println(buffer)
    //bufferff now has list of random nos
    var net = new ArrayBuffer[PastryNode](numNodes)

     // val numRequests = 10
      k = 0

       val instigator = new bootStrapper(numPeers,buffer,net,numRequests)
       instigator.start()

      // initialize all the connections (actors)
      while(k<numNodes){
         var newNode:PastryNode = null
         newNode = new PastryNode(instigator,bSize)
         newNode.start()
         net += newNode
         k += 1
      }
      instigator ! net
      k = 0
      while(k<numNodes){
           net(k)!(net,k) //sending node Id
           k += 1
      }
      k =0
   //  self ! "ready"
   //  println("sending_to "+ k)
    /* Now we have random numbers*/
      while(k<numNodes){
        net(buffer(k)) ! (k,buffer,"sending peer id and full rand array")
        k +=1
      }

      k=0
  def act(){
	  loop{
	    react{
	      case "ready" =>
                if (k<8) {
                  
                    net(buffer(k)) ! (k,buffer,"Join1",peers,peers) 
                    k += 1
                    peers += 1
                } else if (k<numNodes) { 
                    rgen = r.nextInt(k-1)
                    
                    m = 0
                    while(m<=peers){
                            net(buffer(m)) ! (m,buffer,"Join2",peers,k) // we need to change this message to join so that apart from the calculations it is doing noe, it also does join.
                            m +=1
                    }
                    k += 1
                    peers += 1
                }
                if(k == numNodes){
                	exit()
                }
	     	}
	   	}
	}
}