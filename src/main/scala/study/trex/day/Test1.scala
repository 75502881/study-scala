package study.trex.day

class Man(val name: String)
class Superman(val name: String) {
  def emitLaser = println("emit a laster!")

  implicit def man2superman(man: Man): Superman = new Superman(man.name)

}

object Test3 {
  def main(args: Array[String]) {

    var man = new Man("test man")
 
  }

}

