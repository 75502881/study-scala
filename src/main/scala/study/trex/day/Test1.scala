package study.trex.day
object Test {
  class Man(val name: String)
  class Superman(val name: String) {
    def emitLaser = println("emit a laster!")

  }
  implicit def man2superman(man: Man): Superman = new Superman(man.name)

  def main(args: Array[String]) {
    var man = new Test.Man("test man")
    man.emitLaser
  }

}
