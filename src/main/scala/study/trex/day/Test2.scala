package study.trex.day

class Man(val name: String)
class Superman(val name: String) {
  def emitLaser = println(name +" emit a laster!")

}

object TestImp {
  implicit def man2superman(man: Man): Superman = new Superman(man.name)

}
