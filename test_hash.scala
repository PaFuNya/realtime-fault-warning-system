val keys = Seq("101", "102", "103", "104", "105", "112", "114", "115", "116", "117", "118")
for (key <- keys) {
  val machineHash = math.abs(key.hashCode % 100) / 100.0
  val machineHash2 = math.abs(key.hashCode % 10) / 10.0
  val machineHash3 = math.abs(key.hashCode % 1000) / 1000.0
  println(s"Key: $key, Hash1: $machineHash, Hash2: $machineHash2, Hash3: $machineHash3")
}
