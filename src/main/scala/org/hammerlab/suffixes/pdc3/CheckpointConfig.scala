package org.hammerlab.suffixes.pdc3

class CheckpointConfig(dirOpt: Option[String] = None,
                       whitelist: Set[String] = Set(),
                       blacklist: Set[String] = Set(),
                       val compressBackups: Set[String] = Set(),
                       writeClasses: Set[String] = Set()) {
  def backupPathOpt(name: String) =
    if ((blacklist.isEmpty || !blacklist(name)) && (whitelist.isEmpty || whitelist(name)))
      dirOpt
    else
      None
}
