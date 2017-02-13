name := "suffix-arrays"
version := "1.0.0"

addSparkDeps

deps ++= Seq(
  libs.value('iterators),
  libs.value('magic_rdds)
)
