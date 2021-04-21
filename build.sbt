name := "CSYE7200_FinalProj_Team3"

version := "0.1"

scalaVersion := "2.11.8"

lazy val DataClean = project in file("DataClean")

lazy val DataLoad = project in file("DataLoad")

lazy val Statistics = project in file("Statistics")

lazy val AlsRecommendation = project in file("AlsRecommendation")

lazy val root = (project in file(".")).aggregate(DataClean, DataLoad, Statistics, AlsRecommendation)