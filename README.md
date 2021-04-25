# CSYE7200_FinalProj_Team3
### Movie Recommendation

Inspired by a [Kaggle Competition](https://www.kaggle.com/grouplens/movielens-20m-dataset), this project will be based on Spark to recommend movie to user. We ingest user's rating data into a data(ML) model, then the prediction result will be showed in web page which is built by Play Framework. The whole project will be writed in scala.

### Teammates

Mengzhe ZHANG (@Mengzhe-Madeline-Zhang)

Rongqi SUN (@Svelar)

Yue FANG (@tracy626)

### Instruction

1. Start MongoDB services.
2. (Optional) Run 'DataClean/src/main/scala/DataClean.scala' to clean dirty data.
3. Run 'DataLoad/src/main/scala/DataLoad.scala' to load data from csv files to MongoDB.
4. Run 'Statistics/src/main/Scala/Statistics.scala' to create a table of statistical recommendation.
5. Run 'AlsRecommendation/src/main/Scala/AlsOfflineRecommend.scala' to create table a table of user recommendation.
6. Run Play for GUI (`sbt run` in Play directory).
(Detail of UI please check the screenshot video https://youtu.be/OhQoG5l7vFc)

