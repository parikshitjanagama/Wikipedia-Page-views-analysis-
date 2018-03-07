# Wikipedia-Page-views-analysis


##########################################################################################
Performed sequential and parallel analysis on the Wikipedia page-view logs to analyze page-view trends and derive the total average page  views per day, top trending topics etc.

##########################################################################################
In this project I have used wikipedia page view API to download the real time data from wikipedia this data was in JSON format I have extarcted the data that I require from this data such as Page name, Num of views and Time stamp.

https://wikitech.wikimedia.org/wiki/Analytics/AQS/Pageviews


##########################################################################################
I have donwloaded the data of more than 30 pages and I was interested in analysis of this data I have used map reduce technique to perform analysis on data and have done more than 6 different types of analysis on the data and have found out the TOP and LAST N page views , Avergae N views and Total views of these pages in a span of 100 days and also Views on a particular day for a particular page 


##########################################################################################

To RUN -- open App.java and uncomment the CreateBigData.main and run the code it will create the bigdata based on the default articles and 
dates given in the CreateBigData.java file. you can change the article and date range by modifying the prepURL method.
          Now in the App.java uncomment each of those based on what you need and the code will output the desired results.
          

