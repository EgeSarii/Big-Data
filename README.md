# Big-Data


## What is this repository about?
It is about my development in data engineering, especially analysis on Big Data. It has some assignments from a course. It helped me to understand data engineering techniques by making practice.

At the end of the course, I have made a project about data analysis of WebCrawl data. It is a standalone Scala app, built on Apache Spark. At first, I wanted to show the
correlation between the words “Ukraine” and “energy”. This means I want to show the frequency of the words “Ukraine” and “energy” occurring on the same web page between the
timeline February 2022 and May 2022. My expectation for this analysis is to show there is a correlation between these words and also I was wondering in which month the frequency is
the highest. But unfortunately, I couldn’t perform this task because the Web Archive in the cluster includes only the timeline until April 2021 (and some of the days of April).

After that, I changed my experiment. My new goal was to find the frequency of “bitcoin” in 2020 month by month. But this time, I was not planning to look at the body of the webpage,
my scope only contains the title. Also, I have limited the scope to three websites: bbc.com, nytimes.com (new york times), and wsj.com (wall street journal).

I have chosen the word bitcoin because in 2021 we have seen that it has gone to the moon and somehow it gained popularity, especially among my generation. But I don’t believe that it
happened in one night. Moreover, we know that after the declaration of pandemia in March 2020, bitcoin fell to a historical low point. I have limited my scope to only the title of the webpage, because the other way, looking at
the body of the webpage, the content would be too complex and take more time. The reason I have chosen the year 2020 is related to this, after March 2020 it went to the
bottom and in November 2020, people were talking about it will go skyrocketing. So I thought I should look for the year 2020 for the background story of 2021 skyrocketing.
I have chosen three websites because they are big websites which means I will have access to more samples. And they have a special scope for bitcoin especially the new york times
and wall street journal which means I will have access to the aimed sample.

You can find the standalone app in this repository named **Web_Crawl_Project.scala**.
