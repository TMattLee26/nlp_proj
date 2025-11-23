# Data Files:
* r_news_2018.jsonl - All Reddit Posts from r/news in 2018
* All the News DataSet - https://www.kaggle.com/datasets/davidmckinley/all-the-news-dataset/data

 # Install and Use the Conda Enviornment
conda env create -f nlp_proj.yml

# Run Data Fetcher in Background
```
tmux
conda activate nlp_proj
<CMD to Run>
CTRL+B | d # Detaches tmux session but leaves cmd running
tmux list-sessions # Shows detached session
tmux attach <Session Number> # Reattaches session to see cmd status
```

# Next Steps
- Cluster Reddit Posts w/ News Articles based on Word2Vec of Article Names
  and/or Dates
- Train Llama Model to perform summarization of News Articles
- Develop and Employ Ranking Algorithm that uses Reddit UpVote Score to value
  News Articles
- Download Reddit Data for before 2018 (say 2012-2017) and use the NEWSROOM Dataset instead (Includes Summaries which could be better
for LLM)
  - Modify Reddit Data Pull Script
  - ChatGPT Prompt: Write a script to download all NEWSROOM articles from 2012-2017 and save to a file to be handled. I want this script to be optimized to not run for a very long time. Can you save the buffer as you go so if the program crashes, not all progress is lost. I do not want to have to stream the whole dataset just articles from the data range I defined above.
