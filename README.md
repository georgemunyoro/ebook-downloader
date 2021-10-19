# ebook-downloader

## Getting Started
0. Open a terminal and run the following commands.
1. `git clone github.com/georgemunyoro/ebook-downloader`
2. `cd ./ebook-downloader`
3. `python -m venv venv`
4. For Windows: `.\venv\Scripts\activate`, and for GNU/Linux or MacOS: `source ./venv/bin/activate`
5. `pip install -r requirements.txt`
6. Fill out necessary sections in the `.env` file, ensuring that the directory you set for the downloads to be placed in has sufficient storage capacity.
7. Ensure credential file for google API is present in working directory
8. Run the script: `python ./main.py`

### Things to note:
- While using more than one thread is recommended and is much faster, it can result in less successful book downloads due to network concurrency issues. This is simply remedied by simply repeatedly running the script.
- This is not an AI based program, and so sometimes it will make silly mistakes and either download the wrong book, or fail to find the book even if it is available. But you can rest assured knowing that more often than not your books will be found.
- Books will be downloaded in whatever format they are found in, no conversion is done to reduce load on CPU and unsupervised conversion can lead to major quality loss and often times an unusable or illegible output. This is due to the different natures of different formats.
