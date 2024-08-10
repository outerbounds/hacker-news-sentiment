from metaflow import (
    FlowSpec,
    IncludeFile,
    step,
    conda,
    project,
    Flow,
    S3,
    Parameter,
    current,
    retry,
    card,
)
from metaflow.cards import Markdown, ProgressBar
from metaflow import nim, namespace
import tarfile, os, tempfile, shutil, re

# Flow 5
# Analyze sentiment of post comments using an LLM
#
# python hnsentiment.py --environment=conda run --with kubernetes --max-workers 5
#
# The same comments as for hnposts.py apply here:
# 1. --max-workers controls the concurrency sent to your LLM backend. Going
#    higher than what the backend can handle is not useful.
# 2. Depending on the number of posts to analyze and your LLM backend, this
#    flow is likely to take 5-10h to run. It makes sense to deploy to Argo
#    Workflows to keep the run running reliably
#
# After this, you have the datasets ready and you can analyze them
# in a notebook!

PROMPT = """In the scale between 0-10 where 0 is the most negative sentiment and 10 is the most positive sentiment,
rank the following discussion. Reply in this format:

SENTIMENT X

where X is the sentiment rating
"""

MODEL = "meta/llama3-70b-instruct"


@nim(models=[MODEL])
@project(name="hn_sentiment")
class HNSentimentAnalyzeComments(FlowSpec):

    prompt = Parameter("prompt", default=PROMPT)
    num_input_tokens = Parameter("num-input-tokens", default=3000)

    @step
    def start(self):
        # namespace(None)
        comments_run = list(Flow("HNSentimentCommentData").runs("commentresults"))[0]
        self.comments_id = comments_run.id
        print(f"Using comments from {self.comments_id}")
        self.tarballs = comments_run["end"].task["tarballs"].data
        self.next(self.analyze_comments, foreach="tarballs")

    @card(type="blank")
    @retry
    @step
    def analyze_comments(self):
        print("downloading data from", self.input)
        root = tempfile.mkdtemp("hnpost")
        with S3() as s3:
            res = s3.get(self.input)
            tarfile.open(res.path).extractall(path=root)
            datapath = os.path.join(root, "comments")
        print("Data extracted ok")
        status = Markdown("# Starting to analyze")
        progress = ProgressBar(max=len(os.listdir(datapath)), label="Docs analyzed")
        current.card.append(status)
        current.card.append(progress)
        ok = failed = 0
        self.post_sentiment = {}
        for i, post_id in enumerate(os.listdir(datapath)):
            with open(os.path.join(datapath, post_id)) as f:
                try:
                    sentiment, num_tokens = self.analyze(f.read())
                    self.post_sentiment[post_id] = (sentiment, num_tokens)
                    ok += 1
                except Exception as ex:
                    failed += 1
                    print(f"analyzing comments of post {post_id} failed: ", ex)
            status.update(f"## Successfully processed {ok} posts, failed {failed}")
            progress.update(i + 1)
            current.card.refresh()
        shutil.rmtree(root)
        self.next(self.join)

    def analyze(self, data):
        parser = re.compile("SENTIMENT (\d)")
        tokens = data.split()[: self.num_input_tokens]
        doc = " ".join(tokens)
        llm = current.nim.models[MODEL]
        prompt = {"role": "user", "content": f"{self.prompt}\n---\n{doc}"}
        chat_completion = llm(messages=[prompt], model=MODEL, n=1, max_tokens=10)
        s = chat_completion["choices"][0]["message"]["content"]
        try:
            [sentiment_str] = parser.findall(s)
            sentiment = int(sentiment_str)
            # print(f"PRMPT {prompt['content']}: {sentiment}")
        except:
            print(f"Invalid output: {s}")
        return sentiment, len(tokens)

    @step
    def join(self, inputs):
        self.post_sentiment = {}
        for inp in inputs:
            self.post_sentiment.update(inp.post_sentiment)
        print(f"Sentiment recorded for {len(self.post_sentiment)} posts")
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HNSentimentAnalyzeComments()
