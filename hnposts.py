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
from metaflow import nim
import tarfile, os, tempfile, shutil

# Flow 3
# Produce tags for crawled HN posts using an LLM
#
# python hnposts.py --environment=conda run --with kubernetes --max-workers 5
#
# Note two things:
# 1. --max-workers controls the concurrency sent to your LLM backend. Going
#    higher than what the backend can handle is not useful.
# 2. Depending on the number of posts to analyze and your LLM backend, this
#    flow is likely to take 5-10h to run. It makes sense to deploy to Argo
#    Workflows to keep the run running reliably
#
# After this, open hncomments.py

PROMPT = """Assign 10 tags that best describe the following article. Reply only the tags in the following format:
1. first tag
2. second tag
N. Nth tag"""

MODEL = "meta/llama3-70b-instruct"


@nim(models=[MODEL])
@project(name="hn_sentiment")
class HNSentimentAnalyzePosts(FlowSpec):

    prompt = Parameter("prompt", default=PROMPT)
    num_input_tokens = Parameter("num-input-tokens", default=5000)

    @step
    def start(self):
        crawl_run = list(Flow("HNSentimentCrawl").runs("crawldata"))[0]
        self.crawl_id = crawl_run.id
        print(f"Using data from crawl {self.crawl_id}")
        self.tarballs = []
        for task in crawl_run["crawl"]:
            self.tarballs.append(task["url"].data)
        self.next(self.analyze_posts, foreach="tarballs")

    @card(type="blank")
    @retry
    @conda(packages={"beautifulsoup4": "4.12.3"})
    @step
    def analyze_posts(self):
        from bs4 import BeautifulSoup  # pylint: disable=import-error

        print("downloading data from", self.input)
        root = tempfile.mkdtemp("hnpost")
        with S3() as s3:
            res = s3.get(self.input)
            tarfile.open(res.path).extractall(path=root)
            datapath = os.path.join(root, "data")
        print("Data extracted ok")
        status = Markdown("# Starting to analyze")
        progress = ProgressBar(max=len(os.listdir(datapath)), label="Docs analyzed")
        current.card.append(status)
        current.card.append(progress)
        ok = failed = 0
        self.post_tags = {}
        for i, post_id in enumerate(os.listdir(datapath)):
            with open(os.path.join(datapath, post_id)) as f:
                try:
                    tags, num_tokens = self.analyze(f.read(), BeautifulSoup)
                    self.post_tags[post_id] = (tags, num_tokens)
                    ok += 1
                except Exception as ex:
                    failed += 1
                    print(f"analyzing post {post_id} failed: ", ex)
            status.update(f"## Successfully processed {ok} docs, failed {failed}")
            progress.update(i + 1)
            current.card.refresh()
        shutil.rmtree(root)
        self.next(self.join)

    def analyze(self, data, BeautifulSoup):
        soup = BeautifulSoup(data, "html.parser")
        tokens = soup.get_text().split()[: self.num_input_tokens]
        doc = " ".join(tokens)
        llm = current.nim.models[MODEL]
        prompt = {"role": "user", "content": f"{self.prompt}\n---\n{doc}"}
        chat_completion = llm(messages=[prompt], model=MODEL, n=1, max_tokens=400)
        s = chat_completion["choices"][0]["message"]["content"]
        tags = []
        for line in s.strip().splitlines():
            try:
                if "." in line:
                    [_, tag] = line.split(".", 1)
                    tags.append(tag.strip())
            except:
                print(f"Invalid response format: {s}")
                break
        return tags, len(tokens)

    @step
    def join(self, inputs):
        self.post_tags = {}
        for inp in inputs:
            self.post_tags.update(inp.post_tags)
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HNSentimentAnalyzePosts()
