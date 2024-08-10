from metaflow import (
    FlowSpec,
    IncludeFile,
    step,
    conda,
    project,
    Flow,
    Parameter,
    current,
    retry,
    card,
    S3,
    resources,
)
from metaflow.cards import Markdown, ProgressBar
import math, tarfile
import tempfile, os

# Flow 2
# Download HN posts of interest
#
# then run or deploy as
# python hncrawl.py run --with kubernetes --max-workers 100
#
# Note that you can have a high number for --max-workers as
# each task hits a different set of websites (no DDOS'ing).
#
# After the run succeeds, tag it with
#
#  python hncrawl.py tag add --run-id [YOUR_RUN_ID] crawldata
#
# you can choose which crawl to use by moving the tag to
# a run you like
#
# After this, open hnposts.py

def make_batches(items, n):
    bs = math.ceil(len(items) / n)
    return [items[i * bs : (i + 1) * bs] for i in range(n)]


@project(name="hn_sentiment")
class HNSentimentCrawl(FlowSpec):

    num_parallel = Parameter("num-parallel", default=50)
    max_posts = Parameter("max-posts", default=-1)

    @step
    def start(self):
        maxp = None if self.max_posts == -1 else self.max_posts
        posts = Flow("HNSentimentInit").latest_successful_run.data.posts[:maxp]
        self.batches = make_batches(posts, self.num_parallel)
        self.next(self.crawl, foreach="batches")

    @resources(disk=1000, cpu=2, memory=4000)
    @card(type="blank")
    @retry
    @step
    def crawl(self):
        import requests

        self.dir = tempfile.mkdtemp("hncrawl")
        ok = failed = 0
        status = Markdown("# Starting to download")
        progress = ProgressBar(max=len(self.input), label="Urls processed")
        current.card.append(status)
        current.card.append(progress)
        self.successful = set()
        self.failed = set()
        for i, (id, title, score, url) in enumerate(self.input):
            try:
                resp = requests.get(url, allow_redirects=True, timeout=10)
                resp.raise_for_status()
                with open(os.path.join(self.dir, str(id)), mode="wb") as f:
                    f.write(resp.content)
                self.successful.add(id)
                ok += 1
            except:
                self.failed.add(id)
                failed += 1
            if i == len(self.input) - 1 or not i % 20:
                status.update(f"## Successful downloads {ok}, failed {failed}")
                progress.update(i + 1)
                current.card.refresh()
        tarname = f"crawl-{self.index}.tar.gz"
        with tarfile.open(tarname, mode="w:gz") as tar:
            tar.add(self.dir, arcname="data")
        print(f"file size {os.path.getsize(tarname) / 1024**2}MB")
        with S3(run=self) as s3:
            [(_, self.url)] = s3.put_files([(tarname, tarname)])
            print(f"uploaded {self.url}")
        self.next(self.join)

    @step
    def join(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HNSentimentCrawl()
