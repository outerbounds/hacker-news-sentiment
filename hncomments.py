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
import math, tarfile, shutil
import tempfile, os, time, io
import html

# Flow 4
# Produce comments threads for HN posts of interest
#
# First, head to Google Bigquery to export comment data
# as parquet files:
#
# EXPORT DATA
#  OPTIONS (
#    uri = 'gs://your-gs-bucket/hnexport/*.parquet',
#    format = 'parquet')
# AS
# SELECT * FROM `bigquery-public-data.hacker_news.full
# WHERE type='comment' and timestamp > timestamp '2020-01-01'
# ORDER BY timestamp ASC;
#
# Download the parquet files to a local directory 'hn-comments'
# and run locally
#
# python hncomments.py --environment=conda run
#
# The flow will upload the parquet files to S3 when run for
# the first time. You can interrupt the run after the start
# step unless you have a beefy workstation. Tag the run with
#
# python hncomments.py --environment=conda tag add commentdata --run-id [YOUR_RUN]
#
# to use the uploaded data for future runs. Next, to analyze
# data we need more resources, so run in the cloud:
#
# python hncomments.py --environment=conda run --with kubernetes
#
# If you are happy with the results, tag the result with
#
# python hncomments.py --environment=conda tag add commentresults --run-id [YOUR_RUN]
#
# After this, open hnsentiment.py


@project(name="hn_sentiment")
class HNSentimentCommentData(FlowSpec):

    local_parquets = Parameter("local-parquets", default="hn-comments")
    local_mode = Parameter("local-mode", default=False, is_flag=True)
    num_shards = Parameter("num-shards", default=50)

    @step
    def start(self):
        if not self.local_mode:
            self.comment_parquets = self.ensure_parquets()
        posts = Flow("HNSentimentInit").latest_successful_run.data.posts
        self.posts = {id for id, _, _, _ in posts}
        self.next(self.construct_comments)

    def ensure_parquets(self):
        try:
            run = list(Flow("HNSentimentCommentData").runs("commentdata"))[0]
            ret = run["start"].task["comment_parquets"].data
            print(f"Using existing parquets from run {run.id}")
            return ret
        except:
            print(f"Uploading parquets from {self.local_parquets}")
            with S3(run=self) as s3:
                fnames = [
                    (f"hn-comments/{f}", os.path.join(self.local_parquets, f))
                    for f in os.listdir(self.local_parquets)
                ]
                return [url for _, url in s3.put_files(fnames)]

    def ensure_local(self):
        if self.local_mode:
            return self.local_parquets
        else:
            root = "hn-comments"
            with S3() as s3:
                s3objs = s3.get_many(self.comment_parquets)
                os.makedirs(root)
                for obj in s3objs:
                    os.rename(
                        obj.path,
                        os.path.join(root, os.path.basename(obj.path) + ".parquet"),
                    )
                return root

    @conda(packages={"duckdb": "1.0.0"})
    @resources(disk=10000, cpu=8, memory=32000)
    @card(type="blank")
    @retry
    @step
    def construct_comments(self):
        self.parquet_root = self.ensure_local()
        print(f"{len(os.listdir(self.parquet_root))} parquet files loaded")
        post_comments = self.construct()
        self.post_comments_meta = {}
        current.card.append(Markdown("Packaging tarballs"))
        current.card.refresh()
        shards = []
        files = []
        for i in range(self.num_shards):
            tarname = f"comments-{i}.tar.gz"
            shards.append(tarfile.open(tarname, mode="w:gz"))
            files.append((f"comments/{tarname}", tarname))
        for post_id, comments in post_comments.items():
            s = post_id % self.num_shards
            self.post_comments_meta[post_id] = (len(comments), files[s])
            data = html.unescape("\n".join(comments)).encode("utf-8")
            buf = io.BytesIO(data)
            tarinfo = tarfile.TarInfo(name=f"comments/{post_id}")
            tarinfo.size = len(data)
            buf.seek(0)
            shards[s].addfile(tarinfo, buf)
        for shard in shards:
            shard.close()
        with S3(run=self) as s3:
            self.tarballs = [url for _, url in s3.put_files(files)]
            print(f"uploaded {len(self.tarballs)} tarballs")
        self.next(self.end)

    def construct(self):
        BS = 1000
        import duckdb  # pylint: disable=import-error

        num_rows = (
            duckdb.query(f"select count(*) from '{self.parquet_root}/*.parquet'")
            .execute()
            .fetchone()
        )
        status = Markdown(f"# Starting to process: {num_rows} comments in the DB")
        progress = ProgressBar(max=num_rows, label="Comments processed")
        current.card.append(status)
        current.card.append(progress)
        current.card.refresh()

        # BigQuery Hacker News table has an annoying format: Comments don't have
        # a direct reference to their post, just to their immediate parent which
        # may be a post or another comment. Hence we must link deep comments back
        # to their parent post recursively.
        #
        # We do this by going through all comments starting with the oldest
        # comments. As we encounter comments, we create a mapping on the fly that
        # map comments to the original post through their parents. This is
        # based on the assumption that parent comments always precede child comments
        # in time-ordered traversal.
        #
        # The end result is a flattened list of comments per post, returned in
        # post_comments.

        res = duckdb.query(
            f"select text, by, id, parent from '{self.parquet_root}/*.parquet' where"
            "(dead is null or dead=false) and "
            "(deleted is null or deleted=false) "
            "order by timestamp asc"
        ).execute()
        post_comments = {id: [] for id in self.posts}
        mapping = {id: id for id in self.posts}
        mapped = 0
        rows = res.fetchmany(BS)
        row_count = 0
        while rows:
            for text, by, id, parent in rows:
                if parent in mapping:
                    post_comments[mapping[parent]].append(f"<{by}> {text}")
                    mapping[id] = mapping[parent]
                    mapped += 1
            row_count += len(rows)
            status.update(f"## Mapped {mapped} comments")
            progress.update(row_count)
            current.card.refresh()
            rows = res.fetchmany(BS)
        return post_comments

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HNSentimentCommentData()
