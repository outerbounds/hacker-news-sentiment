from metaflow import FlowSpec, IncludeFile, step, conda, project

# download story.parquet from
# https://huggingface.co/datasets/julien040/hacker-news-posts
#
# then run or deploy as
# python hninit.py --package-suffixes .parquet --environment=conda run

@project(name="hn_sentiment")
class HNSentimentInit(FlowSpec):

    @conda(packages={"duckdb": "1.0.0"})
    @step
    def start(self):
        import duckdb  # pylint: disable=import-error

        self.posts = (
            duckdb.query(
                """
            select id, title, score, url from 'story.parquet'
            where score > 20 and
                  to_timestamp(time) > timestamp '2020-01-01' and
                  comments > 5 and
                  url is not null
        """
            )
            .execute()
            .fetchall()
        )
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HNSentimentInit()
