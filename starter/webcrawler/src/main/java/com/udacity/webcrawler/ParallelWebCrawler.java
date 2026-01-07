package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on
 * a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
    private final Clock clock;
    private final Duration timeout;
    private final int popularWordCount;
    private final ForkJoinPool pool;
    private final List<Pattern> ignoredUrls;
    private final int maxDepth;

    @Inject
    private PageParserFactory parserFactory;

    @Inject
    ParallelWebCrawler(
            Clock clock,
            @Timeout Duration timeout,
            @PopularWordCount int popularWordCount,
            @TargetParallelism int threadCount,
            @IgnoredUrls List<Pattern> ignoredUrls,
            @MaxDepth int maxDepth) {
        this.clock = clock;
        this.timeout = timeout;
        this.popularWordCount = popularWordCount;
        this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
        this.ignoredUrls = ignoredUrls;
        this.maxDepth = maxDepth;
    }

    @Override
    public CrawlResult crawl(List<String> startingUrls) {
        Instant deadline = clock.instant().plus(timeout);

        Map<String, Integer> counts = new ConcurrentHashMap<>();
        Set<String> visitedUrls = new ConcurrentSkipListSet<>();

        startingUrls.forEach(url -> {
            WebCrawlerTask task = new WebCrawlerTask(url, deadline, maxDepth,
                    counts, visitedUrls);
            pool.invoke(task);
        });

        pool.shutdown();

        CrawlResult.Builder builder = new CrawlResult.Builder();
        builder.setUrlsVisited(visitedUrls.size());
        builder.setWordCounts(counts.isEmpty() ? counts : WordCounts.sort(counts, popularWordCount));
        return builder.build();
    }

    @Override
    public int getMaxParallelism() {
        return Runtime.getRuntime().availableProcessors();
    }

    private class WebCrawlerTask extends RecursiveAction {

        private final String url;
        private final Instant deadline;
        private final int maxDepth;
        private final Map<String, Integer> counts;
        private final Set<String> visitedUrls;

        public WebCrawlerTask(String url, Instant deadline, int maxDepth,
                Map<String, Integer> counts,
                Set<String> visitedUrls) {
            this.url = url;
            this.deadline = deadline;
            this.maxDepth = maxDepth;
            this.counts = counts;
            this.visitedUrls = visitedUrls;
        }

        @Override
        protected void compute() {
            crawlInternal();
        }

        private void crawlInternal() {
            if (maxDepth == 0 || clock.instant().isAfter(deadline)) {
                return;
            }

            for (Pattern pattern : ignoredUrls) {
                if (pattern.matcher(url).matches()) {
                    return;
                }
            }
            if (visitedUrls.contains(url)) {
                return;
            }
            visitedUrls.add(url);
            PageParser.Result result = parserFactory.get(url).parse();

            for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
                if (counts.containsKey(e.getKey())) {
                    counts.compute(e.getKey(),
                            (key, value) -> value == null ? e.getValue() : e.getValue() + value);
                } else {
                    counts.compute(e.getKey(), (key, value) -> e.getValue());
                }
            }

            List<WebCrawlerTask> tasks = new ArrayList<>();
            for (String link : result.getLinks()) {
                tasks.add(new WebCrawlerTask(link, deadline, maxDepth - 1,
                        counts, visitedUrls));
            }
            invokeAll(tasks);
        }
    }
}
