package abag.xmltoelastic;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.text.StringEscapeUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.jsoup.Jsoup;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

@Service
public class PostService {

    @Value("${bulk.insert.size}")
    private int batchSize;

    @Value("${xml.file.location}")
    private String fileLocation;

    private RestHighLevelClient restHighLevelClient;

    public PostService(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void saveXmlData() throws IOException {

        int rowNumber = 0;

        try (LineIterator lineIterator = FileUtils.lineIterator(new File(fileLocation))) {

            Instant saveStart = Instant.now();

            BulkRequest bulkRequest = new BulkRequest();

            while (lineIterator.hasNext()) {
                String line = lineIterator.nextLine();
                if (line.contains("<row")) {

                    Post post = convertXmlLineToPost(line);
                    IndexRequest indexRequest = new IndexRequest("post")
                            .id(String.valueOf(post.getId()))
                            .type("_doc")
                            .source(
                                    "postType", post.getPostType(),
                                    "score", post.getScore(),
                                    "viewCount", post.getViewCount(),
                                    "year", post.getYear(),
                                    "title", post.getTitle(),
                                    "body", post.getBody()
                            );
                    bulkRequest.add(indexRequest);

                    rowNumber++;

                    if (rowNumber % batchSize == 0) {
                        restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                        bulkRequest = new BulkRequest();
                    }

                    if (rowNumber % 100_000 == 0) {
                        System.out.println(rowNumber + " / 10 000 000 rows inserted");
                    }

                    if (rowNumber == 10_000_000) {
                        break;
                    }
                }
            }
            Instant saveEnd = Instant.now();

            long saveTime = Duration.between(saveStart, saveEnd).toMillis();

            System.out.println("Save time: " + saveTime + "ms");

        }
    }

    private Post convertXmlLineToPost(String line) {
        Post post = new Post();

        post.setId(Integer.parseInt(extractStringField("Id", line)));

        if (line.contains("PostTypeId=\"")) {
            post.setPostType(Integer.parseInt(extractStringField("PostTypeId=\"", line)));
        }

        if (line.contains("Score=\"")) {
            post.setScore(Integer.parseInt(extractStringField("Score", line)));
        }

        if (line.contains("ViewCount=\"")) {
            post.setViewCount(Integer.parseInt(extractStringField("ViewCount=\"", line)));
        }

        if (line.contains("CreationDate=\"")) {
            String date = extractStringField("CreationDate=\"", line);
            int year = Integer.parseInt(date.substring(0, 4));
            post.setYear(year);
        }

        if (line.contains("Body=\"")) {
            String rawBody = extractStringField("Body=\"", line);
            String bodyText = removeHTMLFromString(rawBody);
            post.setBody(bodyText);
        }

        if (line.contains("Title=\"")) {
            String rawTitle = extractStringField("Title=\"", line);
            String titleText = removeHTMLFromString(rawTitle);
            post.setTitle(titleText);
        }

        return post;
    }

    private String extractStringField(String fieldName, String line) {
        int beginFieldIndex = line.indexOf(fieldName);
        int beginIndex = line.indexOf("\"", beginFieldIndex) + 1;
        int endIndex = line.indexOf("\"", beginIndex);
        return line.substring(beginIndex, endIndex);
    }

    private String removeHTMLFromString(String input) {
        String replace = input.replace("&amp;", "");
        String html = StringEscapeUtils.unescapeHtml4(replace);
        return Jsoup.parse(html).text();
    }

}
