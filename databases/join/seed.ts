import { columnDefinition, createTable, insertCSV, Q, run } from "./util.ts";

async function main() {
  const movieColumns: columnDefinition[] = [
    {
      name: "movieId",
      type: "int",
    },
    {
      name: "title",
      type: "text",
    },
    {
      name: "genres",
      type: "text",
    },
  ];

  const linksColumns: columnDefinition[] = [
    {
      name: "movieId",
      type: "int",
    },
    {
      name: "imdbId",
      type: "text",
    },
    {
      name: "tmdbId",
      type: "int",
    },
  ];

  const ratingsColumns: columnDefinition[] = [
    {
      name: "userId",
      type: "int",
    },
    {
      name: "movieId",
      type: "int",
    },
    {
      name: "rating",
      type: "float32",
    },
    {
      name: "timestamp",
      type: "int",
    },
  ];

  const tagsColumns: columnDefinition[] = [
    {
      name: "userId",
      type: "int",
    },
    {
      name: "movieId",
      type: "int",
    },
    {
      name: "tag",
      type: "text",
    },
    {
      name: "timestamp",
      type: "int",
    },
  ];

  const genomeTagsColumns: columnDefinition[] = [
    {
      name: "tagId",
      type: "int",
    },
    {
      name: "tag",
      type: "text",
    },
  ];

  const genomeScoresColumns: columnDefinition[] = [
    {
      name: "movieId",
      type: "int",
    },
    {
      name: "tagId",
      type: "int",
    },
    {
      name: "relevance",
      type: "float64",
    },
  ];

  console.time("Seeding movielens data");

  await Promise.all([
    createTable("movies", movieColumns),
    createTable("links", linksColumns),
    createTable("ratings", ratingsColumns),
    createTable("tags", tagsColumns),
    createTable("genome-tags", genomeTagsColumns),
    createTable("genome-scores", genomeScoresColumns),
  ]);

  await Promise.all([
    insertCSV("../ml-20m/movies.csv", "movies"),
    insertCSV("../ml-20m/links.csv", "links"),
    insertCSV("../ml-20m/ratings.csv", "ratings"),
    insertCSV("../ml-20m/tags.csv", "tags"),
    insertCSV("../ml-20m/genome-tags.csv", "genome-tags"),
    insertCSV("../ml-20m/genome-scores.csv", "genome-scores"),
  ]);

  console.timeEnd;
}

main();
