# Veronica Join Algorithm

## Functionality
The code computes the similarity between a collection of sets (self-join on a single dataset) and print the number of similar pairs. It uses Veronica Join map-reduced based algorithm. 

## How to use
- Place your dataset in `data` directory (Each line represents a set and the tokens of the set are separated by whitespace).
- Set the your desired `threshold` for similarity checking in `Main` (The code uses Jaccard Similarity).
- Everything is set up! Now just run `sbt run` to see the number of similarity pairs.
