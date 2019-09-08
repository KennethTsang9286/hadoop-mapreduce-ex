# Hadoop MapReduce Exercise

## Description

This program takes all txt files in the inputDirectory and count the number of occasions for each ngram in all txt files.

## Output

This program will output the result in a txt file in the outputDirectory with format:

```
a collection 1 file01.txt
a network 1 file01.txt
a part 1 files03.txt
hadoop is 2 file01.txt file03.txt
```

where the first column is the ngram, the second column is the frequency and the last column is the source txt files.

# Input

| args |                                                                                                   desc                                                                                                    |
| ---- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------: |
| 0    |                                                    The value N for the ngram. For example, if the user is interested only in bigrams, then args[0]=2.                                                     |
| 1    | The minimum count for an ngram to be included in the output file. For example, if the user is interested only in ngrams that appear at least 10 times across the whole set of documents, then args[1]=10. |
| 2    |                                                              The directory containing the files in input. For example, args[2]=”/tmp/input/”                                                              |
| 3    |                                                          The directory where the output file will be stored. For example, args[3]=”/tmp/output/”                                                          |
