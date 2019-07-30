# Table of Contents
1. [Introduction] (README.md#introduction)
2. [Approach] (README.md#approach)

# Introduction
In this project, we are given a collection of files containing corpuses of texts. The goal is to create a list of word IDs where each word ID is followed by the list of document IDs that contain the word. In other words, we are reverse indexing the words versus the document.

Requirements:
-- 1. The document IDs must be sorted for each word
-- 2. The words are sorted by word IDs

# Approach
-- 1. Read in the corpus. Perform text cleaning including lowercasing all words, removing punctuation, and filtering empty strings.
-- 2. Concatenate the corpuses from multiple input files. Reduce by key (word) tp make a list of the the documents for each word. Sort the document lists by alphabetic order.
-- 3. Assign word ID to each word. Replace the word in our data object with these IDs. Sort the data object by word ID.
-- 4. Write out the end data object to a CSV file. Collect CSV files from each worker and use Hadoop command to merge them.
