import os
import sys
import time
import nltk
import re
from math import log10
from scipy.spatial.distance import cosine
from numpy.linalg import norm

class CACMSearchEngine():
    """ Set of features to implement a search engine on the CACM collection """
    def __init__(self, path, filename):
        self.__PATH = path
        self.__filename = filename
        # Markers in the collection to split the files
        self.__MARKERS_TO_KEEP = ['.T', '.W', '.K']
        self.__MARKERS_TO_DROP = ['.B', '.A', '.N', '.X', '.C']
        self.__MARKERS = self.__MARKERS_TO_KEEP + self.__MARKERS_TO_DROP
        self.__stop_words = set(nltk.corpus.stopwords.words('english'))
        self.__stop_words.update(['.', ',', '"', "'", '?', '!', ':', ';', '(', ')', '[', ']', '{', '}'])
        # Indexing variables
        self.__documents = []
        self.__tokenized_documents_as_list = []
        self.__clean_documents = {}
        self.__clean_documents_list = []
        # Vector space
        self.__index = {}
        self.__idfs = {}
        self.__keyword_to_vect_position = {}
        self.__vectors = {}

    def __load_data(self):
        """ Loads the collection specified in the path & filename """
        with open(os.path.join(self.__PATH, self.__filename), 'r') as file:
            data = file.read()
            self.__documents = re.split(r'\.I \d+', data)

    @staticmethod
    def __tokenize_document(document):
        """ Convert a document into a list of tokens """
        tokens = nltk.tokenize.word_tokenize(document)
        return tokens

    def __convert_document_list_into_tokenized_lists(self):
        """ Converts the list of documents to a list of lists """
        self.__tokenized_documents_as_list = []
        for document in self.__documents:
            tokens = self.__tokenize_document(document)
            self.__tokenized_documents_as_list.append(tokens)

            sys.stdout.write("Tokenizing process: %d%%                         \r" % (100 * len(self.__tokenized_documents_as_list)/len(self.__documents)))
            sys.stdout.flush()

    def __clean_tokenized_document(self, tokenized_document):
        """ SPECIFIC TO CACM - Drop the unwanted markers in the document """
        if len(tokenized_document) > 0:
            cont = True
            data = [] # Value to return
            for item in tokenized_document: # Document is a list if tokens
                if item in self.__MARKERS: # A marker has been identified
                    if item in self.__MARKERS_TO_DROP: # The marker should be dropped, cont is set to True to skip until next marker
                        cont = True
                        continue
                    else:
                        cont = False
                        continue
                if cont:
                    continue # Skip the data until the next marker
                else:
                    data.append(item.lower()) # Add the data
            return data
        else:
            return []

    def __filter_stop_words(self, clean_document):
        """ Removes the stop words in a document """
        terms_clean = list(filter(lambda word: word not in self.__stop_words, clean_document))
        return terms_clean

    def __clean_all_documents(self):
        for i in range(1, len(self.__tokenized_documents_as_list)):
            res = self.__clean_tokenized_document(self.__tokenized_documents_as_list[i])
            res = self.__filter_stop_words(res)
            self.__clean_documents_list.append(res)
            self.__clean_documents[i] = res
            sys.stdout.write("Cleaning process: %d%%                                             \r" % (100 * i/len(self.__tokenized_documents_as_list)))
            sys.stdout.flush()

    def __create_index(self):
        """ 
        Create an index to compute tfs and idfs
        Returns: {word: {docID: [1, 5, 17 ...] ...} ...}
        """
        # Get the vocabulary and store the positions in each document for each word
        for docID in self.__clean_documents:
            for word_position, word in enumerate(self.__clean_documents[docID]):
                try:
                    self.__index[word][docID].append(word_position)
                except KeyError:
                    # Either word or docID is not defined
                    try:
                        self.__index[word][docID] = []
                    except KeyError:
                        # word is not defined
                        self.__index[word] = {}
                        self.__index[word][docID] = []
                    # Both are defined now
                    self.__index[word][docID].append(word_position)

    def __compute_weights(self):
        """ Computes the tfs """
        N = len(self.__clean_documents_list)
        for word in self.__index:
            self.__idfs[word] = log10(N / len(self.__index[word].keys()))

    def __vectorize_doc_tf_idf(self, docID):
        """ Vectorize document with tf-idf """
        doc = self.__clean_documents[docID]
        vector = [0] * len(self.__keyword_to_vect_position.keys())
        for word in doc:
            if type(vector[self.__keyword_to_vect_position[word]]) == list:
                vector[self.__keyword_to_vect_position[word]] += 1
            else:
                vector[self.__keyword_to_vect_position[word]] = 1
        for word in doc:
            i = self.__keyword_to_vect_position[word]
            if vector[i] > 0:
                vector[i] = (1 + log10(vector[i])) * self.__idfs[word]
        return vector

    def __vectorize_doc_freq_norm(self, docID):
        """ Vectorize document with freq """
        doc = self.__clean_documents[docID]
        vector = [0] * len(self.__keyword_to_vect_position.keys())
        max_tf = 0
        for word in doc:
            if type(vector[self.__keyword_to_vect_position[word]]) == list:
                vector[self.__keyword_to_vect_position[word]] += 1
                if vector[self.__keyword_to_vect_position[word]] > max_tf:
                    max_tf = vector[self.__keyword_to_vect_position[word]]
            else:
                vector[self.__keyword_to_vect_position[word]] = 1
                if vector[self.__keyword_to_vect_position[word]] > max_tf:
                    max_tf = vector[self.__keyword_to_vect_position[word]]
        for word in doc:
            i = self.__keyword_to_vect_position[word]
            if vector[i] > 0:
                vector[i] = vector[i]/max_tf
        return vector

    def initialize_engine(self, ponderation="tf-idf"):
        """ Initialize engine by reading and parsing the data """
        sys.stdout.write("Starting Engine \r")
        sys.stdout.flush()
        sys.stdout.write("Reading database... \r")
        sys.stdout.flush()
        self.__load_data()
        print("Successfully parsed {} documents".format(len(self.__documents)))
        sys.stdout.write("Splitting into separate lists and tokenizing... \r")
        sys.stdout.flush()
        self.__convert_document_list_into_tokenized_lists()
        sys.stdout.write("Dropping unwanted markers and filtering stop-words... \r")
        sys.stdout.flush()
        self.__clean_all_documents()
        sys.stdout.write("Successfully created a clean list of documents ! \r")
        sys.stdout.flush()
        self.__create_index()
        self.__compute_weights()

        # Create the vector space
        for doc in self.__clean_documents_list:
            for word in doc:
                if word not in self.__keyword_to_vect_position.keys():
                    self.__keyword_to_vect_position[word] = len(self.__keyword_to_vect_position.keys())
        
        # Create a vector for each document
        if ponderation == "tf-idf" or ponderation == "tf-idf-norm":
            for docID in self.__clean_documents:
                self.__vectors[docID] = self.__vectorize_doc_tf_idf(docID)

        # If the method is a normalized tf-idf
        if ponderation == "tf-idf-norm":
            for docID in self.__clean_documents:
                n = norm(self.__vectors[docID])
                if n != 0:
                    self.__vectors[docID] = self.__vectors[docID]/n
        
        if ponderation == "freq-norm":
            for docID in self.__clean_documents:
                self.__vectors[docID] = self.__vectorize_doc_freq_norm(docID)

    def search(self, query):
        """ Cleans the query and compares it to the vectors in the database """
        t0 = time.time()
        clean_query = self.__tokenize_document(query)
        clean_query = map(lambda x: x.lower(), clean_query)
        clean_query = self.__filter_stop_words(clean_query)
        # Convert to a vector
        query_vector = [0] * len(self.__keyword_to_vect_position.keys())
        for word in clean_query:
            try:
                query_vector[self.__keyword_to_vect_position[word]] = 1
            except KeyError:
                # The word is not in any document
                pass
        if query_vector == [0] * len(self.__keyword_to_vect_position.keys()):
            return [], time.time() - t0
        # Compute distances
        distances = {}
        for docID in self.__vectors:
            if docID > 0:
                distances[docID] = cosine(self.__vectors[docID], query_vector)

        # Return the list of documents and the time
        def get_distance(t):
            """ Helper """
            return t[1]

        results = sorted(distances.items(), key=get_distance)
        results = [t for t in results if get_distance(t) < 1][:50] # Only the 10 first

        return results, time.time() - t0
