import os
import time
from math import log10
import sys
import nltk
from scipy.spatial.distance import cosine

class CS276SearchEngine():
    """ Set of features to implement a vector search engine on the CS276 collection """
    def __init__(self, path):
        self.__PATH = path
        self.__current_folder = 0
        self.__files_to_load = []
        # NLTK
        self.__stop_words = set(nltk.corpus.stopwords.words('english'))
        self.__stop_words.update(['.', ',', '"', "'", '?', '!', ':', ';', '(', ')', '[', ']', '{', '}'])
        self.__lemmatizer = nltk.stem.WordNetLemmatizer()
        self.__stemmer = nltk.stem.SnowballStemmer("english")
        # Stuff for later
        self.__current_docID = 0
        self.__clean_documents = {}
        self.__clean_documents_list = []
        # Vector space
        self.__index = {}
        self.__idfs = {}
        self.__keyword_to_vect_position = {}
        self.__vectors = {}

    def __get_files_name_to_load(self):
        """ Loads the collection specified in the path and at the correct id """
        file_id = 0
        self.__files_to_load = []
        for root, dirs, files in os.walk(os.path.join(self.__PATH, str(self.__current_folder))):
            for file in files:
                self.__files_to_load.append(file)
                sys.stdout.write("Loading progress: %d%%   \r" % (100 * file_id/len(files)))
                sys.stdout.flush()
                file_id += 1

    @staticmethod
    def __parse_document_into_list(document):
        """ Parse the document into a list """
        return document.split(' ')

    @staticmethod
    def __tokenize_document(document):
        """ Convert a document into a list of tokens """
        tokens = nltk.tokenize.word_tokenize(document)
        return tokens

    def __filter_stop_words(self, document):
        """ Removes the stop words in a document """
        terms_clean = list(filter(lambda word: word not in self.__stop_words, document))
        return terms_clean

    def __lemmatize_document(self, document):
        """ Lemmatizes the document """
        lemmatized_document = []
        for word in document:
            lemmatized_document.append(self.__lemmatizer.lemmatize(word))
        return lemmatized_document

    def __stem_document(self, document):
        """ Stems the document with the SnowBall Stemmer """
        stemmed_document = []
        for word in document:
            stemmed_document.append(self.__stemmer.stem(word))
        return stemmed_document

    def __clean_all_documents(self):
        """ Cleans all the documents in the current folder and updates the list and the doc/docID dict """
        self.__current_docID = 0
        self.__clean_documents = {}
        self.__clean_documents_list = []

        for file in self.__files_to_load:
            with open(os.path.join(self.__PATH, str(self.__current_folder), file), 'r') as f:
                doc = f.read()
                doc = self.__parse_document_into_list(doc)
                doc = self.__filter_stop_words(doc)
                #doc = self.__lemmatize_document(doc)
                #doc = self.__stem_document(doc)
                self.__clean_documents_list.append(doc)
                self.__clean_documents[self.__current_docID] = doc
                sys.stdout.write("Cleaning progress (Stop-words, Stemming, Lemmatizing): %d%%   \r" % (100 * self.__current_docID / len(self.__files_to_load)))
                sys.stdout.flush()
                self.__current_docID += 1

    def __create_index(self):
        """ 
        Create an index to compute tfs and idfs
        Returns: {word: {docID: [1, 5, 17 ...] ...} ...}
        """
        # Get the vocabulary and store the positions in each document for each word
        for docID, doc in enumerate(self.__clean_documents_list):
            for word_position, word in enumerate(doc):
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

    def __vectorize_doc(self, docID):
        """ Vectorize document with a very simple 1-0 rule """
        doc = self.__clean_documents[docID]
        vector = [0] * len(self.__keyword_to_vect_position.keys())
        for word in doc:
            vector[self.__keyword_to_vect_position[word]] += 1
        # Loop again to set the weights
        for word in doc:
            i = self.__keyword_to_vect_position[word]
            vector[i] = (1 + log10(vector[i])) * self.__idfs[word]
        return vector

    def initialize_engine(self):
        """ Initialize engine by reading and parsing the data """
        sys.stdout.write("Starting Engine \r")
        sys.stdout.flush()
        sys.stdout.write("Reading database... \r")
        sys.stdout.flush()
        self.__get_files_name_to_load()
        sys.stdout.write("Dropping unwanted markers and filtering stop-words...      \r")
        sys.stdout.flush()
        self.__clean_all_documents()
        sys.stdout.write("Creating index...                                 \r")
        sys.stdout.flush()
        self.__create_index()
        self.__compute_weights()

        # Create the vector space
        for doc in self.__clean_documents_list:
            for word in doc:
                if word not in self.__keyword_to_vect_position.keys():
                    self.__keyword_to_vect_position[word] = len(self.__keyword_to_vect_position.keys())
        
        # Create a vector for each document
        for docID in self.__clean_documents:
            self.__vectors[docID] = self.__vectorize_doc(docID)

    def search(self, query):
        """ Cleans the query and compares it to the vectors in the database """
        t0 = time.time()
        clean_query = self.__tokenize_document(query)
        clean_query = map(lambda x: x.lower(), clean_query)
        clean_query = self.__filter_stop_words(clean_query)
        clean_query = self.__lemmatize_document(clean_query)
        clean_query = self.__stem_document(clean_query)
        # Convert to a vector
        query_vector = [0] * len(self.__keyword_to_vect_position.keys())
        for word in clean_query:
            query_vector[self.__keyword_to_vect_position[word]] = 1
        # Compute distances
        distances = {}
        for docID in self.__vectors:
            if docID > 0:
                distances[docID] = cosine(self.__vectors[docID], query_vector)

        # Return the list of documents and the time
        def get_distance(_t):
            """ Helper """
            return _t[1]

        results = sorted(distances.items(), key=get_distance)
        results = [t for t in results if get_distance(t) < 1]

        return results, time.time() - t0
