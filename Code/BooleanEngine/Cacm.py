import os
import sys
import re
from threading import Thread
from queue import Queue
import nltk


class CACMSearchEngine():
    """ Set of features to implement a boolean search engine on the CACM collection """
    def __init__(self, path, filename):
        self.__PATH = path
        self.__filename = filename
        # Markers in the collection to split the files
        self.__MARKERS_TO_KEEP = ['.T', '.W', '.K']
        self.__MARKERS_TO_DROP = ['.B', '.A', '.N', '.X', '.C']
        self.__MARKERS = self.__MARKERS_TO_KEEP + self.__MARKERS_TO_DROP
        self.__stop_words = set(nltk.corpus.stopwords.words('english'))
        self.__stop_words.update(
            ['.', ',', '"', "'", '?', '!', ':', ';', '(', ')', '[', ']', '{', '}']
        )
        # Stuff for later
        self.__documents = []
        self.__tokenized_documents_as_list = []
        self.__clean_documents = {}
        self.__clean_documents_list = []
        self.__index = {}
        # Stuff for BSBI
        self.BSBI_vocabulary = {}
        self.__BSBI_tuples = []
        self.__BSBI_tuples_by_termID = []
        self.__BSBI_tuples_by_termID_and_docID = []
        self.BSBI_index = {}
        # Stuff for MapReduce
        self.MR_index = {}

    def __load_data(self):
        """ Loads the collection specified in the path & filename """
        with open(os.path.join(self.__PATH, self.__filename), 'r') as file:
            data = file.read()
            self.__documents = re.split(r'\.I \d+', data)

    def __convert_document_list_into_tokenized_lists(self):
        """ Converts the list of documents to a list of lists """
        self.__tokenized_documents_as_list = []
        for document in self.__documents:
            tokens = self.__tokenize_document(document)
            self.__tokenized_documents_as_list.append(tokens)

            sys.stdout.write(
                "Tokenizing process: %d%%                         \r" % \
                (100 * len(self.__tokenized_documents_as_list)/len(self.__documents))
            )
            sys.stdout.flush()

    @staticmethod
    def __tokenize_document(document):
        """ Convert a document into a list of tokens """
        tokens = nltk.tokenize.word_tokenize(document)
        return tokens # Returns a list of the tokens

    @staticmethod
    def __get_number_of_tokens(document):
        """ Returns the number of tokens in a document """
        return len(document)

    def __clean_tokenized_document(self, tokenized_document):
        """ SPECIFIC TO CACM - Drop the unwanted markers in the document """
        if len(tokenized_document) > 0:
            cont = True
            data = [] # Value to return
            for item in tokenized_document: # Document is a list if tokens
                if item in self.__MARKERS: # A marker has been identified
                    if item in self.__MARKERS_TO_DROP:
                        # The marker should be dropped, \
                        # cont is set to True to skip until next marker
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
        for i in range(len(self.__tokenized_documents_as_list)):
            res = self.__clean_tokenized_document(self.__tokenized_documents_as_list[i])
            res = self.__filter_stop_words(res)
            self.__clean_documents_list.append(res)
            self.__clean_documents[i] = res
            sys.stdout.write(
                "Cleaning process: %d%%                                             \r" % \
                (100 * i/len(self.__tokenized_documents_as_list))
            )
            sys.stdout.flush()

    def initialize_engine(self):
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

    # BSBI ALGORITHM
    @staticmethod
    def __get_termID(t):
        """ Used for sorting """
        return t[0]

    @staticmethod
    def __get_docID(t):
        """ Used for sorting """
        return t[1]

    @staticmethod
    def __get_vocabulary(clean_document):
        """ Returns the vocabulary from a document """
        vocabulary = set()
        for elem in clean_document:
            vocabulary.add(elem)
        return vocabulary

    def create_BSBI_index(self):
        """ Create an index based on the BSBI algorithm """
        # Create a term/termID dictionnary
        current_id = 0
        current_doc_number = 0
        for document in self.__clean_documents_list:
            voc = self.__get_vocabulary(document)
            for word in voc:
                if word in self.BSBI_vocabulary:
                    pass
                else:
                    self.BSBI_vocabulary[word] = current_id
                    current_id += 1
            sys.stdout.write("Building term/termID dictionnary: %d%%                    \r" % (100 * current_doc_number/len(self.__clean_documents_list)))
            sys.stdout.flush()
            current_doc_number += 1

        # Iterate on the documents to gather tuples (termID, docID)
        current_doc_number = 0
        for i in self.__clean_documents:
            current_doc = self.__clean_documents[i]
            for word in current_doc:
                self.__BSBI_tuples.append((self.BSBI_vocabulary[word], i))
            sys.stdout.write("Gathering tuples (termID, docID): %d%%                  \r" % (100 * current_doc_number/len(self.__clean_documents_list)))
            sys.stdout.flush()
            current_doc_number += 1

        # Sort the tuples by termID
        self.__BSBI_tuples_by_termID = sorted(self.__BSBI_tuples, key=self.__get_termID)
        sys.stdout.write("Sorted BSBI Tuples by TermID                                  \r")

        # Sort again on docID
        # Do not break the order. Work with batches with the same termID and sort in place
        current_termID = 0
        current_batch = []
        for t in self.__BSBI_tuples_by_termID:
            if self.__get_termID(t) == current_termID:
                current_batch.append(t)
            else:
                self.__BSBI_tuples_by_termID_and_docID += sorted(current_batch, key=self.__get_docID)
                current_batch = [t]
                current_termID += 1
        sys.stdout.write("Sorted BSBI Tuples by TermID & DocID                        \r")

        # Merge results to get the reverse index
        current_termID = 0
        current_doc_number = 0
        for t in self.__BSBI_tuples_by_termID_and_docID:
            try:
                self.BSBI_index[self.__get_termID(t)]
                try:
                    self.BSBI_index[self.__get_termID(t)][self.__get_docID(t)]
                    self.BSBI_index[self.__get_termID(t)][self.__get_docID(t)] += 1
                except KeyError:
                    self.BSBI_index[self.__get_termID(t)][self.__get_docID(t)] = 1
            except KeyError:
                self.BSBI_index[self.__get_termID(t)] = {}
                self.BSBI_index[self.__get_termID(t)][self.__get_docID(t)] = 1

            sys.stdout.write("Merging results: %d%%                      \r" % (100 * current_doc_number/len(self.__BSBI_tuples_by_termID_and_docID)))
            sys.stdout.flush()
            current_doc_number += 1
        print("Created reverse index for the BSBI algorithm")

    # MAPREDUCE ALGORITHM W/O THREADING
    @staticmethod
    def __mapper(doc, docID):
        """ Create a list of tuples (term, docID) """
        res = []
        for word in doc:
            res.append((word, docID))
        return res

    def __reducer(self, tuples_list):
        """ Aggregate the tuples """
        for t in tuples_list:
            if t[0] in self.MR_index:
                if t[1] in self.MR_index[t[0]]:
                    self.MR_index[t[0]][t[1]] += 1
                else:
                    self.MR_index[t[0]][t[1]] = 1
            else:
                self.MR_index[t[0]] = {}
                self.MR_index[t[0]][t[1]] = 1

    def create_MR_index(self, threading=False):
        if not threading:
            buffer = []
            for i in self.__clean_documents:
                buffer.append(self.__mapper(self.__clean_documents[i], i))
            for b in buffer:
                self.__reducer(b)
        else:
            # Create the synchronised queue
            docs_queue = Queue()
            for doc in self.__clean_documents:
                docs_queue.put(doc)
            buffer = Queue()

            # Create the index
            MR_index = {}
            global continuer
            continuer = True

            # Create the threads
            class mapper(Thread):
                """ Mapper """
                def __init__(self, documents):
                    Thread.__init__(self)
                    self.__documents = documents

                def run(self):
                    while not docs_queue.empty():
                        docID = docs_queue.get()

                        res = []
                        for word in self.__documents[docID]:
                            res.append((word, docID))

                        buffer.put(res)
                        docs_queue.task_done()

            class reducer(Thread):
                """ Reducer """
                def __init__(self):
                    Thread.__init__(self)

                def run(self):
                    while (not buffer.empty()) or continuer:
                        tuples_list = buffer.get()
                        for t in tuples_list:
                            if t[0] in MR_index:
                                if t[1] in MR_index[t[0]]:
                                    MR_index[t[0]][t[1]] += 1
                                else:
                                    MR_index[t[0]][t[1]] = 1
                            else:
                                MR_index[t[0]] = {}
                                MR_index[t[0]][t[1]] = 1
                        buffer.task_done()

            # Run the threads
            M = []
            R = []
            # MAPPERS
            for tm in range(5):
                M.append(mapper(self.__clean_documents))
                M[tm].start()
            # REDUCERS
            for tr in range(5):
                R.append(reducer())
                R[tr].start()
            # Wait for the mappers to finish
            for tm in range(5):
                M[tm].join()
            # Let the reducers stop if the queue is empty
            continuer = False
            # Wait for the reducers to finish
            for tr in range(5):
                R[tr].join()
            # Put the dictionnary back
            self.MR_index = MR_index
