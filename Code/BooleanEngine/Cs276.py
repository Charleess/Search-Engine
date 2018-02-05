import os
import sys
import nltk
from threading import Thread
from queue import Queue

class CS276SearchEngine():
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
        self.__index = {}
        # Stuff for BSBI
        self.BSBI_vocabulary = {}
        self.__BSBI_tuples = []
        self.__BSBI_tuples_by_termID = []
        self.__BSBI_tuples_by_termID_and_docID = []
        self.BSBI_index = {}
        self.__max_termID = 0
        # Stuff for MapReduce
        self.MR_index = {}

    def __get_files_name_to_load(self):
        """ Loads the collection specified in the path and at the correct id """
        file_id = 0
        self.__files_to_load = []
        for root, dirs, files in os.walk(os.path.join(self.__PATH, str(self.__current_folder))):
            for file in files:
                self.__files_to_load.append(file)
                sys.stdout.write("Cleaning progress: %d%%   \r" % (100 * file_id/len(files)))
                sys.stdout.flush()
                file_id += 1

    @staticmethod
    def __parse_document_into_list(document):
        """ Parse the document into a list """
        return document.split(' ')

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
                

    @staticmethod
    def __get_vocabulary(clean_document):
        """ Returns the vocabulary from a document """
        vocabulary = set()
        for elem in clean_document:
            vocabulary.add(elem)
        return vocabulary

    def __initialize_engine(self):
        """ Initialize engine by reading and parsing the data """
        print("Engine started")
        print("Reading database...")
        self.__get_files_name_to_load()
        print("Splitting into documents...")
        print("Dropping unwanted markers and filtering stop-words...               ")
        self.__clean_all_documents()
        print("Successfully created a clean list of documents !              ")

    ################## USED WITH EXTERNAL SORTING TO CREATE THE TERMID/DOCID TUPLES ############# 
    # def __create_partial_term_termID_dict(self):
    #     """ Append to the total dict """
    #     current_doc_number = 0
    #     for document in self.__clean_documents_list:
    #         voc = self.__get_vocabulary(document)
    #         for word in voc:
    #             if word in self.BSBI_vocabulary:
    #                 pass
    #             else: 
    #                 self.BSBI_vocabulary[word] = self.__max_termID
    #                 self.__max_termID += 1
    #         sys.stdout.write("Building term/termID dictionnary: %d%%   \r" % (100 * current_doc_number/len(self.__clean_documents_list)))
    #         sys.stdout.flush()
    #         current_doc_number += 1

    # def __create_term_termID_dict(self):
    #     for i in range(10):
    #         self.__current_folder = i
    #         print("Current folder is: {}".format(self.__current_folder))
    #         self.__initialize_engine()
    #         self.__create_partial_term_termID_dict()
    ##############################################################################################

    @staticmethod
    def __get_termID(t):
        """ Used for sorting """
        return t[0]

    @staticmethod
    def __get_docID(t):
        """ Used for sorting """
        return t[1]
    
    def create_BSBI_index(self):
        """ Supposes that the term/termID (self.BSBI_vocabulary) is already computed, checks if not """

        # Iterate on the documents to gather tuples (termID, docID)
        for i in range(10):
            self.__current_folder = i
            print("Current folder is: {}".format(self.__current_folder))
            self.__initialize_engine()

            # Add the new vocabulary to the document
            current_doc_number = 0
            for document in self.__clean_documents_list:
                voc = self.__get_vocabulary(document)
                for word in voc:
                    if word in self.BSBI_vocabulary:
                        pass
                    else: 
                        self.BSBI_vocabulary[word] = self.__max_termID
                        self.__max_termID += 1
                sys.stdout.write("Building term/termID dictionnary: %d%%                    \r" % (100 * current_doc_number/len(self.__clean_documents_list)))
                sys.stdout.flush()
                current_doc_number += 1 
            current_doc_number = 0
            #self.__BSBI_tuples = []
            #self.__BSBI_tuples_by_termID = []
            #self.__BSBI_tuples_by_termID_and_docID = []
            for j in self.__clean_documents:
                current_doc = self.__clean_documents[j]
                for word in current_doc:
                    self.__BSBI_tuples.append((self.BSBI_vocabulary[word], str(i) + str(j)))
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

        # Merge the tuples
        current_termID = 0
        current_posting_dic = {}
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

    # MAPREDUCE ALGORITHM
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
            for i in range(10):
                self.__current_folder = i
                print("Current folder is: {}".format(self.__current_folder))
                self.__initialize_engine()
                buffer = []
                for i in self.__clean_documents:
                    buffer.append(self.__mapper(self.__clean_documents[i], i))
                for b in buffer:
                    self.__reducer(b)
        else:
            # Create the synchronised queue
            docs_queue = Queue()
            for i in range(10):
                self.__current_folder = i
                print("Current folder is: {}".format(self.__current_folder))
                self.__initialize_engine()
                for doc in self.__clean_documents:
                    docs_queue.put(doc)
            buffer = Queue()

            # Create the index
            MR_index = {}
            global continuer
            continuer = True

            # Create the threads
            class mapper(Thread):
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
